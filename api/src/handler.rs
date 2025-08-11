use std::{io::{Read, Write}, os::unix::net::UnixStream};

use async_channel::Receiver;
use serde::Deserialize;

use crate::{summary::summary, ApiState};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload<'a> {
    correlation_id: &'a str,
    amount: f64,
}

const HTTP_ACCEPTED_RESPONSE: &[u8] = b"HTTP/1.1 202 Accepted\r\n\r\n";

pub async fn handler_loop(state: &ApiState, http_rx: Receiver<UnixStream>) -> anyhow::Result<()> {

    let mut buffer = [0; 256];
    
    while let Ok(mut stream) = http_rx.recv().await {

        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        stream.read(&mut buffer)?;

        if let Ok(httparse::Status::Complete(start)) = req.parse(&buffer) {
            let body = &buffer[start..];
            let path = req.path.unwrap_or("");
            let method = req.method.unwrap_or("").to_string();

            if method == "POST" && path == "/payments" {
                let body_len = body.iter().position(|&b| b == 0).unwrap_or(buffer.len());
                let body = match serde_json::from_slice::<PaymentPayload>(&body[..body_len]) {
                    Ok(body) => body,
                    Err(e) => {
                        eprintln!("Failed to deserialize payment payload: {e}");
                        stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")?;
                        continue;
                    }
                };
                stream.write_all(HTTP_ACCEPTED_RESPONSE)?;
                state.tx.send((body.correlation_id.to_string(), body.amount))?;
            }
            else if path.starts_with("/payments-summary") {
                let mut query = std::collections::HashMap::new();
                for (key, value) in path.split('?').nth(1).unwrap().split('&').map(|s| s.split_once('=').unwrap()) {
                    query.insert(key, value);
                }
                let from = query.get("from").unwrap().parse()?;
                let to = query.get("to").unwrap().parse()?;
                let summary = summary(&state, from, to).await;
                let body = serde_json::to_string(&summary)?;
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", body.len(), body).as_bytes())?;

            }
            else if path == "/purge-payments" {
                println!("Purging payments");
                state.psql_client.batch_execute("DELETE FROM payments_default; DELETE FROM payments_fallback;").await?;
                stream.write_all(HTTP_ACCEPTED_RESPONSE)?;
                println!("Finished purging payments");
            }
            else {
                stream.write_all("HTTP/1.1 404 Not Found\r\n\r\n".as_bytes())?;
            }
        }
        else {
            println!("Invalid request: {}", String::from_utf8_lossy(&buffer));
            stream.write_all("HTTP/1.1 500 Internal Server Error\r\n\r\n".as_bytes())?;
        }
    }
    Ok::<(), anyhow::Error>(())
}