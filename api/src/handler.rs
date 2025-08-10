use async_channel::Receiver;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::UnixStream};

use crate::{summary::summary, ApiState};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: Decimal,
}

const HTTP_ACCEPTED_RESPONSE: &[u8] = b"HTTP/1.1 202 Accepted\r\n\r\n";

pub async fn handler_loop(state: &ApiState, http_rx: Receiver<UnixStream>) -> anyhow::Result<()> {

    while let Ok(mut stream) = http_rx.recv().await {

        stream.write_all(HTTP_ACCEPTED_RESPONSE).await?;

        // let mut headers = [httparse::EMPTY_HEADER; 16];
        // let mut buffer = vec![0; 1024];
        // let mut req = httparse::Request::new(&mut headers);
        // stream.read(&mut buffer).await?;
        // let buffer = Vec::from(&buffer[..]);
        // if let Ok(httparse::Status::Complete(start)) = req.parse(&buffer) {
        //     let body = &buffer[start..];
        //     let path = req.path.unwrap_or("");
        //     let method = req.method.unwrap_or("").to_string();

        //     if method == "POST" && path == "/payments" {
        //         let body = String::from_utf8(body.to_vec())?;
        //         println!("Got payment: {body}");
        //         let body = serde_json::from_str::<PaymentPayload>(&body)?;
        //         let (correlation_id, amount) = (body.correlation_id, body.amount);
        //         _ = tokio::join!(
        //             state.tx.send((correlation_id, amount)),
        //             stream.write_all(HTTP_ACCEPTED_RESPONSE)
        //         );
        //     }
        //     else if path.starts_with("/payments-summary") {
        //         let mut query = std::collections::HashMap::new();
        //         for pair in path.split('?').nth(1).unwrap().split('&').map(|s| s.split_once('=').unwrap()) {
        //             query.insert(pair.0.to_string(), pair.1.to_string());
        //         }
        //         let from = query.get("from").unwrap().parse()?;
        //         let to = query.get("to").unwrap().parse()?;
        //         let summary = summary(&state, from, to).await;
        //         let body = serde_json::to_string(&summary)?;
        //         stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", body.len(), body).as_bytes()).await?;

        //     }
        //     else if path == "/purge-payments" {
        //         state.psql_client.batch_execute("DELETE FROM payments_default; DELETE FROM payments_fallback;").await?;
        //         stream.write_all(HTTP_ACCEPTED_RESPONSE).await?;
        //     }
        //     else {
        //         stream.write_all("HTTP/1.1 404 Not Found\r\n\r\n".as_bytes()).await?;
        //     }
        // }
    }
    Ok::<(), anyhow::Error>(())
}