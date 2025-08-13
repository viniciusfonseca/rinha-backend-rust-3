use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::UnixStream};

use crate::{summary::summary, ApiState};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: f64,
}

const HTTP_ACCEPTED_RESPONSE: &[u8] = b"HTTP/1.1 202 Accepted\r\n\r\n";

pub async fn handler_loop_stream(state: &ApiState, mut stream: UnixStream) -> anyhow::Result<()> {

    let mut buffer = [0; 256];
    loop {
        let n = stream.read(&mut buffer).await?;

        if n == 0 {
            println!("Connection closed");
            break
        }

        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);

        if let Ok(httparse::Status::Complete(start)) = req.parse(&buffer[..n]) {
            let body = &buffer[start..];
            let path = req.path.unwrap_or("");
            let method = req.method.unwrap_or("").to_string();

            if method == "POST" && path == "/payments" {
                let body_len = body.iter().position(|&b| b == 0).unwrap_or(buffer.len());
                let body = match serde_json::from_slice::<PaymentPayload>(&body[..body_len]) {
                    Ok(body) => body,
                    Err(e) => {
                        eprintln!("Failed to deserialize payment payload: {e}");
                        stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await?;
                        stream.flush().await?;
                        continue;
                    }
                };
                stream.write_all(HTTP_ACCEPTED_RESPONSE).await?;
                stream.flush().await?;
                state.tx.send((body.correlation_id, body.amount))?;
            }
            else if path.starts_with("/payments-summary") {
                let (from, to) = get_dates_from_qs(path);
                let summary = summary(&state, from, to).await;
                let body = serde_json::to_string(&summary)?;
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}", body.len(), body).as_bytes()).await?;
                stream.flush().await?;
            }
            else if path == "/purge-payments" {
                println!("Purging payments");
                state.psql_client.batch_execute("DELETE FROM payments_default; DELETE FROM payments_fallback;").await?;
                stream.write_all(HTTP_ACCEPTED_RESPONSE).await?;
                stream.flush().await?;
                println!("Finished purging payments");
            }
            else {
                stream.write_all("HTTP/1.1 404 Not Found\r\n\r\n".as_bytes()).await?;
                stream.flush().await?;
            }
        }
        else {
            println!("Invalid request: {}", String::from_utf8_lossy(&buffer));
            stream.write_all("HTTP/1.1 500 Internal Server Error\r\n\r\n".as_bytes()).await?;
            stream.flush().await?;
        }
    }

    Ok(())
}

fn get_dates_from_qs(path: &str) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let mut query = std::collections::HashMap::new();
    for (key, value) in path.split('?').nth(1).unwrap_or_default().split('&').map(|s| s.split_once('=').unwrap_or_default()) {
        query.insert(key, value);
    }
    (
        query.get("from").and_then(|s| s.parse().ok()),
        query.get("to").and_then(|s| s.parse().ok())
    )
}

#[cfg(test)]
mod tests {

    use chrono::{Days, Utc};

    use crate::handler::get_dates_from_qs;

    #[test]
    fn test_null_date() {
        let path = "/payments-summary";
        let (from, to) = get_dates_from_qs(path);

        assert_eq!(from, None);
        assert_eq!(to, None);
    }

    #[test]
    fn test_null_from_date() {
        let to_qs = Utc::now().to_rfc3339();

        let path = format!("/payments-summary?to={to_qs}");

        let (from, to) = get_dates_from_qs(&path);

        assert_eq!(from, None);
        assert_eq!(to, Some(to_qs.parse().unwrap()));
    }

    #[test]
    fn test_null_to_date() {
        let from_qs = Utc::now().checked_sub_days(Days::new(1)).unwrap().to_rfc3339();

        let path = format!("/payments-summary?from={from_qs}");

        let (from, to) = get_dates_from_qs(&path);

        assert_eq!(from, Some(from_qs.parse().unwrap()));
        assert_eq!(to, None);
    }

    #[test]
    fn test_date() {
        let from_qs = Utc::now().checked_sub_days(Days::new(1)).unwrap().to_rfc3339();
        let to_qs = Utc::now().to_rfc3339();

        let path = format!("/payments-summary?from={from_qs}&to={to_qs}");

        let (from, to) = get_dates_from_qs(&path);

        assert_eq!(from, Some(from_qs.parse().unwrap()));
        assert_eq!(to, Some(to_qs.parse().unwrap()));
    }
}