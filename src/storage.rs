
use chrono::{DateTime, Utc};
use tokio_stream::StreamExt;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::fs::File;
use tokio::io::BufReader;

pub type StorageRecord = (Decimal, String, DateTime<Utc>);

#[derive(Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PaymentsSummaryDetails {
    pub total_requests: i64,
    pub total_amount: Decimal
}

#[derive(Serialize, Default, Debug)]
pub struct PaymentsSummary {
    pub default: PaymentsSummaryDetails,
    pub fallback: PaymentsSummaryDetails
}

pub const PAYMENTS_STORAGE_PATH: &'static str = "/tmp/payments";
pub const PAYMENTS_STORAGE_PATH_HEALTH: &'static str = "/tmp/payments/health.json";
pub const PAYMENTS_STORAGE_PATH_DATA: &'static str = "/tmp/payments/data";


pub async fn get_summary(from: &DateTime<Utc>, to: &DateTime<Utc>) -> anyhow::Result<PaymentsSummary> {
    let mut default = PaymentsSummaryDetails::default();
    let mut fallback = PaymentsSummaryDetails::default();

    let file = match File::open(PAYMENTS_STORAGE_PATH_DATA).await {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PaymentsSummary::default());
        }
        Err(e) => return Err(e.into()),
    };
    let reader = BufReader::new(file);

    let mut csv_deserializer = csv_async::AsyncReaderBuilder::new()
        .has_headers(false)
        .create_deserializer(reader);

    let mut records = csv_deserializer.deserialize::<StorageRecord>();

    while let Some(record) = records.next().await {
        let (amount, payment_processor_id, requested_at): StorageRecord = record?;
        if requested_at > *from && requested_at < *to {
            let summary_details = match payment_processor_id.as_str() {
                "D" => &mut default,
                "F" => &mut fallback,
                _ => continue,
            };
            summary_details.total_requests += 1;
            summary_details.total_amount += amount;
        }
    }

    Ok(PaymentsSummary { default, fallback })
}