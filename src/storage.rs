
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

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

    let data = tokio::fs::read(PAYMENTS_STORAGE_PATH_DATA).await?;
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(data.as_slice());

    for record in csv_reader.deserialize() {
        let (amount, payment_processor_id, requested_at): StorageRecord = record?;
        if requested_at > *from && requested_at < *to {
            match payment_processor_id.as_str() {
                "D" => {
                    default.total_requests += 1;
                    default.total_amount += amount;
                },
                "F" => {
                    fallback.total_requests += 1;
                    fallback.total_amount += amount;
                },
                _ => (),
            }
        }
    }

    Ok(PaymentsSummary { default, fallback })
}