use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::{payment_processor::PaymentProcessorIdentifier, worker::QueueEvent};

pub struct Partition {
    writer: tokio::io::WriteHalf<tokio::fs::File>,
}

type PartitionRecord = (String, Decimal, String, DateTime<Utc>);

impl Partition {
    pub async fn write(&mut self, correlation_id: &str, amount: Decimal, payment_processor_id: &str, requested_at: DateTime<Utc>) -> anyhow::Result<()> {

        let record = (correlation_id.to_string(), amount, payment_processor_id.to_string(), requested_at);
        let mut csv_writer = csv::Writer::from_writer(vec![]);

        csv_writer.serialize(record)?;

        let bytes = &mut csv_writer.into_inner()?;
        bytes.push(b'\n');

        self.writer.write_all(bytes).await?;
        self.writer.flush().await?;

        Ok(())
    }
}

pub struct PaymentsStorage {
    partitions: dashmap::DashMap<i64, Partition>,
}

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

const PAYMENTS_STORAGE_PATH: &'static str = "/tmp/payments";

impl PaymentsStorage {
    pub fn new() -> PaymentsStorage {
        std::fs::create_dir_all(PAYMENTS_STORAGE_PATH).ok();
        PaymentsStorage {
            partitions: dashmap::DashMap::new(),
        }
    }

    pub async fn insert_payment(&self, (correlation_id, amount): &QueueEvent, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {

        let partition_key = requested_at.timestamp_millis() / 1000; // Partition by second

        let mut partition = match self.partitions.entry(partition_key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.into_ref(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let (_, writer) = tokio::io::split(tokio::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(format!("{PAYMENTS_STORAGE_PATH}/{pk}", pk = partition_key.to_string()))
                    .await?);

                entry.insert(Partition { writer })
            }
        };

        let payment_processor = match payment_processor_id {
            PaymentProcessorIdentifier::Default => "D",
            PaymentProcessorIdentifier::Fallback => "F",
        };

        partition.write(&correlation_id, *amount, payment_processor, requested_at).await?;

        Ok(())
    }

    pub async fn get_summary(&self, from: &DateTime<Utc>, to: &DateTime<Utc>) -> anyhow::Result<PaymentsSummary> {
        let mut default = PaymentsSummaryDetails::default();
        let mut fallback = PaymentsSummaryDetails::default();

        let mut partitions = tokio::fs::read_dir(PAYMENTS_STORAGE_PATH).await?;
        while let Some(entry) = partitions.next_entry().await? {
            let path = entry.path();
            
            if !path.is_file() {
                continue;
            }

            let partition_key = path.file_name().unwrap().to_str().unwrap().parse::<i64>()?;
            if partition_key < from.timestamp_millis() / 1000 || partition_key > to.timestamp_millis() / 1000 {
                continue;
            }

            let mut csv_reader = csv::Reader::from_path(&path)?;

            for record in csv_reader.deserialize() {
                let (_, amount, payment_processor_id, _): PartitionRecord = record?;
                if payment_processor_id == "D" {
                    default.total_requests += 1;
                    default.total_amount += amount;
                } else {
                    fallback.total_requests += 1;
                    fallback.total_amount += amount;
                }
            }
        }

        Ok(PaymentsSummary { default, fallback })
    }
}