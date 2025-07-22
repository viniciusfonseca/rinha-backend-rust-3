
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::{payment_processor::PaymentProcessorIdentifier, worker::QueueEvent};

pub struct Partition {
    writer: tokio::io::WriteHalf<tokio::fs::File>,
}

pub type PartitionRecord = (String, Decimal, String, DateTime<Utc>);

impl Partition {
    pub async fn write(&mut self, correlation_id: &str, amount: Decimal, payment_processor_id: &str, requested_at: DateTime<Utc>) -> anyhow::Result<()> {

        let record = (correlation_id.to_string(), amount, payment_processor_id.to_string(), requested_at);
        let mut csv_writer = csv::Writer::from_writer(vec![]);

        csv_writer.serialize(record)?;

        self.writer.write(&mut csv_writer.into_inner()?).await?;
        self.writer.flush().await?;

        Ok(())
    }
}

pub struct PaymentsStorage {
    partitions: scc::HashMap<i64, Partition>,
    first_access_locks: scc::HashMap<i64, tokio::sync::Mutex<()>>
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

pub type PartitionEntry<'a> = scc::hash_map::OccupiedEntry<'a, i64, Partition>;

impl PaymentsStorage {
    pub fn new() -> PaymentsStorage {
        std::fs::create_dir_all(PAYMENTS_STORAGE_PATH).ok();
        PaymentsStorage {
            partitions: scc::HashMap::new(),
            first_access_locks: scc::HashMap::new()
        }
    }

    pub async fn get_partition(&self, partition_key: i64) -> Partition {

        // if let Some(partition) = self.partitions.get_async(&partition_key).await {
        //     return partition;
        // }

        // let lock = self.first_access_locks.entry(partition_key).or_insert_with(|| tokio::sync::Mutex::new(()));
        // let _guard = lock.lock().await;

        // if let Some(partition) = self.partitions.get_async(&partition_key).await {
        //     return partition;
        // }

        let file_path = format!("{PAYMENTS_STORAGE_PATH}/{partition_key}");
        let file_options = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await
            .expect("Failed to open or create partition file");
        let (_, writer) = tokio::io::split(file_options);
        Partition { writer }
        // let partition = Partition { writer };
        // _ = self.partitions.insert_async(partition_key, partition).await;
        // self.partitions.get_async(&partition_key).await.unwrap()
    }

    pub async fn insert_payment(&self, (correlation_id, amount): &QueueEvent, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {

        let partition_key = requested_at.timestamp_millis() / 1000; // Partition by second
        let payment_processor_id = match payment_processor_id {
            PaymentProcessorIdentifier::Default => "D",
            PaymentProcessorIdentifier::Fallback => "F",
        };

        // if let Some(mut partition) = self.partitions.get_async(&partition_key).await {
        //     partition.write(correlation_id, *amount, payment_processor, requested_at).await?;
        //     return Ok(())
        // }

        // let lock = self.first_access_locks.entry(partition_key).or_insert_with(|| tokio::sync::Mutex::new(()));
        // let _guard = lock.lock().await;

        // if let Some(mut partition) = self.partitions.get_async(&partition_key).await {
        //     partition.write(correlation_id, *amount, payment_processor, requested_at).await?;
        // }
        // else {
        //     let mut partition = self.create_partition(partition_key).await;
        //     partition.write(correlation_id, *amount, payment_processor, requested_at).await?;
        //     _ = self.partitions.insert_async(partition_key, partition);
        // }

        let bytes = format!("{},{},{},{}\n", correlation_id, amount, payment_processor_id, requested_at.to_rfc3339()).into_bytes();

        let file_path = format!("{PAYMENTS_STORAGE_PATH}/{partition_key}");
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await?
            .write_all(&bytes)
            .await?;

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

    pub async fn batch_insert(&self, records: &Vec<PartitionRecord>) -> anyhow::Result<()> {
        
        let mut partitions: HashMap<i64, BatchInsert> = HashMap::new();

        for record in records {
            let partition_key = record.3.timestamp_millis() / 1000; // Partition by second
            let entry = partitions.entry(partition_key).or_insert_with(|| BatchInsert {
                partition_key,
                data: Vec::new(),
            });
            let record = format!("{},{},{},{}\n", record.0, record.1, record.2, record.3.to_rfc3339());
            entry.data = [&entry.data, record.as_bytes()].concat();
        }

        for (partition_key, partition) in partitions {
            let file_path = format!("{PAYMENTS_STORAGE_PATH}/{partition_key}");
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await
                .expect("Failed to open or create partition file");
            file.write_all(&partition.data).await?;
            file.flush().await?;

            println!("Batch inserted {} records into partition {}", partition.data.len(), partition_key);
        }

        Ok(())
    }
}

struct BatchInsert {
    pub partition_key: i64,
    pub data: Vec<u8>,
}