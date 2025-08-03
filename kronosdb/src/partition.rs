use std::{collections::BTreeMap, sync::{atomic::{AtomicBool, Ordering}, Arc}};

use chrono::{DateTime, Utc};

use crate::record::Record;

#[derive(Clone)]
pub struct Partition {
    key: i64,
    storage_path: String,
    records: BTreeMap<i64, Record>,
    should_persist: Arc<AtomicBool>,
    pub start_sum: f64,
    pub sum: f64,
    pub start_count: i64,
    pub count: i64
}

impl Partition {

    pub fn new(storage_path: &String, key: i64, start_sum: f64, start_count: i64) -> Self {

        Self {
            key,
            storage_path: storage_path.clone(),
            records: Default::default(),
            should_persist: Arc::new(AtomicBool::new(false)),
            start_sum,
            sum: start_sum,
            start_count,
            count: start_count
        }
    }

    pub async fn insert_record(&mut self, timestamp: DateTime<Utc>, record: Record) -> (f64, i64) {
        _ = self.records.insert(timestamp.timestamp_millis(), record);
        self.should_persist.store(true, std::sync::atomic::Ordering::Relaxed);
        self.update_sum_count().await
    }

    pub async fn update_sum_count(&mut self) -> (f64, i64) {
        let mut sum = self.start_sum;
        let mut count = self.start_count;
        for (_, record) in self.records.iter_mut() {
            sum += record.amount;
            count += 1;
            record.sum = sum;
            record.count = count;
        }
        self.sum = sum;
        self.count = count;
        (sum, count)
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {

        if !self.should_persist.load(Ordering::Relaxed) {
            return Ok(());
        }

        let partition_fs_path = format!("{}/{}", self.storage_path, self.key);

        let mut contents = String::new();

        for (timestamp, record) in self.records.iter() {
            let data = format!("{},{:.2},{}",
                timestamp,
                record.sum,
                record.count,
            );
            contents.push_str(&data);
            contents.push('\n');
        }

        tokio::fs::write(partition_fs_path, contents).await?;
        
        self.should_persist.store(false, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }
}