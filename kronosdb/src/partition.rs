use std::{collections::BTreeMap, sync::{atomic::{AtomicBool, AtomicI64, Ordering}, Arc}};

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::{atomicf64::AtomicF64, record::Record};

#[derive(Clone)]
pub struct Partition {
    key: i64,
    storage_path: String,
    records: Arc<RwLock<BTreeMap<i64, Record>>>,
    should_persist: Arc<AtomicBool>,
    pub start_sum: Arc<AtomicF64>,
    sum: Arc<AtomicF64>,
    pub start_count: Arc<AtomicI64>,
    count: Arc<AtomicI64>
}

impl Partition {

    pub fn new(storage_path: &String, key: i64) -> Self {

        Self {
            key,
            storage_path: storage_path.clone(),
            records: Default::default(),
            should_persist: Arc::new(AtomicBool::new(false)),
            start_sum: Arc::new(AtomicF64::new(0.0)),
            sum: Arc::new(AtomicF64::new(0.0)),
            start_count: Arc::new(AtomicI64::new(0)),
            count: Arc::new(AtomicI64::new(0))
        }
    }

    pub async fn insert_record(&self, timestamp: DateTime<Utc>, record: Record) -> (f64, i64) {
        let mut records = self.records.write().await;
        _ = records.insert(timestamp.timestamp_millis(), record);
        self.should_persist.store(true, std::sync::atomic::Ordering::Relaxed);
        self.update_sum_count().await
    }

    pub async fn update_sum_count(&self) -> (f64, i64) {
        let mut sum = self.start_sum.load(Ordering::SeqCst);
        let mut count = self.start_count.load(Ordering::SeqCst);
        let read_guard = self.records.read().await;
        for (_, record) in read_guard.iter() {
            sum += record.amount.load(Ordering::SeqCst);
            self.sum.store(sum, Ordering::SeqCst);
            count += 1;
            self.count.store(count, Ordering::SeqCst);
        }
        self.sum.store(sum, Ordering::SeqCst);
        self.count.store(count, Ordering::SeqCst);
        (sum, count)
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {

        if !self.should_persist.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        let partition_fs_path = format!("{}/{}", self.storage_path, self.key);

        let mut contents = String::new();

        let read_guard = self.records.read().await;
        for (timestamp, record) in read_guard.iter() {
            let data = format!("{},{},{}",
                timestamp,
                record.sum.load(std::sync::atomic::Ordering::SeqCst),
                record.count.load(std::sync::atomic::Ordering::SeqCst),
            );
            contents.push_str(&data);
            contents.push('\n');
        }

        tokio::fs::write(partition_fs_path, contents).await?;
        
        self.should_persist.store(false, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }
}