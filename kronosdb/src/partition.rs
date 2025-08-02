use std::sync::{atomic::{AtomicBool, AtomicI64, Ordering}, Arc};

use chrono::{DateTime, Utc};
use scc::{ebr::Guard, TreeIndex};

use crate::{atomicf64::AtomicF64, record::Record};

#[derive(Clone)]
pub struct Partition {
    key: i64,
    storage_path: String,
    records: TreeIndex<i64, Record>,
    guard: Arc<Guard>,
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
            guard: Default::default(),
            should_persist: Arc::new(AtomicBool::new(false)),
            start_sum: Arc::new(AtomicF64::new(0.0)),
            sum: Arc::new(AtomicF64::new(0.0)),
            start_count: Arc::new(AtomicI64::new(0)),
            count: Arc::new(AtomicI64::new(0))
        }
    }

    pub async fn insert_record(&self, timestamp: DateTime<Utc>, record: Record) -> (f64, i64) {
        _ = self.records.insert_async(timestamp.timestamp_millis(), record).await;
        self.should_persist.store(true, std::sync::atomic::Ordering::Relaxed);
        self.update_sum_count()
    }

    pub fn update_sum_count(&self) -> (f64, i64) {
        let mut sum = self.start_sum.load(Ordering::SeqCst);
        let mut count = self.start_count.load(Ordering::SeqCst);
        for (_, record) in self.records.iter(&self.guard) {
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

        for (timestamp, record) in self.records.iter(&self.guard) {
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