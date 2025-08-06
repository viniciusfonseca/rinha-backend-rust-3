use std::sync::{atomic::{AtomicBool, AtomicI64, Ordering}, Arc};

use chrono::{DateTime, Timelike, Utc};
use crossbeam_skiplist::SkipMap;
use uuid::{ContextV7, NoContext, Timestamp, Uuid};

use crate::{atomicf64::AtomicF64, record::Record};

#[derive(Clone)]
pub struct Partition {
    key: i64,
    storage_path: String,
    records: Arc<SkipMap<u128, Record>>,
    should_persist: Arc<AtomicBool>,
    pub start_sum: Arc<AtomicF64>,
    pub sum: Arc<AtomicF64>,
    pub start_count: Arc<AtomicI64>,
    pub count: Arc<AtomicI64>,
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
            count: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn insert_record(&self, timestamp: DateTime<Utc>, record: Record) {
        let timestamp = Timestamp::from_unix(NoContext, timestamp.nanosecond() as u64, 0); // essa é uma das maiores loucuras que já fiz programando
        let id = Uuid::new_v7(timestamp);
        _ = self.records.insert(id.as_u128(), record);
        self.should_persist.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn update_sum_count(&self) -> (f64, i64) {
        let mut sum = self.start_sum.load(Ordering::SeqCst);
        let mut count = self.start_count.load(Ordering::SeqCst);
        for entry in self.records.iter() {
            let record = entry.value();
            sum += record.amount;
            count += 1;
            record.sum.store(sum, Ordering::SeqCst);
            record.count.store(count, Ordering::SeqCst);
        }
        self.sum.store(sum, Ordering::SeqCst);
        self.count.store(count, Ordering::SeqCst);
        (sum, count)
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {

        if !self.should_persist.load(Ordering::Relaxed) || self.records.is_empty() {
            return Ok(());
        }

        let partition_fs_path = format!("{}/{}", self.storage_path, self.key);

        let contents = self.records.iter()
            .map(|entry| {
                let (nanos, record) = (entry.key(), entry.value());
                record.to_string(nanos)
            })
            .collect::<Vec<_>>();

        tokio::fs::write(partition_fs_path, contents.join("\n")).await?;
        
        self.should_persist.store(false, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }
}