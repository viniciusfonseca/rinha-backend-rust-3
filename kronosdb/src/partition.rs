use std::sync::{atomic::{AtomicBool, AtomicI64, Ordering}, Arc};

use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;

use crate::{atomicf64::AtomicF64, record::Record};

#[derive(Clone)]
pub struct Partition {
    key: i64,
    storage_path: String,
    records: Arc<SkipMap<i64, Record>>,
    should_persist: Arc<AtomicBool>,
    pub start_sum: Arc<AtomicF64>,
    pub sum: Arc<AtomicF64>,
    pub start_count: Arc<AtomicI64>,
    pub count: Arc<AtomicI64>,
}

impl Partition {

    pub fn new(storage_path: &String, key: i64, start_sum: f64, start_count: i64) -> Self {
        Self {
            key,
            storage_path: storage_path.clone(),
            records: Default::default(),
            should_persist: Arc::new(AtomicBool::new(false)),
            start_sum: Arc::new(AtomicF64::new(start_sum)),
            sum: Arc::new(AtomicF64::new(start_sum)),
            start_count: Arc::new(AtomicI64::new(start_count)),
            count: Arc::new(AtomicI64::new(start_count)),
        }
    }

    pub fn insert_record(&self, timestamp: DateTime<Utc>, record: Record) -> (f64, i64) {
        _ = self.records.insert(timestamp.timestamp_millis(), record);
        let (sum, count) = self.update_sum_count();
        self.should_persist.store(true, std::sync::atomic::Ordering::Relaxed);
        (sum, count)
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

        if !self.should_persist.load(Ordering::Relaxed) {
            return Ok(());
        }

        let partition_fs_path = format!("{}/{}", self.storage_path, self.key);

        let contents = self.records.iter()
            .map(|entry| {
                let (timestamp, record) = (entry.key(), entry.value());
                let data = format!("{},{:.2},{}",
                    timestamp,
                    record.sum.load(Ordering::SeqCst),
                    record.count.load(Ordering::SeqCst),
                );
                data
            })
            .collect::<Vec<String>>()
            .join("\n");

        tokio::fs::write(partition_fs_path, contents).await?;
        
        self.should_persist.store(false, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }
}