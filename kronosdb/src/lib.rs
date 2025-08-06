use std::sync::{atomic::Ordering, Arc};

use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;

pub use crate::{partition::Partition, record::Record};

mod atomicf64;
mod partition;
mod query;
mod record;

#[derive(Clone)]
pub struct Storage {
    storage_path: String,
    partitions: Arc<SkipMap<i64, Partition>>,
}

impl Storage {

    pub fn connect(storage_path: String) -> Self {
        std::fs::create_dir_all(&storage_path).unwrap();
        Self {
            storage_path,
            partitions: Arc::new(SkipMap::new()),
        }
    }

    pub fn insert_data(&self, timestamp: DateTime<Utc>, data: f64) -> anyhow::Result<()> {

        let partition_key = timestamp.timestamp();

        self.partitions.get_or_insert_with(partition_key, || Partition::new(&self.storage_path, partition_key))
            .value()
            .insert_record(timestamp, Record::new(data));

        Ok(())
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {
        
        let mut partitions = Vec::new();

        let mut sum = 0.0;
        let mut count = 0;

        for entry in self.partitions.iter() {
            partitions.push(entry.key().to_string());
            let partition = entry.value();

            partition.start_sum.store(sum, Ordering::SeqCst);
            partition.start_count.store(count, Ordering::SeqCst);
            (sum, count) = partition.update_sum_count();

            partition.persist_to_disk().await?;
        }

        tokio::fs::write(format!("{}/partitions", self.storage_path), partitions.join("\n")).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{SubsecRound, Utc};

    use crate::Storage;
    
    #[tokio::test]
    async fn test_persistence() -> anyhow::Result<()> {

        _ = tokio::fs::remove_dir_all("/tmp/kronosdb-test").await;
        let storage_path = "/tmp/kronosdb-test".to_string();
        let storage = Storage::connect(storage_path);

        let start = Utc::now().trunc_subsecs(6);
        let data = 19.90;

        for _ in 0..15000 {
            storage.insert_data(Utc::now().trunc_subsecs(6), data)?;
        }

        storage.persist_to_disk().await?;

        let (sum, count) = storage.query_diff_from_fs(&start, &Utc::now()).await?;

        assert_eq!(sum, data * 15000.0);
        assert_eq!(count, 15000);
        
        Ok(())
    }
}