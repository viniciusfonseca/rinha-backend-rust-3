use std::{collections::{btree_map::Entry, BTreeMap}, sync::Arc};

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

pub use crate::{partition::Partition, record::Record};

mod atomicf64;
mod partition;
mod query;
mod record;

pub struct ReadOnlyStorage {
    storage_path: String,
}

#[derive(Clone)]
pub struct Storage {
    storage_path: String,
    partitions: Arc<RwLock<BTreeMap<i64, Partition>>>,
}

impl Storage {

    pub fn connect(storage_path: String) -> Self {
        std::fs::create_dir_all(&storage_path).unwrap();
        Self {
            storage_path,
            partitions: Default::default(),
        }
    }

    pub async fn insert_data(&self, timestamp: DateTime<Utc>, data: f64) -> anyhow::Result<()> {

        let mut partition_key = timestamp.timestamp();

        let mut guard = self.partitions.write().await;
        
        let (mut sum, mut count) = guard.entry(partition_key)
            .or_insert_with(|| Partition::new(&self.storage_path, partition_key))
            .insert_record(timestamp, Record::new(data)).await;

        loop {
            partition_key += 1;
            match guard.entry(partition_key) {
                Entry::Occupied(partition) => {
                    let partition = partition.get();
                    partition.start_sum.store(sum, std::sync::atomic::Ordering::SeqCst);
                    partition.start_count.store(count, std::sync::atomic::Ordering::SeqCst);
                    (sum, count) = partition.update_sum_count().await;
                },
                Entry::Vacant(_) => break,
            }
        }

        Ok(())
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {

        let guard = self.partitions.read().await;

        futures::future::join_all(
            guard.iter()
                .map(|(_, partition)| partition.persist_to_disk())
        ).await;

        Ok(())
    }

}