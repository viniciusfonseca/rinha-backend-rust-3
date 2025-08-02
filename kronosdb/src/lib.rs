use chrono::{DateTime, Utc};
use scc::{ebr::Guard, HashIndex, TreeIndex};

use crate::{partition::Partition, record::Record};

mod atomicf64;
mod partition;
mod record;

pub struct Storage {
    storage_path: String,
    partitions: HashIndex<i64, Partition>,
    guard: Guard
}

impl Storage {

    pub fn connect(storage_path: String) -> Self {
        Self {
            storage_path,
            partitions: Default::default(),
            guard: Default::default()
        }
    }

    pub async fn query_diff_from_fs(&self, from: DateTime<Utc>, to: DateTime<Utc>) -> anyhow::Result<(f64, i64)> {

        let from_key = from.timestamp();
        let to_key = to.timestamp();

        let partition_keys = TreeIndex::new();
        let mut tasks = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.storage_path).await?;

        while let Some(entry) = entries.next_entry().await? {

            let partition_key = entry.file_name().to_string_lossy().parse::<i64>()?;
            if partition_key < from_key || partition_key > to_key {
                continue;
            }

            tasks.push(partition_keys.insert_async(partition_key, 0));
        }

        futures::future::join_all(tasks).await;

        let mut iter = partition_keys.iter(&self.guard);

        let (_, first_partition) = iter.next().unwrap();
        let (_, last_partition) = iter.last().unwrap();

        if let (Ok((first_sum, first_count)), Ok((last_sum, last_count))) = tokio::join!(
            async move {
                let first_partition_data = tokio::fs::read_to_string(format!("{}/{}", self.storage_path, first_partition)).await?;
                let records = first_partition_data.split('\n');
                for record in records {
                    let columns = record.split(',').collect::<Vec<_>>();
                    let timestamp = columns[0].parse::<i64>()?;
                    if timestamp < from_key {
                        continue;
                    }
                    return anyhow::Ok((columns[1].parse::<f64>()?, columns[2].parse::<i64>()?))
                }
                Ok((0.0, 0))
            },
            async move {
                let last_partition_data = tokio::fs::read_to_string(format!("{}/{}", self.storage_path, last_partition)).await?;
                let records = last_partition_data.split('\n');
                for record in records {
                    let columns = record.split(',').collect::<Vec<_>>();
                    let timestamp = columns[0].parse::<i64>()?;
                    if timestamp > to_key {
                        continue;
                    }
                    return anyhow::Ok((columns[1].parse::<f64>()?, columns[2].parse::<i64>()?))
                }
                Ok((0.0, 0))
            }
        ) {
            return Ok((last_sum - first_sum, last_count - first_count));
        }

        Ok((0.0, 0))
    }

    pub async fn insert_data(&self, timestamp: DateTime<Utc>, record: Record) -> anyhow::Result<()> {

        let mut partition_key = timestamp.timestamp();

        let (mut sum, mut count) = self.partitions.entry_async(partition_key).await
            .or_insert_with(|| Partition::new(&self.storage_path, partition_key))
            .get()
            .insert_record(timestamp, record).await;

        loop {
            partition_key += 1;
            match self.partitions.entry_async(partition_key).await {
                scc::hash_index::Entry::Occupied(partition) => {
                    let partition = partition.get();
                    partition.start_sum.store(sum, std::sync::atomic::Ordering::SeqCst);
                    partition.start_count.store(count, std::sync::atomic::Ordering::SeqCst);
                    (sum, count) = partition.update_sum_count();
                },
                scc::hash_index::Entry::Vacant(_) => break,
            }
        }

        Ok(())
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {

        futures::future::join_all(
            self.partitions.iter(&self.guard)
                .map(|(_, partition)| partition.persist_to_disk())
        ).await;

        Ok(())
    }

}