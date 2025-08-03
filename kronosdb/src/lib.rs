use std::{collections::{btree_map::Entry, BTreeMap}, sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

pub use crate::{partition::Partition, record::Record};

mod partition;
mod record;

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

        let (start_sum, start_count) = match guard.last_entry() {
            Some(entry) => (entry.get().sum, entry.get().count),
            None => (0.0, 0)
        };
        
        let (mut sum, mut count) = guard.entry(partition_key)
            .or_insert_with(|| Partition::new(&self.storage_path, partition_key, start_sum, start_count))
            .insert_record(timestamp, Record::new(data)).await;

        loop {
            partition_key += 1;
            match guard.entry(partition_key) {
                Entry::Occupied(mut partition) => {
                    let partition = partition.get_mut();
                    partition.start_sum = sum;
                    partition.start_count = count;
                    (sum, count) = partition.update_sum_count().await;
                },
                Entry::Vacant(_) => break,
            }
        }

        Ok(())
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {

        let guard = self.partitions.read().await;
        let mut partitions = Vec::new();
        let tasks = guard.iter()
            .map(|(key, partition)| { partitions.push(key.to_string()); partition.persist_to_disk() })
            .collect::<Vec<_>>();
        
        _ = tokio::join!(
            futures::future::join_all(tasks),
            tokio::fs::write(format!("{}/partitions", self.storage_path), partitions.join("\n"))
        );

        Ok(())
    }

    pub async fn query_diff_from_fs(&self, from: &DateTime<Utc>, to: &DateTime<Utc>) -> anyhow::Result<(f64, i64)> {

        let from_key = from.timestamp();
        let to_key = to.timestamp();

        let start = Instant::now();
        let partition_keys = tokio::fs::read_to_string(format!("{}/partitions", self.storage_path)).await?;
        println!("read partition keys in {}ms", start.elapsed().as_millis());
        
        let mut partition_keys = partition_keys.split('\n').filter(|key| {
            !key.is_empty() && {
                let key = key.parse::<i64>().unwrap();
                key >= from_key && key <= to_key
            }
        })
        .collect::<Vec<_>>();

        if partition_keys.is_empty() {
            return Ok((0.0, 0));
        }

        partition_keys.sort();
        let mut iter = partition_keys.iter();

        let first_partition = iter.next().unwrap();
        let last_partition = iter.last().unwrap_or(first_partition);

        let from_key = from.timestamp_millis();
        let to_key = to.timestamp_millis();

        if let (Ok((first_sum, first_count)), Ok((last_sum, last_count))) = tokio::join!(
            async move {
                let first_partition_data = tokio::fs::read_to_string(format!("{}/{}", self.storage_path, first_partition)).await?;
                let records = first_partition_data.split('\n');
                let mut columns = Vec::new();
                for record in records {
                    if record.is_empty() {
                        continue;
                    }
                    columns = record.split(',').collect::<Vec<_>>();
                    let timestamp = columns[0].parse::<i64>()?;
                    if timestamp < from_key {
                        continue;
                    }
                    else { break }
                }
                return anyhow::Ok((columns[1].parse::<f64>()?, columns[2].parse::<i64>()?))
            },
            async move {
                let last_partition_data = tokio::fs::read_to_string(format!("{}/{}", self.storage_path, last_partition)).await?;
                let records = last_partition_data.split('\n');
                let mut columns = Vec::new();
                for record in records.rev() {
                    if record.is_empty() {
                        continue;
                    }
                    columns = record.split(',').collect::<Vec<_>>();
                    let timestamp = columns[0].parse::<i64>()?;
                    if timestamp > to_key {
                        continue;
                    }
                    else { break }
                }
                return anyhow::Ok((columns[1].parse::<f64>()?, columns[2].parse::<i64>()?))
            }
        ) {
            return Ok((last_sum - first_sum, last_count - first_count));
        }

        Ok((0.0, 0))
    }

}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use crate::Storage;
    
    fn timestamp(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap_or_default()
    }

    #[tokio::test]
    async fn test_persistence() -> anyhow::Result<()> {

        let storage_path = "/tmp/kronosdb-test".to_string();
        let storage = Storage::connect(storage_path);

        let start = 1754176191000 as i64;
        let data = 19.90;

        storage.insert_data(timestamp(start), data).await?;
        storage.insert_data(timestamp(start + 10), data).await?;
        storage.insert_data(timestamp(start + 20), data).await?;
        storage.insert_data(timestamp(start + 30), data).await?;
        storage.insert_data(timestamp(start + 40), data).await?;

        storage.persist_to_disk().await?;

        let (sum, count) = storage.query_diff_from_fs(&timestamp(start + 10), &timestamp(start + 30)).await?;
        assert_eq!(sum, data * 2.0);
        assert_eq!(count, 2);

        Ok(())
    }
}