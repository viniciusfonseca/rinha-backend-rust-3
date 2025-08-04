use std::sync::{atomic::Ordering, Arc};

use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use uuid::Uuid;

pub use crate::{partition::Partition, record::Record};

mod atomicf64;
mod partition;
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

        let (start_sum, start_count) = match self.partitions.back() {
            Some(entry) => (entry.value().sum.load(Ordering::SeqCst), entry.value().count.load(Ordering::SeqCst)),
            None => (0.0, 0)
        };
        
        let (mut sum, mut count) = self.partitions.get_or_insert_with(partition_key,
            || Partition::new(&self.storage_path, partition_key, start_sum, start_count))
            .value()
            .insert_record(timestamp, Record::new(data));

        let mut updating = false;
        for entry in self.partitions.iter() {
            if updating {
                let partition = entry.value();
                partition.start_sum.store(sum, Ordering::SeqCst);
                partition.start_count.store(count, Ordering::SeqCst);
                (sum, count) = partition.update_sum_count();
            }
            updating = *entry.key() == partition_key;
        }

        Ok(())
    }

    pub async fn persist_to_disk(&self) -> anyhow::Result<()> {
        
        let mut partitions = Vec::new();

        for entry in self.partitions.iter() {
            partitions.push(entry.key().to_string());
            let partition = entry.value();
            partition.persist_to_disk().await?;
        }

        tokio::fs::write(format!("{}/partitions", self.storage_path), partitions.join("\n")).await?;

        Ok(())
    }

    pub async fn query_diff_from_fs(&self, from: &DateTime<Utc>, to: &DateTime<Utc>) -> anyhow::Result<(f64, i64)> {

        let from_key = from.timestamp();
        let to_key = to.timestamp();
        let partition_keys = tokio::fs::read_to_string(format!("{}/partitions", self.storage_path)).await?;
        
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

        let (from_secs, from_nanos) = (from.timestamp(), 0);
        let (to_secs, to_nanos) = (to.timestamp(), 999_999_999);

        if let (Ok((first_sum, first_count)), Ok((last_sum, last_count))) = tokio::join!(
            async move {
                let first_partition_data = tokio::fs::read_to_string(format!("{}/{}", self.storage_path, first_partition)).await?;
                let records = first_partition_data.split('\n');
                let mut columns = Vec::new();
                for record in records {
                    columns = record.split(',').collect::<Vec<_>>();
                    let id = columns[0].parse()?;
                    let (seconds, nanos) = Uuid::from_u128(id).get_timestamp().unwrap().to_unix();
                    if (seconds < from_secs.try_into().unwrap()) || (seconds == from_secs.try_into().unwrap() && nanos < from_nanos) {
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
                    columns = record.split(',').collect::<Vec<_>>();
                    let id = columns[0].parse()?;
                    let (seconds, sub_seconds) = Uuid::from_u128(id).get_timestamp().unwrap().to_unix();
                    if (seconds > to_secs.try_into().unwrap()) || (seconds == to_secs.try_into().unwrap() && sub_seconds > to_nanos) {
                        continue;
                    }
                    else { break }
                }
                return anyhow::Ok((columns[1].parse::<f64>()?, columns[2].parse::<i64>()?))
            }
        ) {
            println!("{} {}", first_sum, last_sum);
            return Ok((last_sum - first_sum, last_count - first_count));
        }

        Ok((0.0, 0))
    }

}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::Storage;
    
    #[tokio::test]
    async fn test_persistence() -> anyhow::Result<()> {

        _ = tokio::fs::remove_dir_all("/tmp/kronosdb-test").await;
        let storage_path = "/tmp/kronosdb-test".to_string();
        let storage = Storage::connect(storage_path);

        let start = Utc::now();
        let data = 19.90;

        for _ in 0..15000 {
            storage.insert_data(Utc::now(), data)?;
        }

        storage.persist_to_disk().await?;

        let (sum, count) = storage.query_diff_from_fs(&start, &Utc::now()).await?;

        assert_eq!(sum, data * 15000.0);
        assert_eq!(count, 15000);
        
        Ok(())
    }
}