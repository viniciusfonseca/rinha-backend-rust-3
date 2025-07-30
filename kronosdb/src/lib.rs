use std::collections::HashMap;

use crate::partition::Partition;

mod partition;

pub struct KronosDB {
    storage_path: String,
    files: scc::HashMap<u64, Partition>,
    first_access_locks: scc::HashMap<u64, tokio::sync::Mutex<()>>
}

type PartitionEntry<'a> = scc::hash_map::OccupiedEntry<'a, u64, Partition>;

impl KronosDB {

    pub fn connect(storage_path: String) -> Self {
        Self {
            storage_path,
            files: scc::HashMap::new(),
            first_access_locks: scc::HashMap::new()
        }
    }

    pub async fn get_partition<'a>(&'a self, timestamp: u64) -> anyhow::Result<PartitionEntry<'a>> {
        let partition_key = timestamp / 1000;
        match self.files.get_async(&partition_key).await {
            Some(partition) => Ok(partition),
            None => {
                let _lock = self.first_access_locks.entry_async(partition_key).await.or_insert_with(|| tokio::sync::Mutex::new(()));
                let _lock = _lock.lock().await;
                match self.files.get_async(&partition_key).await {
                    Some(partition) => Ok(partition),
                    None => {
                        let partition = Partition::create(&self.storage_path, partition_key).await?;
                        _ = self.files.insert_async(partition_key, partition).await;
                        Ok(self.files.get_async(&partition_key).await.unwrap())
                    }
                }
            }
        }
    }

}