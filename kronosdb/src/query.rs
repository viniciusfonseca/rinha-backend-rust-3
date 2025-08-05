use chrono::{DateTime, SubsecRound, Timelike, Utc};

use crate::Storage;

impl Storage {

    pub async fn query_diff_from_fs(&self, from: &DateTime<Utc>, to: &DateTime<Utc>) -> anyhow::Result<(f64, i64)> {

        let from = from.trunc_subsecs(6);
        let to = to.trunc_subsecs(6);

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

        let from_nanos = from.nanosecond();
        let to_nanos = to.nanosecond();

        if let (Ok((first_sum, first_count)), Ok((last_sum, last_count))) = tokio::join!(
            async move {
                let first_partition_data = tokio::fs::read_to_string(format!("{}/{}", self.storage_path, first_partition)).await?;
                let records = first_partition_data.split('\n');
                let mut columns = Vec::new();
                for record in records {
                    columns = record.split(',').collect::<Vec<_>>();
                    let nanos = columns[0].parse::<u32>()?;
                    if nanos < from_nanos {
                        continue;
                    }
                    else { break }
                }
                let amount = columns[1].parse::<f64>()?;
                let sum = columns[2].parse::<f64>()?;
                let count = columns[3].parse::<i64>()?;
                return anyhow::Ok((sum - amount, count - 1))
            },
            async move {
                let last_partition_data = tokio::fs::read_to_string(format!("{}/{}", self.storage_path, last_partition)).await?;
                let records = last_partition_data.split('\n');
                let mut columns = Vec::new();
                for record in records.rev() {
                    columns = record.split(',').collect::<Vec<_>>();
                    let nanos = columns[0].parse::<u32>()?;
                    if nanos > to_nanos {
                        continue;
                    }
                    else { break }
                }
                let sum = columns[2].parse::<f64>()?;
                let count = columns[3].parse::<i64>()?;
                return anyhow::Ok((sum, count))
            }
        ) {
            println!("{} {}", first_sum, last_sum);
            return Ok((last_sum - first_sum, last_count - first_count));
        }

        Ok((0.0, 0))
    }
}