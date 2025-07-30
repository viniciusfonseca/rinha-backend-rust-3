use tokio::io::AsyncWriteExt;

pub struct Partition {
    pub key: u64,
    pub file: tokio::fs::File
}

impl Partition {
    pub async fn create(storage_path: &String, key: u64) -> anyhow::Result<Self> {

        let file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(format!("{storage_path}/{key}"))
            .await?;

        Ok(Self {
            key,
            file
        })
    }

    pub async fn write(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.file.write_all(data).await?;
        self.file.flush().await?;
        Ok(())
    }
}