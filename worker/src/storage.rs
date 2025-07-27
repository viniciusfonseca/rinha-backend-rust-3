use chrono::{DateTime, Utc};

use crate::payment_processor::PaymentProcessorIdentifier;


pub struct Storage {
    client: tokio_postgres::Client,
    insert_statement: tokio_postgres::Statement,
}

impl Storage {
    pub async fn init() -> Self {
        let (client, connection) = tokio_postgres::connect("host=localhost user=postgres dbname=mydb", tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        let insert_statement = client.prepare("INSERT INTO payments (amount, payment_processor_id, requested_at) VALUES ($1, $2, $3)").await.unwrap();

        Self { client, insert_statement }
    }

    pub async fn save_payment(&self, amount: f64, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {
        self.client.execute(&self.insert_statement, &[&amount, &payment_processor_id.to_string(), &requested_at]).await?;
        Ok(())
    }
}