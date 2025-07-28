use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::payment_processor::PaymentProcessorIdentifier;

pub struct Storage {
    client: tokio_postgres::Client,
    insert_statement: tokio_postgres::Statement,
}

const PAYMENTS_INSERT_QUERY: &'static str = "
    INSERT INTO payments (amount, payment_processor_id, requested_at)
    VALUES ($1, $2, $3)
";

impl Storage {
    pub async fn init() -> anyhow::Result<Self> {
        let psql_url = std::env::var("DATABASE_URL")?;
        let (client, connection) = tokio_postgres::connect(&psql_url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {e}");
            }
        });

        let insert_statement = client.prepare(PAYMENTS_INSERT_QUERY).await?;

        Ok(Self { client, insert_statement })
    }

    pub async fn save_payment(&self, amount: Decimal, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {
        self.client.execute(&self.insert_statement, &[&amount, &payment_processor_id.to_string(), &requested_at]).await?;
        Ok(())
    }
}