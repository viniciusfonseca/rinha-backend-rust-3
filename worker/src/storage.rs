use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::payment_processor::PaymentProcessorIdentifier;

pub struct Storage {
    client: tokio_postgres::Client,
    insert_statement_default: tokio_postgres::Statement,
    insert_statement_fallback: tokio_postgres::Statement,
}

const PAYMENTS_INSERT_DEFAULT_QUERY: &'static str = "
    INSERT INTO payments_default (amount, requested_at)
    VALUES ($1, $2)
";

const PAYMENTS_INSERT_FALLBACK_QUERY: &'static str = "
    INSERT INTO payments_fallback (amount, requested_at)
    VALUES ($1, $2)
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

        let insert_statement_default = client.prepare(PAYMENTS_INSERT_DEFAULT_QUERY).await?;
        let insert_statement_fallback = client.prepare(PAYMENTS_INSERT_FALLBACK_QUERY).await?;

        Ok(Self {
            client,
            insert_statement_default,
            insert_statement_fallback,
        })
    }

    pub async fn save_payment(&self, amount: Decimal, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {
        match payment_processor_id {
            PaymentProcessorIdentifier::Default => self.client.execute(&self.insert_statement_default, &[&amount, &requested_at]).await?,
            PaymentProcessorIdentifier::Fallback => self.client.execute(&self.insert_statement_fallback, &[&amount, &requested_at]).await?,
        };
        Ok(())
    }
}