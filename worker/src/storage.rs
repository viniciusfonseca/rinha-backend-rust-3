use std::collections::HashMap;
use chrono::{DateTime, Utc};

use crate::{payment_processor::PaymentProcessorIdentifier, WorkerState};

impl WorkerState {

    pub async fn init_db(&self) -> anyhow::Result<()> {

        let url = format!("{db_url}/query", db_url = self.db_url);

        let mut form_data = HashMap::new();
        form_data.insert("q", "CREATE DATABASE db");

        let status_code = self.reqwest_client.post(url)
            .form(&form_data).send()
            .await?
            .status()
            .as_u16();

        println!("CREATE DB status code: {status_code}");

        Ok(())
    }

    pub async fn save_payment(&self, amount: f64, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {

        let url = format!("{db_url}/write?db=db", db_url = self.db_url);

        let payment_processor_id = payment_processor_id.to_string();
        let requested_at = requested_at.timestamp_millis();

        self.reqwest_client.post(url)
            .body(format!("payments,payment_processor_id={payment_processor_id} amount={amount} {requested_at}"))
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}