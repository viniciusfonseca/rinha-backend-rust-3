use chrono::{DateTime, Utc};

use crate::{payment_processor::PaymentProcessorIdentifier, WorkerState};

impl WorkerState {

    pub async fn save_payment(&self, amount: f64, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {

        let url = format!("{db_url}/api/v2/write?bucket=db&org=myorg", db_url = self.db_url);

        let payment_processor_id = payment_processor_id.to_string();
        let requested_at = requested_at.timestamp_millis();

        let res = self.reqwest_client.post(url)
            .header("Authorization", "Token sec-token")
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(format!("payments,payment_processor_id={payment_processor_id} amount={amount} {requested_at}"))
            .send()
            .await?;

        let status_code = res.status().as_u16();
        let res_text = res.text().await.unwrap();

        println!("SAVE PAYMENT status code: {status_code} - {res_text}");

        Ok(())
    }
}