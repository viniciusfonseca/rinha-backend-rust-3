use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::prelude::FromRow;

use crate::{app_state::AppState, payment_processor::PaymentProcessorIdentifier, worker::QueueEvent};

#[derive(FromRow, Default, Clone, Debug)]
pub struct PaymentsSummaryDetailsRow {
    pub total_requests: i64,
    pub total_amount: Decimal,
    pub payment_processor: String
}

impl Into<PaymentsSummaryDetails> for PaymentsSummaryDetailsRow {
    fn into(self) -> PaymentsSummaryDetails {
        PaymentsSummaryDetails {
            total_requests: self.total_requests,
            total_amount: self.total_amount
        }
    }
}

#[derive(Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PaymentsSummaryDetails {
    pub total_requests: i64,
    pub total_amount: Decimal
}

#[derive(Serialize, Default, Debug)]
pub struct PaymentsSummary {
    pub default: PaymentsSummaryDetails,
    pub fallback: PaymentsSummaryDetails
}

impl PaymentsSummary {
    fn get(query_result: Vec<PaymentsSummaryDetailsRow>, payment_processor: String) -> PaymentsSummaryDetails {
        match query_result.iter().find(|row| row.payment_processor == payment_processor) {
            Some(element) => element.clone().into(),
            None => PaymentsSummaryDetails::default()
        }
    }
}

pub struct PaymentsStorage;

const INSERT_STATEMENT: &'static str = "INSERT INTO payments (correlation_id, amount, payment_processor, requested_at) VALUES ($1, $2, $3, $4)";

const PAYMENTS_SUMMARY_QUERY: &'static str = "
SELECT COUNT(correlation_id) as total_requests, COALESCE(SUM(amount), 0) as total_amount, payment_processor
FROM PAYMENTS
WHERE requested_at BETWEEN $1 AND $2
GROUP BY payment_processor";

impl PaymentsStorage {

    pub async fn insert_payment(&self, state: &Arc<AppState>, (correlation_id, amount): &QueueEvent, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {

        let payment_processor = match payment_processor_id {
            PaymentProcessorIdentifier::Default => "D",
            PaymentProcessorIdentifier::Fallback => "F",
        };

        sqlx::query(INSERT_STATEMENT)
            .bind(correlation_id)
            .bind(amount)
            .bind(payment_processor)
            .bind(requested_at)
            .execute(&state.pg_pool)
            .await?;

        Ok(())
    }

    pub async fn get_summary(&self, state: &Arc<AppState>, from: &Option<&DateTime<Utc>>, to: &Option<&DateTime<Utc>>) -> Result<PaymentsSummary, sqlx::Error> {
        sqlx::query_as::<_, PaymentsSummaryDetailsRow>(PAYMENTS_SUMMARY_QUERY)
            .bind(from)
            .bind(to)
            .fetch_all(&state.pg_pool)
            .await
            .map(|rows| {
                let default = PaymentsSummary::get(rows.clone(), "D".to_string());
                let fallback = PaymentsSummary::get(rows, "F".to_string());
                PaymentsSummary { default, fallback }
            })
    }
}