#[derive(Clone)]
pub struct Record {
    pub amount: f64,
    pub sum: f64,
    pub count: i64
}

impl Record {
    pub fn new(amount: f64) -> Self {
        Self {
            amount,
            sum: 0.0,
            count: 0
        }
    }
}