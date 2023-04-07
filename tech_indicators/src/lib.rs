pub mod rsi;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Ohlc {
    pub ticker: String,
    pub time: bson::DateTime,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: u64,
}

/// Simple Moving Average
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}sma
pub fn sma(src: &Vec<f64>, n: usize) -> Vec<f64> {
    src.windows(n)
        .map(|xs| xs.iter().sum::<f64>() / n as f64)
        .collect()
}

/// Relative Moving Average
/// https://www.tradingcode.net/tradingview/relative-moving-average/
pub fn rma(src: &Vec<f64>, n: usize) -> Vec<f64> {
    let alpha = 1f64 / n as f64;
    let sma = src.iter().take(n).sum::<f64>() / n as f64;
    let mut rma = vec![sma];

    for v in src.iter().skip(n) {
        rma.push(
            alpha * v + (1f64 - alpha) * rma.last().expect("rma should be impossible to be empty"),
        );
    }
    rma
}

#[cfg(test)]
mod test {
    use super::*;
    use float_cmp::approx_eq;

    #[test]
    fn test_rma() {
        let src = vec![
            100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0,
        ];
        let length = 3;
        let rma_values = rma(&src, length);

        let expected_rma = vec![
            101.0, // Initial SMA
            102.0, 103.0, 104.0, 105.0, 106.0,
        ];

        assert_eq!(rma_values.len(), expected_rma.len());
        for (value, expected) in rma_values.iter().zip(expected_rma.iter()) {
            assert!(
                approx_eq!(f64, *value, *expected, epsilon = 1e6),
                "value: {}, expected: {}",
                value,
                expected
            );
        }
    }
}
