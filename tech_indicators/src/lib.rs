pub mod fuzzy;
pub mod rsi_utills;

use rsi_utills::{compute_rsi_vec, rma_rs, smooth_rs};
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DTValue<T> {
    time: bson::DateTime,
    value: T,
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

/// Relative Strength Index (Smooth version?)
// https://www.omnicalculator.com/finance/rsi
pub fn rsi_smooth(data: &Vec<Ohlc>, n: usize) -> Vec<DTValue<f64>> {
    compute_rsi_vec(data, n, smooth_rs)
}

/// Relative Strength Index (TradingView version)
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}rsi
pub fn rsi(data: &Vec<Ohlc>, n: usize) -> Vec<DTValue<f64>> {
    compute_rsi_vec(data, n, rma_rs)
}

fn std_dev(data: &Vec<f64>, n: usize) -> Vec<f64> {
    data.windows(n)
        .map(|xs| {
            let mean = xs.iter().sum::<f64>() / n as f64;
            (xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64).sqrt()
        })
        .collect()
}

/// return (sma, lower, upper)
fn bb_utill(data: &Vec<f64>, n: usize, mult: f64) -> Vec<(f64, f64, f64)> {
    let basis = sma(data, n);
    let dev = std_dev(data, n);
    basis
        .iter()
        .zip(dev.iter())
        .map(|(sma, d)| (*sma, sma - mult * d, sma + mult * d))
        .collect()
}

/// Bollinger Bands
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}bb
pub fn bb(data: &Vec<Ohlc>, n: usize, mult: f64) -> Vec<DTValue<(f64, f64, f64)>> {
    let bb_res = bb_utill(&data.iter().map(|x| x.close).collect(), n, mult);

    data.iter()
        .skip(n)
        .zip(bb_res.iter())
        .map(|(ohlc, v)| DTValue {
            time: ohlc.time,
            value: *v,
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use float_cmp::approx_eq;

    #[test]
    fn test_sma() {
        let src = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0];
        let length = 3;
        let sma_values = sma(&src, length);

        let expected_sma = vec![
            20.0, // (10 + 20 + 30) / 3
            30.0, // (20 + 30 + 40) / 3
            40.0, // (30 + 40 + 50) / 3
            50.0, // (40 + 50 + 60) / 3
            60.0, // (50 + 60 + 70) / 3
        ];

        assert_eq!(sma_values.len(), expected_sma.len());
        for (value, expected) in sma_values.iter().zip(expected_sma.iter()) {
            assert!(
                approx_eq!(f64, *value, *expected, epsilon = 1e-6),
                "value: {}, expected: {}",
                value,
                expected
            );
        }
    }
}
