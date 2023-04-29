mod adx_utills;
pub mod fuzzy;
mod rsi_utills;

use adx_utills::calc_adx;
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

fn close_p(data: &[Ohlc]) -> Vec<f64> {
    data.iter().map(|x| x.close).collect()
}

fn nan_iter(n: usize) -> impl Iterator<Item = f64> {
    std::iter::repeat(f64::NAN).take(n)
}

fn none_iter<T: Copy>(n: usize) -> impl Iterator<Item = Option<T>> {
    std::iter::repeat(None).take(n)
}

/// Embed datetiume from [Ohlc] to Iterator of T, and we need to ensure that the data and ohlc order are matched.
///
/// Note that this also consume the data iterator.
fn embed_datetime<T, I: IntoIterator<Item = T>>(data: I, ohlc: &[Ohlc]) -> Vec<DTValue<T>> {
    ohlc.iter()
        .zip(data.into_iter())
        .map(|(x, v)| DTValue {
            time: x.time,
            value: v,
        })
        .collect()
}

/// Exponential Weighted Moving Average
/// https://corporatefinanceinstitute.com/resources/capital-markets/exponentially-weighted-moving-average-ewma/#:~:text=What%20is%20the%20Exponentially%20Weighted,technical%20analysis%20and%20volatility%20modeling.
pub fn ewma(src: &[f64], alpha: f64, first: f64, n: usize) -> Vec<f64> {
    let mut res = nan_iter(n - 1).chain(vec![first]).collect::<Vec<f64>>();

    for v in src.iter().skip(n) {
        res.push(
            alpha * v + (1f64 - alpha) * res.last().expect("res should be impossible to be empty"),
        )
    }
    res
}

/// Simple Moving Average
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}sma
pub fn sma(src: &[f64], n: usize) -> Vec<f64> {
    nan_iter(n - 1)
        .chain(src.windows(n).map(|xs| xs.iter().sum::<f64>() / n as f64))
        .collect()
}

/// Relative Moving Average
/// https://www.tradingcode.net/tradingview/relative-moving-average/
pub fn rma(src: &[f64], n: usize) -> Vec<f64> {
    let alpha = 1f64 / n as f64;
    let sma = src.iter().take(n).sum::<f64>() / n as f64;

    ewma(src, alpha, sma, n)
}

/// Exponential Moving Average
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}ema
pub fn ema(src: &[f64], n: usize) -> Vec<f64> {
    let alpha = 2f64 / (n as f64 + 1f64);
    let sma = src.iter().take(n).sum::<f64>() / n as f64;

    ewma(src, alpha, sma, n)
}

/// Relative Strength Index (Smooth version?)
// https://www.omnicalculator.com/finance/rsi
pub fn rsi_smooth(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    compute_rsi_vec(data, n, smooth_rs)
}

/// Relative Strength Index (TradingView version)
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}rsi
pub fn rsi(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    compute_rsi_vec(data, n, rma_rs)
}

fn std_dev(src: &[f64], n: usize) -> Vec<f64> {
    nan_iter(n - 1)
        .chain(src.windows(n).map(|xs| {
            let mean = xs.iter().sum::<f64>() / n as f64;
            (xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64).sqrt()
        }))
        .collect()
}

/// return (sma, lower, upper)
fn bb_utill(data: &[f64], n: usize, mult: f64) -> Vec<(f64, f64, f64)> {
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
pub fn bb(data: &[Ohlc], n: usize, mult: f64) -> Vec<DTValue<(f64, f64, f64)>> {
    let bb_res = bb_utill(&close_p(data), n, mult);
    embed_datetime(bb_res, data)
}

fn calc_macd_line(src: &[f64], short: usize, long: usize) -> Vec<f64> {
    if short > long {
        panic!("short should be less than long");
    }

    ema(src, short)
        .iter()
        .zip(ema(src, long).iter())
        .map(|(a, b)| a - b)
        .collect::<Vec<f64>>()
}

fn calc_signal_line(macd_line: &[f64], n: usize) -> Vec<f64> {
    macd_line
        .iter()
        .take_while(|x| x.is_nan())
        .copied()
        .chain(ema(
            &macd_line
                .iter()
                .skip_while(|x| x.is_nan())
                .copied()
                .collect::<Vec<f64>>(),
            n,
        ))
        .collect()
}

/// Moving Average Convergence/Divergence
/// https://www.tradingview.com/support/solutions/43000502344-macd-moving-average-convergence-divergence/
///
/// Currently every parameters is hard-coded, and we are using sma instead of ema
pub fn macd(data: &[Ohlc]) -> Vec<DTValue<(f64, f64, f64)>> {
    let dt = close_p(data);

    // shorter ema - longer ema
    let macd_line = calc_macd_line(&dt, 12, 26);
    let signal_line = calc_signal_line(&macd_line, 9);

    let hist = macd_line
        .iter()
        .zip(signal_line.iter())
        .map(|(a, b)| a - b)
        .collect::<Vec<f64>>();

    let zipped = macd_line
        .iter()
        .zip(signal_line.iter())
        .zip(hist.iter())
        .map(|((a, b), c)| (*a, *b, *c));

    embed_datetime(zipped, data)
}

pub fn adx(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    calc_adx(data, n)
}

#[cfg(test)]
mod test {
    use super::*;
    use float_cmp::approx_eq;

    #[test]
    fn test_ema() {
        let src = vec![1.0, 2.0, 3.0];
        let length = 2;
        let ema_values = ema(&src, length);
        assert_eq!(ema_values.len(), src.len());

        for (value, expected) in ema_values
            .iter()
            .zip(vec![f64::NAN, 3.0 / 2.0, 2.0 / 3.0 * 3.0 + 1.0 / 3.0 * 3.0 / 2.0].iter())
        {
            assert!(approx_eq!(f64, *value, *expected, ulps = 2));
        }
    }

    #[test]
    fn test_sma() {
        let src = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0];
        let length = 3;
        let sma_values = sma(&src, length);

        let expected_sma = vec![
            f64::NAN,
            f64::NAN,
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
