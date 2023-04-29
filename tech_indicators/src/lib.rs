mod adx_utills;
pub mod fuzzy;
mod rsi_utills;

use adx_utills::calc_adx;
use rsi_utills::{compute_rsi_vec, rma_rs};
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

fn none_iter<T: Copy>(n: usize) -> impl Iterator<Item = Option<T>> {
    std::iter::repeat(None).take(n)
}

pub fn to_option_vec<T: Copy>(src: &[T]) -> Vec<Option<T>> {
    src.iter().map(|x| Some(*x)).collect()
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
///
fn ewma(src: &[Option<f64>], alpha: f64, first: f64, n: usize) -> Vec<Option<f64>> {
    let mut res = src
        .iter()
        .take_while(|x| x.is_none())
        .copied()
        .chain(none_iter(n - 1))
        .chain(std::iter::once(Some(first)))
        .collect::<Vec<Option<f64>>>();

    for v in src.iter().skip_while(|x| x.is_none()).skip(n) {
        if let (Some(v), Some(last)) = (v, res.last()) {
            res.push(Some(alpha * v + (1f64 - alpha) * last.unwrap_or(0.0)));
        } else {
            res.push(None)
        }
    }
    res
}

fn windows_compute(
    src: &[Option<f64>],
    n: usize,
    f: impl Fn(&[Option<f64>]) -> Option<f64>,
) -> Vec<Option<f64>> {
    let skipped_src = src
        .iter()
        .skip_while(|x| x.is_none())
        .copied()
        .collect::<Vec<Option<f64>>>();

    none_iter(src.len() - skipped_src.len() + n - 1)
        .chain(skipped_src.windows(n).map(|xs| f(xs)))
        .collect()
}

/// Simple Moving Average
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}sma
pub fn sma(src: &[Option<f64>], n: usize) -> Vec<Option<f64>> {
    windows_compute(src, n, |xs| {
        Some(xs.iter().filter_map(|v| *v).sum::<f64>() / n as f64)
    })
}

/// Relative Moving Average
/// https://www.tradingcode.net/tradingview/relative-moving-average/
///
/// Need to guarantee that we only have None on the first part of src
fn rma(src: &[Option<f64>], n: usize) -> Vec<Option<f64>> {
    let alpha = 1f64 / n as f64;
    let sma = src.iter().filter_map(|v| *v).take(n).sum::<f64>() / n as f64;

    ewma(src, alpha, sma, n)
}

/// Exponential Moving Average
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}ema
pub fn ema(src: &[Option<f64>], n: usize) -> Vec<Option<f64>> {
    let alpha = 2f64 / (n as f64 + 1f64);
    let sma = src.iter().filter_map(|v| *v).take(n).sum::<f64>() / n as f64;

    ewma(src, alpha, sma, n)
}

/// Relative Strength Index (Smooth version?)
/// https://www.omnicalculator.com/finance/rsi
///
/// Deprecated
pub fn rsi_smooth(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    //compute_rsi_vec(data, n, smooth_rs)
    vec![]
}

/// Relative Strength Index (TradingView version)
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}rsi
pub fn rsi(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    compute_rsi_vec(data, n, rma_rs)
}

fn std_dev(src: &[Option<f64>], n: usize) -> Vec<Option<f64>> {
    windows_compute(src, n, |xs| {
        let mean = xs.iter().filter_map(|v| *v).sum::<f64>() / n as f64;
        Some(
            (xs.iter()
                .map(|x| {
                    if let Some(v) = x {
                        (v - mean).powi(2)
                    } else {
                        0.0
                    }
                })
                .sum::<f64>()
                / n as f64)
                .sqrt(),
        )
    })
}

/// return (sma, lower, upper)
fn bb_utill(src: &[f64], n: usize, mult: f64) -> Vec<(f64, f64, f64)> {
    let dt = to_option_vec(src);
    let basis = sma(&dt, n);
    let dev = std_dev(&dt, n);
    basis
        .iter()
        .zip(dev.iter())
        .map(|(sma, d)| {
            if let (Some(sma), Some(d)) = (sma, d) {
                (*sma, sma - mult * d, sma + mult * d)
            } else {
                (f64::NAN, f64::NAN, f64::NAN)
            }
        })
        .collect()
}

/// Bollinger Bands
/// https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}bb
pub fn bb(data: &[Ohlc], n: usize, mult: f64) -> Vec<DTValue<(f64, f64, f64)>> {
    let bb_res = bb_utill(&close_p(data), n, mult);
    embed_datetime(bb_res, data)
}

fn calc_macd_line(src: &[Option<f64>], short: usize, long: usize) -> Vec<Option<f64>> {
    if short > long {
        panic!("short should be less than long");
    }

    ema(src, short)
        .iter()
        .zip(ema(src, long).iter())
        .map(|(a, b)| {
            if let (Some(a), Some(b)) = (a, b) {
                Some(a - b)
            } else {
                None
            }
        })
        .collect()
}

/// Moving Average Convergence/Divergence
/// https://www.tradingview.com/support/solutions/43000502344-macd-moving-average-convergence-divergence/
///
/// Currently every parameters is hard-coded, and we are using sma instead of ema
pub fn macd(data: &[Ohlc]) -> Vec<DTValue<(f64, f64, f64)>> {
    let dt = to_option_vec(&close_p(data));

    // shorter ema - longer ema
    let macd_line = calc_macd_line(&dt, 12, 26);
    let signal_line = ema(&macd_line, 9);

    let hist = macd_line
        .iter()
        .zip(signal_line.iter())
        .map(|(a, b)| {
            if let (Some(a), Some(b)) = (a, b) {
                Some(a - b)
            } else {
                None
            }
        })
        .collect::<Vec<Option<f64>>>();

    let zipped = macd_line
        .iter()
        .zip(signal_line.iter())
        .zip(hist.iter())
        .map(|((a, b), c)| {
            (
                a.unwrap_or(f64::NAN),
                b.unwrap_or(f64::NAN),
                c.unwrap_or(f64::NAN),
            )
        });

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
    fn test_rma_with_none() {
        let data = vec![None, Some(1.0), Some(2.0), Some(3.0)];
        let rma = rma(&data, 2);

        for (v, expected) in rma.iter().zip(
            vec![
                None,
                None,
                Some(3.0 / 2.0),
                Some((1.0 / 2.0) * 3.0 + (1.0 / 2.0) * (3.0 / 2.0)),
            ]
            .iter(),
        ) {
            assert_eq!(v, expected);
        }
    }

    #[test]
    fn test_rma() {
        let data = vec![Some(0.5), Some(1.0), Some(2.0), Some(3.0)];
        let rma = rma(&data, 3);

        for (v, expected) in rma.iter().zip(
            vec![
                None,
                None,
                Some(3.5 / 3.0),
                Some((1.0 / 3.0) * 3.0 + (2.0 / 3.0) * (3.5 / 3.0)),
            ]
            .iter(),
        ) {
            if let (Some(v), Some(expected)) = (v, expected) {
                assert!(approx_eq!(f64, *v, *expected, epsilon = 1e-6));
            } else {
                assert_eq!(v, expected)
            }
        }
    }

    #[test]
    fn test_ema() {
        let src = vec![Some(1.0), Some(2.0), Some(3.0)];
        let length = 2;
        let ema_values = ema(&src, length);
        assert_eq!(ema_values.len(), src.len());

        for (v, expected) in ema_values.iter().zip(
            vec![
                None,
                Some(3.0 / 2.0),
                Some((2.0 / 3.0) * 3.0 + (1.0 / 3.0) * (3.0 / 2.0)),
            ]
            .iter(),
        ) {
            if let (Some(v), Some(expected)) = (v, expected) {
                assert!(approx_eq!(f64, *v, *expected, epsilon = 1e-6));
            } else {
                assert_eq!(v, expected)
            }
        }
    }

    #[test]
    fn test_sma() {
        let src = vec![Some(10.0), Some(20.0), Some(30.0), Some(40.0)];
        let sma_values = sma(&src, 3);

        let expected_sma = vec![
            None,
            None,
            Some(20.0), // (10 + 20 + 30) / 3
            Some(30.0), // (20 + 30 + 40) / 3
        ];

        assert_eq!(sma_values.len(), expected_sma.len());
        for (value, expected) in sma_values.iter().zip(expected_sma.iter()) {
            if let (Some(value), Some(expected)) = (value, expected) {
                assert!(approx_eq!(f64, *value, *expected, epsilon = 1e-6));
            } else {
                assert_eq!(value, expected)
            }
        }
    }
}
