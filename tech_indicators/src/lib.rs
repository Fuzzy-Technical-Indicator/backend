mod adx_utills;
pub mod fuzzy;
mod rsi_utills;

use adx_utills::calc_adx;
use itertools::izip;
use rsi_utills::{compute_rsi_vec, rma_rs};
use serde::{Deserialize, Serialize};
use rayon::prelude::*;

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

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Hash)]
pub struct DTValue<T> {
    time: bson::DateTime,
    value: T,
}

fn close_p(data: &[Ohlc]) -> Vec<f64> {
    data.par_iter().map(|x| x.close).collect()
}

fn none_iter<T: Copy>(n: usize) -> impl Iterator<Item = Option<T>> {
    std::iter::repeat(None).take(n)
}

fn none_par_iter<T: Send + Sync + Clone>(n: usize) -> rayon::iter::RepeatN<Option<T>> {
    rayon::iter::repeat(None::<T>).take(n)
}

pub fn to_option_vec<T: Copy>(src: &[T]) -> Vec<Option<T>> {
    src.iter().map(|x| Some(*x)).collect()
}

/// Embed datetiume from [Ohlc] to Iterator of T, and we need to ensure that the data and ohlc order are matched.
///
/// Note that this also consume the data iterator.
fn embed_datetime<T>(data: &[T], ohlc: &[Ohlc]) -> Vec<DTValue<T>>
where T: Send + Sync + Copy 
{
    ohlc.par_iter()
        .zip(data.par_iter())
        .map(|(x, v)| DTValue {
            time: x.time,
            value: *v,
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
    f: impl Fn(&[Option<f64>]) -> Option<f64> + Send + Sync,
) -> Vec<Option<f64>> {
    let skipped_src = src
        .iter()
        .skip_while(|x| x.is_none())
        .copied()
        .collect::<Vec<Option<f64>>>();

    none_par_iter(src.len() - skipped_src.len() + n - 1)
        .chain(skipped_src.par_windows(n).map(f))
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
        .par_iter()
        .zip(dev.par_iter())
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
    embed_datetime(&bb_res, data)
}

fn calc_macd_line(
    src: &[Option<f64>],
    short: usize,
    long: usize,
    f: impl Fn(&[Option<f64>], usize) -> Vec<Option<f64>>,
) -> Vec<Option<f64>> {
    if short > long {
        panic!("short should be less than long");
    }

    f(src, short)
        .par_iter()
        .zip(f(src, long).par_iter())
        .map(|(a, b)| {
            if let (Some(a), Some(b)) = (a, b) {
                Some(a - b)
            } else {
                None
            }
        })
        .collect()
}

/// xs - ys
fn vec_diff(xs: &[Option<f64>], ys: &[Option<f64>]) -> Vec<Option<f64>> {
    xs.par_iter()
        .zip(ys.par_iter())
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
    let macd_line = calc_macd_line(&dt, 12, 26, ema);
    let signal_line = ema(&macd_line, 9);
    let hist = vec_diff(&macd_line, &signal_line);

    let zipped = macd_line
        .par_iter()
        .zip(signal_line.par_iter())
        .zip(hist.par_iter())
        .map(|((a, b), c)| {
            (
                a.unwrap_or(f64::NAN),
                b.unwrap_or(f64::NAN),
                c.unwrap_or(f64::NAN),
            )
        })
        .collect::<Vec<(f64, f64, f64)>>();

    embed_datetime(&zipped, data)
}

fn get_q(d0: f64, d1: f64) -> f64 {
    if d1 > 0.0 && d0 <= 0.0 {
        75.0 // long
    } else if d1 < 0.0 && d0 >= 0.0 {
        25.0 // short
    } else {
        50.0 // neutral
    }
}

fn strength_term<
    'a,
    I1: IntoIterator<Item = &'a Option<f64>>,
    I2: IntoIterator<Item = &'a Option<f64>>,
>(
    a: I1,
    b: I2,
    mult: f64,
) -> Vec<Option<f64>> {
    let mut max_d = f64::NAN;
    a.into_iter()
        .zip(b.into_iter())
        .map(|(x0, x1)| {
            if let (Some(x0), Some(x1)) = (x0, x1) {
                let d = x1 - x0;
                max_d = max_d.max(d);
                return Some(mult * d / max_d);
            }
            None
        })
        .collect()
}

/// From Naranjo paper
pub fn my_macd(data: &[Ohlc]) -> Vec<DTValue<f64>> {
    let dt = to_option_vec(&close_p(data));

    let short_sma = sma(&dt, 12);
    let long_sma = sma(&dt, 26);
    let macd_line = vec_diff(&short_sma, &long_sma);
    let signal_line = ema(&macd_line, 9);
    let divergence = vec_diff(&macd_line, &signal_line);

    let open = to_option_vec(&data.iter().map(|x| x.open).collect::<Vec<f64>>());
    let o = strength_term(&open, open.iter().skip(1), 3.0);
    let div = strength_term(&divergence, divergence.iter().skip(1), 3.0);
    let sma_d = strength_term(&short_sma, &long_sma, 10.0);

    let res = izip!(divergence.windows(2), o, div, sma_d).map(|(dv, x, y, z)| {
        if let (Some(d0), Some(d1), Some(x), Some(y), Some(z)) = (dv[0], dv[1], x, y, z) {
            let q = get_q(d0, d1);
            return q - x - y - z;
        }
        f64::NAN
    })
    .collect::<Vec<f64>>();

    embed_datetime(&res, data)
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
