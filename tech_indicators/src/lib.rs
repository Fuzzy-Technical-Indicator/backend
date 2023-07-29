mod adx_utills;
pub mod fuzzy;
pub mod math;
pub mod ta;
mod utils;

use adx_utills::calc_adx;
use itertools::izip;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    time: i64,
    value: T,
}

fn volume(data: &[Ohlc]) -> Vec<Option<u64>> {
    data.par_iter().map(|x| Some(x.volume)).collect()
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
fn embed_datetime<T>(data: &[T], ohlc: &[Ohlc]) -> Vec<DTValue<T>>
where
    T: Send + Sync + Copy,
{
    ohlc.par_iter()
        .zip(data.par_iter())
        .map(|(x, v)| DTValue {
            time: x.time.timestamp_millis(),
            value: *v,
        })
        .collect()
}

/// Relative Strength Index (TradingView version)
///
/// [reference](https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}rsi)
pub fn rsi(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    let (gain, loss) = utils::compute_gainloss(data);
    let rs_vec = utils::rma_rs(&gain, &loss, n);
    let rsi = rs_vec
        .par_iter()
        .map(|rs_o| {
            if let Some(rs) = rs_o {
                100.0 - 100.0 / (1.0 + rs)
            } else {
                100.0 - 100.0 / (1.0 + f64::NAN)
            }
        })
        .collect::<Vec<f64>>();

    embed_datetime(&rsi, data)
}

/// return (sma, lower, upper)
fn bb_utill(src: &[f64], n: usize, mult: f64) -> Vec<(f64, f64, f64)> {
    let dt = to_option_vec(src);
    let basis = ta::sma(&dt, n);
    let dev = ta::stdev(&dt, n);
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
    let macd_line = calc_macd_line(&dt, 12, 26, ta::ema);
    let signal_line = ta::ema(&macd_line, 9);
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

    let short_sma = ta::sma(&dt, 12);
    let long_sma = ta::sma(&dt, 26);
    let macd_line = vec_diff(&short_sma, &long_sma);
    let signal_line = ta::ema(&macd_line, 9);
    let divergence = vec_diff(&macd_line, &signal_line);

    let open = to_option_vec(&data.iter().map(|x| x.open).collect::<Vec<f64>>());
    let o = strength_term(&open, open.iter().skip(1), 3.0);
    let div = strength_term(&divergence, divergence.iter().skip(1), 3.0);
    let sma_d = strength_term(&short_sma, &long_sma, 10.0);

    let res = izip!(divergence.windows(2), o, div, sma_d)
        .map(|(dv, x, y, z)| {
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

/// On Balance Volume
pub fn obv(data: &[Ohlc]) -> Vec<DTValue<f64>> {
    let close = to_option_vec(&close_p(data));
    let signs = &math::sign(&ta::change(&close, 1));
    let values = ta::cum(&math::mult_u64(&signs, &volume(data)));
    let result = values
        .par_iter()
        .map(|v| match v {
            Some(v) => *v,
            None => f64::NAN,
        })
        .collect::<Vec<f64>>();

    embed_datetime(&result, data)
}
