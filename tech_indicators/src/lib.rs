pub mod fuzzy;
pub mod math;
pub mod ta;
mod utils;

use itertools::{izip, Itertools};
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

pub trait OhlcSliceOps {
    fn volumes(&self) -> Vec<Option<u64>>;
    fn opens(&self) -> Vec<Option<f64>>;
    fn closes(&self) -> Vec<Option<f64>>;
    fn highs(&self) -> Vec<Option<f64>>;
    fn lows(&self) -> Vec<Option<f64>>;
    fn ohlcv(
        &self,
    ) -> Vec<(
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<u64>,
    )>;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Hash)]
pub struct DTValue<T> {
    pub time: i64,
    pub value: T,
}

impl OhlcSliceOps for [Ohlc] {
    fn volumes(&self) -> Vec<Option<u64>> {
        self.par_iter().map(|x| Some(x.volume)).collect()
    }

    fn opens(&self) -> Vec<Option<f64>> {
        self.par_iter().map(|x| Some(x.open)).collect()
    }

    fn closes(&self) -> Vec<Option<f64>> {
        self.par_iter().map(|x| Some(x.close)).collect()
    }

    fn highs(&self) -> Vec<Option<f64>> {
        self.par_iter().map(|x| Some(x.high)).collect()
    }

    fn lows(&self) -> Vec<Option<f64>> {
        self.par_iter().map(|x| Some(x.low)).collect()
    }

    /// return (open, high, low, close, volume) list
    fn ohlcv(
        &self,
    ) -> Vec<(
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<u64>,
    )> {
        self.par_iter()
            .map(|x| {
                (
                    Some(x.open),
                    Some(x.high),
                    Some(x.low),
                    Some(x.close),
                    Some(x.volume),
                )
            })
            .collect()
    }
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

/// Bollinger Bands
///
/// [reference](https://www.tradingview.com/pine-script-reference/v5/#fun_ta{dot}bb)
pub fn bb(data: &[Ohlc], n: usize, mult: f64) -> Vec<DTValue<(f64, f64, f64)>> {
    let src = data.closes();

    let basis = ta::sma(&src, n);
    let dev = ta::stdev(&src, n);
    let bb_res = basis
        .par_iter()
        .zip(dev.par_iter())
        .map(|(sma_opt, d_opt)| match (sma_opt, d_opt) {
            (Some(sma), Some(d)) => (*sma, sma - mult * d, sma + mult * d),
            _ => (f64::NAN, f64::NAN, f64::NAN),
        })
        .collect::<Vec<(f64, f64, f64)>>();

    embed_datetime(&bb_res, data)
}

/// Moving Average Convergence/Divergence
///
/// [reference](https://www.tradingview.com/support/solutions/43000502344-macd-moving-average-convergence-divergence/)
pub fn macd(
    data: &[Ohlc],
    fastlen: usize,
    slowlen: usize,
    siglen: usize,
) -> Vec<DTValue<(f64, f64, f64)>> {
    if fastlen > slowlen {
        panic!("fastlen should be less than slowlen");
    }

    let src = data.closes();
    let (macd_line, signal_line, hist) = ta::macd(&src, fastlen, slowlen, siglen);
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

/// d0 is previous divergence, d1 is the current divergence
/// return
/// - 25 for short
/// - 75 for long
/// - 50 for neutral
fn get_q(d0: f64, d1: f64) -> f64 {
    if d1 > 0.0 && d0 <= 0.0 {
        75.0 // long
    } else if d1 < 0.0 && d0 >= 0.0 {
        25.0 // short
    } else {
        50.0 // neutral
    }
}

fn strength_term(l1: &[Option<f64>], l2: &[Option<f64>], mult: f64) -> Vec<Option<f64>> {
    let mut max_d = f64::MIN;
    l1.into_iter()
        .zip(l2.into_iter())
        .map(|(x0, x1)| match (x0, x1) {
            (Some(v0), Some(v1)) => {
                let d = v1 - v0;
                max_d = max_d.max(d);
                return Some(mult * d / max_d);
            }
            _ => None,
        })
        .collect()
}

/// From Naranjo paper
pub fn naranjo_macd(data: &[Ohlc]) -> Vec<DTValue<f64>> {
    let src = data.closes();

    // copied from ta::macd
    let short_sma = ta::sma(&src, 12);
    let long_sma = ta::sma(&src, 26);
    let macd_line = utils::vec_diff(&short_sma, &long_sma);
    let signal_line = ta::ema(&macd_line, 9);
    let divergence = utils::vec_diff(&macd_line, &signal_line);

    let opens = data.opens();
    let skipped_opens = opens.clone().into_iter().skip(1).collect_vec();
    let skipped_divergence = divergence.clone().into_iter().skip(1).collect_vec();

    let open_term = strength_term(&opens, &skipped_opens, 3.0);
    let div = strength_term(&divergence, &skipped_divergence, 3.0);
    let sma_d = strength_term(&short_sma, &long_sma, 10.0);

    let res = izip!(divergence.windows(2), open_term, div, sma_d)
        .map(|(dv, x, y, z)| match (dv[0], dv[1], x, y, z) {
            (Some(d0), Some(d1), Some(x), Some(y), Some(z)) => {
                let q = get_q(d0, d1);
                if q == 50.0 {
                    return 50.0;
                }
                q - x - y - z
            }
            _ => f64::NAN,
        })
        .collect::<Vec<f64>>();

    embed_datetime(&res, data)
}

pub fn adx(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    let (dm_p, dm_m) = ta::dm(data);
    let tr = ta::rma(&ta::tr(data), n);

    let plus = ta::di(&ta::rma(&dm_p, n), &tr);
    let minus = ta::di(&ta::rma(&dm_m, n), &tr);

    let sum = plus
        .par_iter()
        .zip(minus.par_iter())
        .map(|(p_opt, m_opt)| match (p_opt, m_opt) {
            (Some(p), Some(m)) => Some(p + m),
            _ => None,
        });

    let adx = plus
        .par_iter()
        .zip(minus.par_iter())
        .zip(sum)
        .map(|((p, m), s)| match (p, m, s) {
            (Some(p), Some(m), Some(s)) => Some((p - m).abs() / s),
            _ => None,
        })
        .collect::<Vec<Option<f64>>>();

    let smooth_adx = ta::rma(&adx, n)
        .par_iter()
        .map(|x| match x {
            Some(v) => 100.0 * v,
            _ => f64::NAN,
        })
        .collect::<Vec<f64>>();

    embed_datetime(&smooth_adx, data)
}

/// On Balance Volume
pub fn obv(data: &[Ohlc]) -> Vec<DTValue<f64>> {
    let close = data.closes();
    let signs = &math::sign(&ta::change(&close, 1));
    let values = ta::cum(&math::mult_u64(&signs, &data.volumes()));
    let result = values
        .par_iter()
        .map(|v| match v {
            Some(v) => *v,
            None => f64::NAN,
        })
        .collect::<Vec<f64>>();

    embed_datetime(&result, data)
}

pub fn aroon(data: &[Ohlc], length: usize) -> Vec<DTValue<(f64, f64)>> {
    let len = length as f64;

    let upper = ta::highestbars(&data.highs(), length)
        .iter()
        .map(|opt| match opt {
            Some(x) => 100f64 * ((x + len) / len),
            _ => f64::NAN,
        })
        .collect::<Vec<f64>>();

    let lower = ta::lowestbars(&data.lows(), length)
        .iter()
        .map(|opt| match opt {
            Some(x) => 100f64 * ((x + len) / len),
            _ => f64::NAN,
        })
        .collect::<Vec<f64>>();

    let zipped = upper
        .into_par_iter()
        .zip(lower.into_par_iter())
        .collect::<Vec<(f64, f64)>>();

    embed_datetime(&zipped, data)
}

pub fn accum_dist(data: &[Ohlc]) -> Vec<DTValue<f64>> {
    let mfm = data
        .ohlcv()
        .par_iter()
        .map(
            |(_, h_opt, l_opt, c_opt, v_opt)| match (c_opt, l_opt, h_opt, v_opt) {
                (Some(close), Some(low), Some(high), Some(volume)) => {
                    if high == low {
                        return Some(0.0);
                    }
                    Some(((2.0 * close - low - high) / (high - low)) * (*volume as f64))
                }
                _ => None,
            },
        )
        .collect::<Vec<Option<f64>>>();

    let ac = ta::cum(&mfm)
        .par_iter()
        .map(|x| match x {
            Some(v) => *v,
            _ => f64::NAN,
        })
        .collect::<Vec<f64>>();

    embed_datetime(&ac, data)
}

pub fn stoch(
    data: &[Ohlc],
    period_k: usize,
    period_d: usize,
    smooth_k: usize,
) -> Vec<DTValue<(f64, f64)>> {
    let k = ta::sma(
        &ta::stoch(&data.closes(), &data.highs(), &data.lows(), period_k),
        smooth_k,
    );
    let d = ta::sma(&k, period_d);

    let result = k
        .par_iter()
        .zip(d.par_iter())
        .map(|(x0, x1)| match (x0, x1) {
            (Some(v0), Some(v1)) => (*v0, *v1),
            (Some(v0), _) => (*v0, f64::NAN),
            (_, Some(v1)) => (f64::NAN, *v1),
            _ => (f64::NAN, f64::NAN),
        })
        .collect::<Vec<(f64, f64)>>();

    embed_datetime(&result, data)
}
