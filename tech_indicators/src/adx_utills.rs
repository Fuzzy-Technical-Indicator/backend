use crate::{embed_datetime, none_iter, rma, DTValue, Ohlc, none_par_iter};
use rayon::prelude::*;

/// True Range
fn tr(data: &[Ohlc]) -> Vec<Option<f64>> {
    none_iter(1)
        .chain(data.iter().zip(data.iter().skip(1)).map(|(t0, t1)| {
            Some(
                (t1.high - t1.low)
                    .max((t1.high - t0.close).abs())
                    .max((t0.close - t1.low).abs()),
            )
        }))
        .collect()
}

/// f is a function in that takes (t0, t1) and do something
fn change(data: &[Ohlc], f: fn((&Ohlc, &Ohlc)) -> Option<f64>) -> Vec<Option<f64>> {
    none_par_iter(1)
        .chain(data.par_iter().zip(data.par_iter().skip(1)).map(f))
        .collect()
}

/// Directional Movement
fn calc_dm(data: &[Ohlc]) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
    let up = change(data, |(t0, t1)| Some(t1.high - t0.high));
    let down = change(data, |(t0, t1)| Some(t0.low - t1.low));

    up.par_iter()
        .zip(down.par_iter())
        .map(|(u, d)| {
            if let (Some(u), Some(d)) = (u, d) {
                return (
                    Some(if u > d && *u > 0.0 { *u } else { 0f64 }),
                    Some(if u < d && *d > 0.0 { *d } else { 0f64 }),
                );
            }
            (None, None)
        })
        .unzip()
}

/// Directional Index
fn calc_di(dm: &[Option<f64>], tr: &[Option<f64>]) -> Vec<Option<f64>> {
    dm.par_iter()
        .zip(tr.par_iter())
        .map(|(dm_p, tr)| {
            if let (Some(dm_p), Some(tr)) = (dm_p, tr) {
                Some(100.0 * dm_p / tr)
            } else {
                None
            }
        })
        .collect()
}

pub fn calc_adx(data: &[Ohlc], n: usize) -> Vec<DTValue<f64>> {
    let (dm_p, dm_m) = calc_dm(data);
    let tr = rma(&tr(data), n);

    let plus = calc_di(&rma(&dm_p, n), &tr);
    let minus = calc_di(&rma(&dm_m, n), &tr);

    let sum = plus.par_iter().zip(minus.par_iter()).map(|(p, m)| {
        if let (Some(p), Some(m)) = (p, m) {
            Some(p + m)
        } else {
            None
        }
    });

    let adx = plus
        .par_iter()
        .zip(minus.par_iter())
        .zip(sum)
        .map(|((p, m), s)| {
            if let (Some(p), Some(m), Some(s)) = (p, m, s) {
                Some((p - m).abs() / s)
            } else {
                None
            }
        })
        .collect::<Vec<Option<f64>>>();

    let smooth_adx = rma(&adx, n)
        .par_iter()
        .map(|x| if let Some(v) = x { 100.0 * v } else { f64::NAN })
        .collect::<Vec<f64>>();

    embed_datetime(&smooth_adx, data)
}

#[cfg(test)]
mod test {
    use super::*;

    fn ohlc_with(high: f64, low: f64, close: f64) -> Ohlc {
        Ohlc {
            ticker: "".to_string(),
            time: bson::DateTime::now(),
            open: 0.0,
            high,
            low,
            close,
            volume: 0,
        }
    }

    fn test_set() -> Vec<Ohlc> {
        vec![
            ohlc_with(2.0, 1.0, 2.0),
            ohlc_with(3.0, 2.0, 2.5),
            ohlc_with(4.0, 3.0, 3.0),
            ohlc_with(5.0, 2.0, 3.0),
        ]
    }

    #[test]
    fn test_change() {
        let data = test_set();
        let up = change(&data, |(t0, t1)| Some(t1.high - t0.high));
        let down = change(&data, |(t0, t1)| Some(t0.low - t1.low));

        for (v, expected) in up
            .iter()
            .zip(vec![None, Some(1.0), Some(1.0), Some(1.0)].iter())
        {
            assert_eq!(v, expected);
        }
        for (v, expected) in down
            .iter()
            .zip(vec![None, Some(-1.0), Some(-1.0), Some(1.0)].iter())
        {
            assert_eq!(v, expected);
        }
    }

    #[test]
    fn test_dm() {
        let data = test_set();
        let (dm_p, dm_m) = calc_dm(&data);

        for (v, expected) in dm_p
            .iter()
            .zip(vec![None, Some(1.0), Some(1.0), Some(0.0)].iter())
        {
            assert_eq!(v, expected);
        }
        for (v, expected) in dm_m
            .iter()
            .zip(vec![None, Some(0.0), Some(0.0), Some(0.0)].iter())
        {
            assert_eq!(v, expected);
        }
    }

    #[test]
    fn test_tr() {
        let data = test_set();
        let tr = tr(&data);

        for (v, expected) in tr
            .iter()
            .zip(vec![None, Some(1.0), Some(1.5), Some(3.0)].iter())
        {
            assert_eq!(v, expected);
        }
    }
}
