use crate::{embed_datetime, none_iter, DTValue, Ohlc};

fn ewma(src: &[Option<f64>], alpha: f64, first: f64, n: usize) -> Vec<Option<f64>> {
    let mut res = src
        .iter()
        .take_while(|x| x.is_none())
        .cloned()
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

/// need to guarantee that we on;y have None on the first part of src
fn rma(src: &[Option<f64>], n: usize) -> Vec<Option<f64>> {
    let alpha = 1f64 / n as f64;
    let sma = src.iter().filter_map(|v| *v).take(n).sum::<f64>() / n as f64;

    ewma(src, alpha, sma, n)
}

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
    none_iter(1)
        .chain(data.iter().zip(data.iter().skip(1)).map(f))
        .collect()
}

fn calc_dm(data: &[Ohlc]) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
    let up = change(data, |(t0, t1)| Some(t1.high - t0.high));
    let down = change(data, |(t0, t1)| Some(t0.low - t1.low));

    up.iter()
        .zip(down.iter())
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
    dm.iter()
        .zip(tr.iter())
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
    // (dm_plus, dm_minus)
    let (dm_p, dm_m) = calc_dm(data);
    let tr = rma(&tr(data), n);

    let plus = calc_di(&rma(&dm_p, n), &tr);
    let minus = calc_di(&rma(&dm_m, n), &tr);

    let sum = plus.iter().zip(minus.iter()).map(|(p, m)| {
        if let (Some(p), Some(m)) = (p, m) {
            Some(p + m)
        } else {
            None
        }
    });

    let adx = plus
        .iter()
        .zip(minus.iter())
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
        .iter()
        .map(|x| if let Some(v) = x { 100.0 * v } else { f64::NAN })
        .collect::<Vec<f64>>();

    embed_datetime(smooth_adx, data)
}

#[cfg(test)]
mod test {
    use float_cmp::approx_eq;

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
    fn test_adx() {
        let adx = calc_adx(&test_set(), 2);

        println!("{:?}", adx);

        for (v, expected) in adx.iter().zip(vec![f64::NAN, 1.0, 1.0, f64::NAN].iter()) {
            assert_eq!(v.value, *expected);
        }
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
                assert!(approx_eq!(f64, *v, *expected, epsilon = 0.0001));
            } else {
                assert_eq!(v, expected)
            }
        }
    }
}
