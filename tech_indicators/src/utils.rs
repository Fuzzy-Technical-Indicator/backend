use rayon::prelude::*;

use crate::{none_par_iter, ta, Ohlc};

pub fn compute_gainloss(data: &[Ohlc]) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
    rayon::iter::once((None, None))
        .chain(
            data.par_iter()
                .zip(data.par_iter().skip(1))
                .map(|(prev, curr)| {
                    (
                        Some((curr.close - prev.close).max(0f64)),
                        Some((prev.close - curr.close).max(0f64)),
                    )
                }),
        )
        .unzip()
}

pub fn rma_rs(gain: &[Option<f64>], loss: &[Option<f64>], n: usize) -> Vec<Option<f64>> {
    ta::rma(gain, n)
        .par_iter()
        .zip(ta::rma(loss, n).par_iter())
        .map(|(g_opt, l_opt)| match (g_opt, l_opt) {
            (Some(g), Some(l)) => Some(g / l),
            _ => None,
        })
        .collect()
}

/// The differnce between list `xs` and list `ys`
pub fn vec_diff(xs: &[Option<f64>], ys: &[Option<f64>]) -> Vec<Option<f64>> {
    xs.par_iter()
        .zip(ys.par_iter())
        .map(|(a_opt, b_opt)| match (a_opt, b_opt) {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        })
        .collect()
}

/// f is a function in that takes (t0, t1) and do something
pub fn process_pairs(data: &[Ohlc], f: fn((&Ohlc, &Ohlc)) -> Option<f64>) -> Vec<Option<f64>> {
    none_par_iter(1)
        .chain(data.par_iter().zip(data.par_iter().skip(1)).map(f))
        .collect()
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
    fn test_process_pairs() {
        let data = test_set();
        let up = process_pairs(&data, |(t0, t1)| Some(t1.high - t0.high));
        let down = process_pairs(&data, |(t0, t1)| Some(t0.low - t1.low));

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
}
