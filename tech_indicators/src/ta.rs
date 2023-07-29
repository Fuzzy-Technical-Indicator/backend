use crate::{none_iter, none_par_iter};
use rayon::prelude::*;

/// Compares the current `source` value to it's value `length` bars ago and return the difference
pub fn change(src: &[Option<f64>], length: usize) -> Vec<Option<f64>> {
    none_par_iter(length)
        .chain(
            src.par_iter()
                .zip(src.par_iter().skip(length))
                .map(|(x0, x1)| match (x0, x1) {
                    (Some(v0), Some(v1)) => Some(v1 - v0),
                    _ => None,
                }),
        )
        .collect()
}

/// Cumulative (total) sum of `source`. In other words it's a sum of all elements of `source`.
pub fn cum(src: &[Option<f64>]) -> Vec<Option<f64>> {
    let mut s = 0.0;
    src.iter()
        .map(|opt| {
            opt.map(|v| {
                s += v;
                s
            })
        })
        .collect()
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

/// Exponential Weighted Moving Average
///
/// [reference](https://corporatefinanceinstitute.com/resources/capital-markets/exponentially-weighted-moving-average-ewma/#:~:text=What%20is%20the%20Exponentially%20Weighted,technical%20analysis%20and%20volatility%20modeling).
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

/// The sma function returns the moving average, that is the sum of last y values of x, divided by y.
pub fn sma(src: &[Option<f64>], length: usize) -> Vec<Option<f64>> {
    windows_compute(src, length, |xs| {
        Some(xs.iter().filter_map(|v| *v).sum::<f64>() / length as f64)
    })
}

/// Moving average used in RSI. It is the exponentially weighted moving average with alpha = 1 / length.
pub fn rma(src: &[Option<f64>], length: usize) -> Vec<Option<f64>> {
    let alpha = 1f64 / length as f64;
    let sma = src.iter().filter_map(|v| *v).take(length).sum::<f64>() / length as f64;

    ewma(src, alpha, sma, length)
}

/// The ema function returns the exponentially weighted moving average. In ema weighting factors decrease exponentially. It calculates by using a formula: EMA = alpha * source + (1 - alpha) * EMA[1], where alpha = 2 / (length + 1).
pub fn ema(src: &[Option<f64>], length: usize) -> Vec<Option<f64>> {
    let alpha = 2f64 / (length as f64 + 1f64);
    let sma = src.iter().filter_map(|v| *v).take(length).sum::<f64>() / length as f64;

    ewma(src, alpha, sma, length)
}

pub fn stdev(src: &[Option<f64>], length: usize) -> Vec<Option<f64>> {
    windows_compute(src, length, |xs| {
        let mean = xs.iter().filter_map(|v| *v).sum::<f64>() / length as f64;
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
                / length as f64)
                .sqrt(),
        )
    })
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
