use crate::none_par_iter;
use rayon::prelude::*;

/// Compares the current `source` value to it's value `length` bars ago and return the difference
pub fn change(source: &[Option<f64>], length: usize) -> Vec<Option<f64>> {
    none_par_iter(1)
        .chain(
            source
                .par_iter()
                .zip(source.par_iter().skip(length))
                .map(|(x0, x1)| match (x0, x1) {
                    (Some(v0), Some(v1)) => Some(v1 - v0),
                    _ => None,
                }),
        )
        .collect()
}

/// Cumulative (total) sum of `source`. In other words it's a sum of all elements of `source`.
pub fn cum(source: &[Option<f64>]) -> Vec<Option<f64>> {
    let mut s = 0.0;
    source
        .iter()
        .map(|opt| {
            opt.map(|v| {
                s += v;
                s
            })
        })
        .collect()
}
