use rayon::prelude::*;

use crate::{ta, Ohlc};

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
