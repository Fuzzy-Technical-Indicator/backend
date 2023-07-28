use rayon::prelude::*;

/// Sign (signum) of `number` is zero if `number` is zero, 1.0 if `number` is greater than zero, -1.0 if `number` is less than zero.
pub fn sign(number: &[Option<f64>]) -> Vec<Option<f64>> {
    number
        .par_iter()
        .map(|opt| {
            opt.map(|v| {
                if v == 0.0 {
                    return 0.0;
                } else if v > 0.0 {
                    return 1.0;
                }
                -1.0
            })
        })
        .collect()
}

pub fn mult_u64(multiplicand: &[Option<f64>], multiplier: &[Option<u64>]) -> Vec<Option<f64>> {
    multiplicand
        .par_iter()
        .zip(multiplier.par_iter())
        .map(|(opt_v, opt_m)| match (opt_v, opt_m) {
            (Some(v), Some(m)) => Some(v * (*m as f64)),
            _ => None,
        })
        .collect()
}
