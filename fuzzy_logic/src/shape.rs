use std::sync::Arc;

use crate::F;

pub fn trapezoidal(a: f64, b: f64, c: f64, d: f64, e: f64) -> F {
    Arc::new(move |x| {
        if x >= a && x < b {
            return ((x - a) * e) / (b - a);
        } else if x >= b && x <= c {
            return e;
        } else if x > c && x <= d {
            return e * (1.0 - (x - c).abs() / (d - c));
        }
        0.0
    })
}

pub fn triangle(a: f64, b: f64, s: f64) -> F {
    Arc::new(move |x| {
        if (a - s) <= x && x <= (a + s) {
            return b * (1.0 - (x - a).abs() / s);
        }
        0.0
    })
}

pub fn zero() -> F {
    Arc::new(|_| 0.0)
}
