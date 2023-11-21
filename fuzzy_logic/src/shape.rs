use std::{collections::HashMap, sync::Arc};

use crate::F;

#[derive(Clone)]
pub struct Shape {
    pub function: F,
    pub name: Option<String>,
    pub parameters: Option<HashMap<String, f64>>,
}

pub fn trapezoidal(a: f64, b: f64, c: f64, d: f64, e: f64) -> Shape {
    Shape {
        function: Arc::new(move |x| {
            if x >= a && x < b {
                return ((x - a) * e) / (b - a);
            } else if x >= b && x <= c {
                return e;
            } else if x > c && x <= d {
                return e * (1.0 - (x - c).abs() / (d - c));
            }
            0.0
        }),
        name: Some("trapezoidal".into()),
        // need to rename this
        parameters: Some(HashMap::from([
            ("a".into(), a),
            ("b".into(), b),
            ("c".into(), c),
            ("d".into(), d),
            ("e".into(), e),
        ])),
    }
}

pub fn triangle(a: f64, b: f64, s: f64) -> Shape {
    Shape {
        function: Arc::new(move |x| {
            if (a - s) <= x && x <= (a + s) {
                return b * (1.0 - (x - a).abs() / s);
            }
            0.0
        }),
        name: Some("triangle".into()),
        parameters: Some(HashMap::from([
            ("center".into(), a),
            ("height".into(), b),
            ("width".into(), s),
        ])),
    }
}

pub fn zero() -> Shape {
    Shape {
        function: Arc::new(|_| 0.0),
        name: Some("zero".into()),
        parameters: None,
    }
}
