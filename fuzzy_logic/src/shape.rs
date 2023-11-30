use std::{collections::HashMap, sync::Arc};

use crate::F;

#[derive(Clone)]
pub struct Shape {
    pub function: F,
    pub name: Option<String>,
    pub parameters: Option<HashMap<String, f64>>,
    pub latex: Option<Vec<String>>,
}

impl Shape {
    pub fn default_with(f: F) -> Self {
        Self {
            function: f,
            name: None,
            parameters: None,
            latex: None,
        }
    }
}

pub fn trapezoid(a: f64, b: f64, c: f64, d: f64, h: f64) -> Shape {
    // https://www.desmos.com/calculator/tbja7obnka
    Shape {
        function: Arc::new(move |x| {
            if x >= a && x < b {
                return ((x - a) * h) / (b - a);
            } else if x >= b && x <= c {
                return h;
            } else if x > c && x <= d {
                return h * (1.0 - (x - c).abs() / (d - c));
            }
            0.0
        }),
        name: Some("trapezoid".into()),
        // need to rename this
        parameters: Some(HashMap::from([
            ("a".into(), a),
            ("b".into(), b),
            ("c".into(), c),
            ("d".into(), d),
            ("height".into(), h),
        ])),
        latex: Some(vec![
            format!(
                r"y = \left\{{ {} \le x \le {} : \frac{{(x - {}){}}}{{{} - {}}}\right\}}",
                a, b, a, h, b, a
            ),
            format!(r"y = \left\{{ {} \le x \le {} : {} \right\}}", b, c, h),
            format!(
                r"y = \left\{{ {} \le x \le {} : \left(1 - \frac{{ \left|x - {}\right| }} {{{} - {}}}\right) \cdot {} \right\}}",
                c, d, c, d, c, h
            ),
        ]),
    }
}

pub fn triangle(center: f64, height: f64, width: f64) -> Shape {
    Shape {
        function: Arc::new(move |x| {
            if (center - width) <= x && x <= (center + width) {
                return height * (1.0 - (x - center).abs() / width);
            }
            0.0
        }),
        name: Some("triangle".into()),
        parameters: Some(HashMap::from([
            ("center".into(), center),
            ("height".into(), height),
            ("width".into(), width),
        ])),
        latex: Some(vec![format!(
            r"y = \left\{{ {} \le x \le {} : {} \cdot \left(1 - \frac{{\left|x - {}\right|}}{{{}}}\right) \right\}}",
            center - width,
            center + width,
            height,
            center,
            width
        )]),
    }
}

pub fn zero() -> Shape {
    Shape {
        function: Arc::new(|_| 0.0),
        name: Some("zero".into()),
        parameters: None,
        latex: Some(vec!["y = 0".to_string()]),
    }
}
