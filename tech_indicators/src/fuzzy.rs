use fuzzy_logic::{
    linguistic::LinguisticVar,
    shape::{trapezoidal, triangle},
    FuzzyEngine,
};
use rayon::prelude::*;

use crate::DTValue;

fn bb_percent(price: f64, v: (f64, f64, f64)) -> f64 {
    let (sma, lower, upper) = v;

    if price > sma {
        return (price - sma) / (upper - sma);
    }

    (sma - price) / (sma - lower)
}

pub fn fuzzy_indicator(
    rsi: Vec<DTValue<f64>>,
    bb: Vec<DTValue<(f64, f64, f64)>>,
    price: Vec<f64>,
) -> Vec<DTValue<Vec<f64>>> {
    // TODO: Make this faster
    let f_engine = FuzzyEngine::new()
        .add_cond(LinguisticVar::new(
            vec![
                ("low", triangle(0.0, 1.0, 30.0)),
                ("medium", triangle(50.0, 1.0, 30.0)),
                ("high", triangle(100.0, 1.0, 30.0)),
            ],
            (0.0, 100.0),
        ))
        .add_cond(LinguisticVar::new(
            vec![
                ("long", triangle(-120.0, 1.0, 30.0)),
                ("wait", trapezoidal(-100.0, -50.0, 50.0, 100.0, 1.0)),
                ("short", triangle(120.0, 1.0, 30.0)),
            ],
            (-150.0, 150.0),
        ))
        .add_output(LinguisticVar::new(
            // long
            vec![
                ("weak", triangle(0.0, 1.0, 15.0)),
                ("strong", triangle(30.0, 1.0, 30.0)),
                ("verystrong", triangle(100.0, 1.0, 60.0)),
            ],
            (0.0, 100.0),
        ))
        .add_output(LinguisticVar::new(
            // short
            vec![
                ("weak", triangle(0.0, 1.0, 15.0)),
                ("strong", triangle(30.0, 1.0, 30.0)),
                ("verystrong", triangle(100.0, 1.0, 60.0)),
            ],
            (0.0, 100.0),
        ))
        .add_rule(
            vec![Some("high"), Some("long")],
            vec![Some("weak"), Some("weak")],
        )
        .add_rule(
            vec![Some("high"), Some("wait")],
            vec![Some("weak"), Some("strong")],
        )
        .add_rule(
            vec![Some("high"), Some("short")],
            vec![Some("weak"), Some("verystrong")],
        )
        .add_rule(
            vec![Some("medium"), Some("long")],
            vec![Some("weak"), Some("strong")],
        )
        .add_rule(
            vec![Some("medium"), Some("wait")],
            vec![Some("weak"), Some("weak")],
        )
        .add_rule(
            vec![Some("medium"), Some("short")],
            vec![Some("strong"), Some("weak")],
        )
        .add_rule(
            vec![Some("low"), Some("long")],
            vec![Some("verystrong"), Some("weak")],
        )
        .add_rule(
            vec![Some("low"), Some("wait")],
            vec![Some("strong"), Some("weak")],
        )
        .add_rule(
            vec![Some("low"), Some("short")],
            vec![Some("weak"), Some("weak")],
        );

    rsi.par_iter()
        .zip(bb.par_iter())
        .zip(price.par_iter())
        .map(|((rsi_v, bb_v), p)| {
            let res = f_engine
                .inference(vec![Some(rsi_v.value), Some(bb_percent(*p, bb_v.value))])
                .unwrap();
            DTValue {
                time: rsi_v.time,
                value: res
                    .iter()
                    .map(|x| x.centroid_defuzz(0.1)) // the resolution of the centroid make so much impact on the performance
                    .collect::<Vec<f64>>(),
            }
        })
        .collect::<Vec<DTValue<Vec<f64>>>>()
}
