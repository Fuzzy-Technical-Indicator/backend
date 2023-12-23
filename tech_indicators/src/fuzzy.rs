use fuzzy_logic::FuzzyEngine;
use rayon::prelude::*;

use crate::DTValue;

pub fn fuzzy_indicator(
    fuzzy_engine: &FuzzyEngine,
    inputs: Vec<(i64, Vec<Option<f64>>)>,
) -> Vec<DTValue<Vec<f64>>> {
    inputs
        .into_par_iter()
        .map(|data| {
            let res = fuzzy_engine.inference(data.1).unwrap();
            DTValue {
                time: data.0,
                value: res
                    .iter()
                    .map(|x| x.centroid_defuzz(0.1))
                    .collect::<Vec<f64>>(),
            }
        })
        .collect()
}
