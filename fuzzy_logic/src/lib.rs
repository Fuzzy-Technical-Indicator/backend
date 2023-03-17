use std::{collections::HashMap, rc::Rc};

use linguistic::LinguisticVar;
use set::FuzzySet;

pub mod pure;

pub mod linguistic;
pub mod plot;
pub mod set;
pub mod shape;

type F = Rc<dyn Fn(f64) -> f64>;

pub fn arange(start: f64, stop: f64, interval: f64) -> Vec<f64> {
    if stop < start {
        panic!("end can not be less than start");
    } else if interval <= 0f64 {
        panic!("interval must be > 0");
    }
    let r = 1.0 / interval;

    (0..)
        .map(|i| start + i as f64 * interval)
        .map(|x| (x * r).round() / r)
        .take_while(|&x| x <= stop)
        .collect::<Vec<f64>>()
}

// Vec<Rule>
// Hashmap<String, LinguisticVar>

type Rule = (Vec<(String, String)>, Vec<(String, String)>);

pub struct FuzzyEngine {
    inputs_var: HashMap<String, LinguisticVar>,
    outputs_var: HashMap<String, LinguisticVar>,
    rules: Vec<Rule>,
}

impl FuzzyEngine {
    pub fn new() -> Self {
        FuzzyEngine {
            inputs_var: HashMap::new(),
            outputs_var: HashMap::new(),
            rules: Vec::new(),
        }
    }

    pub fn add_cond(self, name: &str, var: LinguisticVar) -> Self {
        let mut inputs_var = self.inputs_var;
        inputs_var.insert(name.to_string(), var);
        Self {
            inputs_var,
            outputs_var: self.outputs_var,
            rules: self.rules,
        }
    }

    pub fn add_output(self, name: &str, var: LinguisticVar) -> Self {
        let mut outputs_var = self.outputs_var;
        outputs_var.insert(name.to_string(), var);
        Self {
            inputs_var: self.inputs_var,
            outputs_var,
            rules: self.rules,
        }
    }

    pub fn add_rule(self, cond: Vec<(&str, &str)>, output: Vec<(&str, &str)>) -> Self {
        // This side effect is negligible ?
        let mut rules = self.rules;
        rules.push((
            cond.iter()
                .map(|(var, term)| (var.to_string(), term.to_string()))
                .collect::<Vec<(String, String)>>(),
            output
                .iter()
                .map(|(var, term)| (var.to_string(), term.to_string()))
                .collect::<Vec<(String, String)>>(),
        ));
        Self {
            inputs_var: self.inputs_var,
            outputs_var: self.outputs_var,
            rules,
        }
    }

    pub fn inference(&self, inputs: Vec<(&str, f64)>) -> Vec<Option<FuzzySet>> {
        let inputs_map: HashMap<_, _> =
            HashMap::from_iter(inputs.iter().map(|(k, v)| (k.to_string(), *v)));

        self.rules
            .iter()
            .map(|(cond, res)| {
                let aj = cond
                    .iter()
                    .map(|(var, term)| {
                        self.inputs_var
                            .get(var)
                            .unwrap()
                            .term(term)
                            .unwrap()
                            .degree_of(*inputs_map.get(var).unwrap())
                    })
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();

                let out = res
                    .iter()
                    .map(|(var, term)| {
                        self.outputs_var
                            .get(var)
                            .unwrap()
                            .term(term)
                            .unwrap()
                            .min(aj)
                    })
                    .collect::<Vec<FuzzySet>>();

                out.iter().fold(None, |acc, x| match acc {
                    None => Some(x.clone()),
                    Some(y) => y.std_union(x),
                })
            })
            .collect::<Vec<Option<FuzzySet>>>()
    }
}

#[cfg(test)]
mod tests {
    use crate::{plot::plot_linguistic, shape::triangle};

    use super::*;

    #[test]
    fn test_arange() {
        assert_eq!(arange(0f64, 5f64, 1f64), vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]);
        assert_eq!(
            arange(0f64, 0.5f64, 0.1f64),
            vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
        );
    }

    /*
    #[test]
    #[should_panic]
    fn test_adding_rule() {
        let rsi = LinguisticVar::new(
            vec![
                (&triangular(20f64, 1.0, 20f64), "low"),
                (&triangular(80f64, 1.0, 20f64), "high"),
            ],
            arange(0f64, 100f64, 0.01),
        );

        let mut f_engine = FuzzyEngine::new([rsi.clone()], [rsi]);
        f_engine.add_rule(["medium".into()], ["low".into()]);
    }
    */

    #[test]
    fn test_basic() {
        let f_engine = FuzzyEngine::new()
            .add_cond(
                "temp",
                LinguisticVar::new(
                    vec![
                        ("cold", triangle(15f64, 1.0, 10f64)),
                        ("little cold", triangle(28f64, 1.0, 10f64)),
                        ("hot", triangle(40f64, 1.0, 20f64)),
                    ],
                    arange(0f64, 50f64, 0.01),
                ),
            )
            .add_cond(
                "humidity",
                LinguisticVar::new(
                    vec![
                        ("low", triangle(25f64, 1.0, 25f64)),
                        ("normal", triangle(45f64, 1.0, 30f64)),
                        ("high", triangle(85f64, 1.0, 25f64)),
                    ],
                    arange(0f64, 100f64, 0.01),
                ),
            )
            .add_output(
                "signal",
                LinguisticVar::new(
                    vec![
                        ("weak", triangle(0f64, 1.0, 15f64)),
                        ("strong", triangle(30f64, 1.0, 30f64)),
                    ],
                    arange(0f64, 50f64, 0.01),
                ),
            )
            .add_rule(
                vec![("temp", "cold"), ("humidity", "low")],
                vec![("signal", "weak")],
            )
            .add_rule(
                vec![("temp", "little cold"), ("humidity", "low")],
                vec![("signal", "weak")],
            )
            .add_rule(
                vec![("temp", "hot"), ("humidity", "low")],
                vec![("signal", "strong")],
            );

        plot_linguistic(&f_engine.inputs_var.get("temp").unwrap(), "temp", "images").unwrap();
        plot_linguistic(
            &f_engine.inputs_var.get("humidity").unwrap(),
            "humidity",
            "images",
        )
        .unwrap();

        let result = f_engine.inference(vec![("temp", 19f64), ("humidity", 10f64)]);
        match result[0] {
            Some(ref x) => {
                plot::plot_set(x, "test", "images").unwrap();
                println!("{:?}", x.centroid_defuzz())
            }
            _ => println!("None"),
        }
    }
}
