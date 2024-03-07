use std::sync::Arc;

use linguistic::LinguisticVar;
use set::FuzzySet;
use shape::zero;

pub mod linguistic;
pub mod plot;
pub mod set;
pub mod shape;

type F = Arc<dyn Fn(f64) -> f64 + Send + Sync>;

pub fn arange(start: f64, stop: f64, interval: f64) -> Vec<f64> {
    if stop < start {
        panic!("end can not be less than start");
    } 
    if interval <= 0f64 {
        panic!("interval must be > 0");
    }
    let r = 1.0 / interval;
    (0..)
        .map(|i| start + i as f64 * interval)
        .map(|x| (x * r).round() / r)
        .take_while(|&x| x <= stop)
        .collect::<Vec<f64>>()
}

type Rule = (Vec<Option<String>>, Vec<Option<String>>);

pub struct FuzzyEngine {
    pub inputs_var: Vec<LinguisticVar>,
    pub outputs_var: Vec<LinguisticVar>,
    pub rules: Vec<Rule>,
}

impl FuzzyEngine {
    pub fn new() -> Self {
        FuzzyEngine {
            inputs_var: Vec::new(),
            outputs_var: Vec::new(),
            rules: Vec::new(),
        }
    }

    pub fn add_cond(self, var: LinguisticVar) -> Self {
        let mut inputs_var = self.inputs_var;
        inputs_var.push(var);
        Self {
            inputs_var,
            outputs_var: self.outputs_var,
            rules: self.rules,
        }
    }

    pub fn add_output(self, var: LinguisticVar) -> Self {
        let mut outputs_var = self.outputs_var;
        outputs_var.push(var);
        Self {
            inputs_var: self.inputs_var,
            outputs_var,
            rules: self.rules,
        }
    }

    /// check inputs linguistic variable with the given terms
    fn check_inputs(&self, terms: &Vec<Option<&str>>) -> bool {
        self.inputs_var.len() == terms.len()
            && self
                .inputs_var
                .iter()
                .zip(terms.iter())
                .all(|(var, term)| match term {
                    None => true,
                    Some(t) => var.term(t).is_some(),
                })
    }

    /// check outputs linguistic variable with the given terms
    fn check_outputs(&self, terms: &Vec<Option<&str>>) -> bool {
        self.outputs_var.len() == terms.len()
            && self
                .outputs_var
                .iter()
                .zip(terms.iter())
                .all(|(var, term)| match term {
                    None => true,
                    Some(t) => var.term(t).is_some(),
                })
    }

    pub fn is_valid(&self) -> bool {
        self.rules
            .iter()
            .any(|(cond, res)| cond.iter().any(|c| c.is_some()) && res.iter().any(|r| r.is_some()))
    }

    pub fn add_rule(self, cond: Vec<Option<&str>>, output: Vec<Option<&str>>) -> Self {
        if !self.check_inputs(&cond) {
            panic!("The terms given in the condition are not in the linguistic variable");
        }
        if !self.check_outputs(&output) {
            panic!("The terms given in the output are not in the linguistic variable");
        }

        let mut rules = self.rules;
        rules.push((
            cond.iter()
                .map(|term| term.map(|x| x.to_string()))
                .collect::<Vec<Option<String>>>(),
            output
                .iter()
                .map(|term| term.map(|x| x.to_string()))
                .collect::<Vec<Option<String>>>(),
        ));
        Self {
            inputs_var: self.inputs_var,
            outputs_var: self.outputs_var,
            rules,
        }
    }

    pub fn remove_cond(self, index: usize) -> Self {
        let mut inputs_var = self.inputs_var;
        inputs_var.remove(index);
        Self {
            inputs_var,
            outputs_var: self.outputs_var,
            rules: self.rules,
        }
    }

    pub fn remove_output(self, index: usize) -> Self {
        let mut outputs_var = self.outputs_var;
        outputs_var.remove(index);
        Self {
            inputs_var: self.inputs_var,
            outputs_var,
            rules: self.rules,
        }
    }

    pub fn remove_rule(self, index: usize) -> Self {
        let mut rules = self.rules;
        rules.remove(index);
        Self {
            inputs_var: self.inputs_var,
            outputs_var: self.outputs_var,
            rules,
        }
    }

    pub fn inference(&self, inputs: Vec<Option<f64>>) -> Option<Vec<FuzzySet>> {
        self.rules
            .iter()
            .map(|(cond, res)| {
                let aj = compute_aj(&self.inputs_var, cond, &inputs).unwrap();
                min_sets(&self.outputs_var, res, aj)
            })
            .fold(None::<Vec<FuzzySet>>, |acc, x| match acc {
                None => Some(x),
                Some(a) => Some(
                    a.iter()
                        .zip(x.iter())
                        .map(|(a, b)| a.std_union(b).unwrap())
                        .collect(),
                ),
            })
    }
}

fn compute_aj(
    inputs_var: &Vec<LinguisticVar>,
    cond: &Vec<Option<String>>,
    inputs: &Vec<Option<f64>>,
) -> Option<f64> {
    cond.iter()
        .zip(inputs_var.iter())
        .zip(inputs.iter())
        .map(|((term, var), input)| match term {
            None => 1.0, // no term ??
            Some(term) => match input {
                Some(v) => var.degree_of(term, *v).unwrap_or(1.0),
                None => 1.0,
            },
        })
        .min_by(|a, b| a.total_cmp(b))
}

fn min_sets(outputs_var: &Vec<LinguisticVar>, res: &Vec<Option<String>>, aj: f64) -> Vec<FuzzySet> {
    res.iter()
        .zip(outputs_var.iter())
        .map(|(term, var)| match term {
            None => FuzzySet::new(var.universe, zero()),
            Some(term) => var.term(term).unwrap().min(aj),
        })
        .collect()
}

fn union_sets(sets: Vec<Option<FuzzySet>>) -> Option<FuzzySet> {
    sets.iter().fold(None, |acc, x| match x {
        None => acc,
        Some(x) => match acc {
            None => Some(x.clone()),
            Some(y) => y.std_union(x),
        },
    })
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

    #[test]
    #[should_panic]
    fn test_adding_rule() {
        let f_engine = FuzzyEngine::new().add_cond(LinguisticVar::new(
            vec![
                ("cold", triangle(15f64, 1.0, 10f64)),
                ("little cold", triangle(28f64, 1.0, 10f64)),
                ("hot", triangle(40f64, 1.0, 20f64)),
            ],
            (0f64, 1f64),
        ));

        f_engine.add_rule(vec![Some("shit"), None], vec![None]);
    }

    #[test]
    fn test_removing_cond() {
        let f_engine = FuzzyEngine::new()
            .add_cond(LinguisticVar::new(vec![], (0f64, 1f64)))
            .add_cond(LinguisticVar::new(vec![], (0f64, 1f64)));

        assert_eq!(f_engine.remove_cond(0).inputs_var.len(), 1);
    }

    #[test]
    fn test_removing_output() {
        let f_engine = FuzzyEngine::new()
            .add_output(LinguisticVar::new(vec![], (0f64, 1f64)))
            .add_output(LinguisticVar::new(vec![], (0f64, 1f64)));

        assert_eq!(f_engine.remove_output(0).outputs_var.len(), 1);
    }

    #[test]
    fn test_removing_rule() {
        let f_engine = FuzzyEngine::new()
            .add_cond(LinguisticVar::new(vec![], (0f64, 1f64)))
            .add_output(LinguisticVar::new(vec![], (0f64, 1f64)))
            .add_rule(vec![None], vec![None])
            .add_rule(vec![None], vec![None]);

        assert_eq!(f_engine.remove_rule(0).rules.len(), 1);
    }

    #[test]
    fn test_basic() {
        let f_engine = FuzzyEngine::new()
            .add_cond(LinguisticVar::new(
                vec![
                    ("cold", triangle(15f64, 1.0, 10f64)),
                    ("little cold", triangle(28f64, 1.0, 10f64)),
                    ("hot", triangle(40f64, 1.0, 20f64)),
                ],
                (0f64, 50f64),
            ))
            .add_cond(LinguisticVar::new(
                vec![
                    ("low", triangle(25f64, 1.0, 25f64)),
                    ("normal", triangle(45f64, 1.0, 30f64)),
                    ("high", triangle(85f64, 1.0, 25f64)),
                ],
                (0f64, 100f64),
            ))
            .add_output(LinguisticVar::new(
                vec![
                    ("weak", triangle(0f64, 1.0, 15f64)),
                    ("strong", triangle(30f64, 1.0, 30f64)),
                ],
                (0f64, 50f64),
            ))
            .add_rule(vec![Some("cold"), Some("low")], vec![Some("weak")])
            .add_rule(vec![Some("little cold"), Some("low")], vec![Some("weak")])
            .add_rule(vec![Some("hot"), Some("low")], vec![Some("strong")]);

        plot_linguistic(&f_engine.inputs_var[0], "temp", "images/t.svg").unwrap();
        plot_linguistic(&f_engine.inputs_var[1], "humidity", "images/h.svg").unwrap();

        let result = f_engine.inference(vec![Some(19f64), Some(10f64)]).unwrap();

        plot::plot_set(&result[0], "signal", "images/r.svg").unwrap();
        println!("{:?}", result[0].centroid_defuzz(0.01))
    }
}
