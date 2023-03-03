pub mod set;
pub mod shape;

pub mod temp;

use crate::set::*;

pub struct FuzzyEngine<const N: usize, const M: usize> {
    inputs_var: [LinguisticVar; N],
    outputs_var: [LinguisticVar; M],
    rules: Vec<(Vec<String>, Vec<String>)>, // list of ([input1_term, input2_term, ...] -> output_term)
}

impl<const N: usize, const M: usize> FuzzyEngine<N, M> {
    pub fn new(
        inputs_var: [LinguisticVar; N],
        output_var: [LinguisticVar; M],
    ) -> FuzzyEngine<N, M> {
        FuzzyEngine {
            inputs_var,
            outputs_var: output_var,
            rules: Vec::<(Vec<String>, Vec<String>)>::new(),
        }
    }

    pub fn add_rule(&mut self, cond: [&str; N], res: [&str; M]) {
        for i in 0..self.inputs_var.len() {
            self.inputs_var[i].term(&cond[i]); // check if term "cond[i]" exist
        }
        for i in 0..self.outputs_var.len() {
            self.outputs_var[i].term(&res[i]); // term() check if term "res" is exist
        }

        let conditions: Vec<String> = cond.iter().map(|x| x.to_string()).collect();
        let results: Vec<String> = res.iter().map(|x| x.to_string()).collect();
        self.rules.push((conditions, results));
    }

    pub fn calculate(&self, inputs: [f64; N]) -> Vec<FuzzySet> {
        let mut temp: Vec<Vec<FuzzySet>> = vec![];
        for j in 0..self.rules.len() {
            let mut aj = f64::MAX;
            for i in 0..self.rules[j].0.len() {
                let fuzzy_set = self.inputs_var[i].term(&self.rules[j].0[i]);
                let v = fuzzy_set.degree_of(inputs[i]);
                aj = aj.min(v);
            }

            let mut t: Vec<FuzzySet> = vec![];
            for i in 0..self.rules[j].1.len() {
                t.push(
                    self.outputs_var[i]
                        .term(&self.rules[j].1[i])
                        .min(aj, format!("f{}", j)),
                );
            }

            temp.push(t);
        }
        let mut res: Vec<FuzzySet> = vec![];
        for i in 0..M {
            res.push(temp[0][i].std_union(&temp[0][i], "".into()));
        }
        for j in 1..temp.len() {
            for i in 0..M {
                res[i] = res[i].std_union(&temp[j][i], "".into());
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shape::*;

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

    #[test]
    fn test_basic() {
        let temp = LinguisticVar::new(
            vec![
                (&triangular(15f64, 1.0, 10f64), "cold"),
                (&triangular(28f64, 1.0, 10f64), "little cold"),
                (&triangular(40f64, 1.0, 20f64), "hot"),
            ],
            arange(0f64, 50f64, 0.01),
        );
        let humidity = LinguisticVar::new(
            vec![
                (&triangular(25f64, 1.0, 25f64), "low"),
                (&triangular(45f64, 1.0, 30f64), "normal"),
                (&triangular(85f64, 1.0, 25f64), "high"),
            ],
            arange(0f64, 100f64, 0.01),
        );
        let signal = LinguisticVar::new(
            vec![
                (&triangular(0f64, 1.0, 15f64), "weak"),
                (&triangular(30f64, 1.0, 30f64), "strong"),
            ],
            arange(0f64, 50f64, 0.01),
        );

        let mut f_engine = FuzzyEngine::new([temp, humidity], [signal]);

        f_engine.add_rule(["cold", "low"], ["weak"]);
        f_engine.add_rule(["little cold", "low"], ["weak"]);
        f_engine.add_rule(["hot", "low"], ["strong"]);

        let result = f_engine.calculate([25f64, 10f64]);
        println!("{:?}", result[0].centroid_defuzz());
    }
}
