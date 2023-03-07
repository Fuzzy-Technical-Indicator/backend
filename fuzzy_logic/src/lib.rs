use std::rc::Rc;

use linguistic::LinguisticVar;
use set::FuzzySet;

pub mod pure;
pub mod set;
pub mod shape;
pub mod linguistic;

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

pub struct FuzzyEngine<const N: usize, const M: usize>
{
    inputs_var: [LinguisticVar; N],
    outputs_var: [LinguisticVar; M],
    rules: Vec<(Vec<String>, Vec<String>)>, // list of ([input1_term, input2_term, ...] -> output_term)
    // maybe rules should be a list of pair (linguistic_var, term) => (linguistic_var, term)
}

impl<const N: usize, const M: usize> FuzzyEngine<N, M> {
    pub fn new(inputs_var: [LinguisticVar; N], output_var: [LinguisticVar; M]) -> Self {
        FuzzyEngine {
            inputs_var,
            outputs_var: output_var,
            rules: Vec::<(Vec<String>, Vec<String>)>::new(),
        }
    }

    /// has side effect, mutates self
    /* 
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
    */

    pub fn calculate(&self, inputs: [f64; N]) -> Vec<Option<FuzzySet>> {
        self
            .rules
            .iter()
            .map(|(cond, res)| {
                let aj = cond
                    .iter()
                    .enumerate()
                    .map(|(i, x)| self.inputs_var[i].term(x).unwrap().degree_of(inputs[i]))
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();

                let out = res.iter()
                    .enumerate()
                    .map(|(i, x)| self.outputs_var[i].term(x).unwrap().min(aj))
                    .collect::<Vec<FuzzySet>>();
                
                out.iter().fold(None, |acc, x| 
                    match acc {
                        None => Some(x.clone()),
                        Some(y) => y.std_union(x)
                    }
                )
            })
            .collect::<Vec<Option<FuzzySet>>>()
    }
}


#[cfg(test)]
mod tests {
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
    */
}
