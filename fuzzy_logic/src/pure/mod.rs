use self::linguistic::LinguisticVar;
use self::set::FuzzySet;

pub mod linguistic;
pub mod set;
pub mod shape;

pub struct FuzzyEngine<const N: usize, const M: usize, F>
where
    F: Fn(f64) -> f64 + Copy,
{
    inputs_var: [LinguisticVar<F>; N],
    outputs_var: [LinguisticVar<F>; M],
    rules: Vec<(Vec<String>, Vec<String>)>, // list of ([input1_term, input2_term, ...] -> output_term)
}

impl<const N: usize, const M: usize, F> FuzzyEngine<N, M, F>
where
    F: Fn(f64) -> f64 + Copy,
{
    pub fn new(inputs_var: [LinguisticVar<F>; N], output_var: [LinguisticVar<F>; M]) -> Self {
        FuzzyEngine {
            inputs_var,
            outputs_var: output_var,
            rules: Vec::<(Vec<String>, Vec<String>)>::new(),
        }
    }

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

    pub fn calculate(&self, inputs: [f64; N]) -> Vec<FuzzySet<F>> {
        let outs = self
            .rules
            .iter()
            .map(|(cond, res)| {
                let aj = cond
                    .iter()
                    .enumerate()
                    .map(|(i, x)| self.inputs_var[i].term(x).unwrap().degree_of(inputs[i]))
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();

                res.iter()
                    .enumerate()
                    .map(|(i, x)| self.outputs_var[i].term(x).unwrap().min(aj))
                    .collect::<Vec<FuzzySet<_>>>()
            })
            .collect::<Vec<Vec<FuzzySet<_>>>>();

        /*
        let res = outs[0].iter().fold(None, |acc, x|
            match acc {
                None => Some(*x),
                Some(acc) => acc.std_union(x)
            }
        );
        */

        /*
        let mut res: Vec<FuzzySet<_>> = vec![];
        for i in 0..M {
            res.push(outs[0][i].std_union(&outs[0][i]));
        }
        for j in 1..outs.len() {
            for i in 0..M {
                res[i] = res[i].std_union(&outs[j][i]).unwrap();
            }
        }
        */
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
