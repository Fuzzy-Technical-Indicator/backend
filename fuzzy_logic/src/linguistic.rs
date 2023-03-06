use std::collections::HashMap;

use crate::set::FuzzySet;

pub struct LinguisticVar<F: Fn(f64) -> f64 + Copy> {
    pub sets: HashMap<String, FuzzySet<F>>,
    pub universe: Vec<f64>,
}

impl<F: Fn(f64) -> f64 + Copy> LinguisticVar<F> {
    pub fn new(inputs: Vec<(&str, F)>, universe: Vec<f64>) -> Self {
        let sets: HashMap<String, FuzzySet<F>> = HashMap::from_iter(
            inputs
                .iter()
                .map(|(name, f)| (name.to_string(), FuzzySet::new(&universe, *f))),
        );
        LinguisticVar { sets, universe }
    }

    pub fn term(&self, name: &str) -> Option<&FuzzySet<F>> {
        self.sets.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{arange, shape::triangle};

    #[test]
    fn linguistic() {
        let var1 = LinguisticVar::new(
            vec![
                ("normal", triangle(5f64, 0.8, 3f64)),
                ("weak", triangle(3f64, 0.8, 1.5f64)),
            ],
            arange(0f64, 10f64, 0.01),
        );

        assert_eq!(var1.term("normal").unwrap().degree_of(5.0), 0.8);
        assert_eq!(var1.term("weak").unwrap().degree_of(3.0), 0.8);
        match var1.term("strongl") {
            Some(_) => assert!(false),
            None => assert!(true),
        }
    }
}
