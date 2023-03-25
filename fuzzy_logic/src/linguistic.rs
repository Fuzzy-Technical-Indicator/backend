use std::collections::HashMap;

use crate::{arange, set::FuzzySet, F};

pub struct LinguisticVar {
    pub sets: HashMap<String, FuzzySet>,
    pub universe: (f64, f64),
}

impl LinguisticVar {
    pub fn new(inputs: Vec<(&str, F)>, universe: (f64, f64)) -> Self {
        let sets: HashMap<String, FuzzySet> = HashMap::from_iter(
            inputs
                .iter()
                .map(|(name, f)| (name.to_string(), FuzzySet::new(universe, f.clone()))),
        );
        LinguisticVar { sets, universe }
    }

    pub fn term(&self, name: &str) -> Option<&FuzzySet> {
        self.sets.get(name)
    }

    pub fn get_finite_universe(&self, resolution: f64) -> Vec<f64> {
        arange(self.universe.0, self.universe.1, resolution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{arange, shape::triangle};

    #[test]
    fn test_empty() {
        let var1 = LinguisticVar::new(vec![], (0f64, 10f64));
        assert_eq!(var1.sets.len(), 0);
    }

    #[test]
    fn test_term() {
        let var1 = LinguisticVar::new(
            vec![
                ("normal", triangle(5f64, 0.8, 3f64)),
                ("weak", triangle(3f64, 0.8, 1.5f64)),
            ],
            (0f64, 10f64),
        );

        assert_eq!(var1.term("normal").unwrap().degree_of(5.0), 0.8);
        assert_eq!(var1.term("weak").unwrap().degree_of(3.0), 0.8);
        match var1.term("strongl") {
            Some(_) => assert!(false),
            None => assert!(true),
        }
    }
}
