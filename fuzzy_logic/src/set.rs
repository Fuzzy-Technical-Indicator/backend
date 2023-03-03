use crate::shape::*;

pub fn arange(start: f64, stop: f64, interval: f64) -> Vec<f64> {
    if stop < start {
        panic!("end can not be less than start");
    } else if interval <= 0f64 {
        panic!("interval must be > 0");
    }

    let mut members: Vec<f64> = vec![];
    let r = 1.0 / interval;
    let mut n = start;
    while n <= stop {
        members.push(n);
        n += interval;
        if interval < 1.0 {
            n = (n * r).round() / r;
        }
    }
    members
}

#[derive(Clone)]
pub struct LinguisticVar {
    pub sets: Vec<FuzzySet>,
    pub universe: Vec<f64>,
}

impl LinguisticVar {
    pub fn new(inputs: Vec<(&dyn Shape, &str)>, universe: Vec<f64>) -> LinguisticVar {
        let mut sets: Vec<FuzzySet> = vec![];
        for item in inputs {
            sets.push(FuzzySet::new(&universe, item.0, item.1.to_string()));
        }
        LinguisticVar { sets, universe }
    }

    pub fn term(&self, name: &str) -> &FuzzySet {
        match self.sets.iter().find(|x| x.name == name.to_string()) {
            Some(x) => x,
            None => panic![
                "there're no fuzzy set name {} in this linguistic variable",
                name
            ],
        }
    }
}

#[derive(Debug, Clone)]
pub struct FuzzySet {
    pub name: String,
    pub universe: Vec<f64>, // universe of discourse that own this set
    pub membership: Vec<f64>,
}

impl FuzzySet {
    pub fn new(universe: &Vec<f64>, fuzzy_f: &dyn Shape, name: String) -> FuzzySet {
        let mut membership: Vec<f64> = vec![];
        for i in 0..universe.len() {
            membership.push(fuzzy_f.function(universe[i]));
        }
        FuzzySet {
            name: name.to_string(),
            universe: universe.clone(),
            membership,
        }
    }

    pub fn degree_of(&self, input: f64) -> f64 {
        // edge case
        if input < self.universe[0] {
            return self.membership[0];
        } else if input > self.universe[self.universe.len() - 1] {
            return self.membership[self.membership.len() - 1];
        }
        let mut min_x = f64::MAX;
        let mut j: usize = 0;
        for (i, x) in self.universe.iter().enumerate() {
            let diff = (x - input).abs();
            if diff < min_x {
                j = i;
                min_x = diff;
            }
        }
        self.membership[j]
    }

    pub fn centroid_defuzz(&self) -> f64 {
        let top_sum = self
            .universe
            .iter()
            .enumerate()
            .fold(0.0, |s, (x, y)| s + (self.membership[x] * y));
        let bot_sum = self.membership.iter().fold(0.0, |s, v| s + v);
        if bot_sum == 0.0 {
            return 0.0;
        }
        top_sum / bot_sum
    }

    pub fn min(&self, input: f64, name: String) -> FuzzySet {
        let mut membership: Vec<f64> = vec![];
        for i in 0..self.membership.len() {
            membership.push(self.membership[i].min(input));
        }
        FuzzySet {
            name: name.to_string(),
            universe: self.universe.clone(),
            membership,
        }
    }

    pub fn std_union(&self, set: &FuzzySet, name: String) -> FuzzySet {
        // check if domain is equal or not?
        if self.universe != set.universe {
            panic!("domain needs to be equal");
        }

        // if equal
        let mut membership: Vec<f64> = vec![];
        for i in 0..self.membership.len() {
            membership.push(self.membership[i].max(set.membership[i]));
        }
        FuzzySet {
            name: name.to_string(),
            universe: self.universe.clone(),
            membership,
        }
    }

    pub fn std_intersect(&self, set: &FuzzySet, name: String) -> FuzzySet {
        // check if domain is equal or not?
        if self.universe != set.universe {
            panic!("domain needs to be equal");
        }

        // if equal
        let mut membership: Vec<f64> = vec![];
        for i in 0..self.membership.len() {
            membership.push(self.membership[i].min(set.membership[i]));
        }
        FuzzySet {
            name: name.to_string(),
            universe: self.universe.clone(),
            membership,
        }
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

    #[test]
    fn test_degree() {
        let s1 = FuzzySet::new(
            &arange(0.0, 10.0, 0.01),
            &triangular(5f64, 0.8f64, 3f64),
            "f1".into(),
        );

        assert_eq!(s1.degree_of(11.0f64), 0.0);
        assert_eq!(s1.degree_of(5.0f64), 0.8);
        assert_eq!(s1.degree_of(3.5f64), 0.4);
        assert_eq!(s1.degree_of(0.0f64), 0.0);
        assert_eq!(s1.degree_of(-1.0f64), 0.0);
    }

    #[test]
    fn linguistic() {
        let var1 = LinguisticVar::new(
            vec![
                (&triangular(5f64, 0.8, 3f64), "normal"),
                (&triangular(3f64, 0.8, 1.5f64), "weak"),
            ],
            arange(0f64, 10f64, 0.01),
        );

        assert_eq!(var1.term("normal").degree_of(5.0), 0.8);
        assert_eq!(var1.term("weak").degree_of(3.0), 0.8);
    }
}
