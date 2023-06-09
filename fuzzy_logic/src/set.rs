use rayon::prelude::*;
use std::sync::Arc;

use crate::{arange, F};

fn minf(mf: &F, input: f64) -> F {
    let f = Arc::clone(mf);
    Arc::new(move |x: f64| -> f64 { input.min((f)(x)) })
}

fn std_unionf(mf1: &F, mf2: &F) -> F {
    let f1 = Arc::clone(mf1);
    let f2 = Arc::clone(mf2);
    Arc::new(move |x: f64| -> f64 { (f1)(x).max((f2)(x)) })
}

fn std_intersectf(mf1: &F, mf2: &F) -> F {
    let f1 = Arc::clone(mf1);
    let f2 = Arc::clone(mf2);
    Arc::new(move |x: f64| -> f64 { (f1)(x).min((f2)(x)) })
}

#[derive(Clone)]
pub struct FuzzySet {
    pub universe: (f64, f64), // a range
    pub membership_f: F,      // a function
}

impl FuzzySet {
    pub fn new(universe: (f64, f64), fuzzy_f: F) -> Self {
        if universe.1 < universe.0 {
            panic!("universe end can not be less than start");
        }

        FuzzySet {
            universe,
            membership_f: fuzzy_f,
        }
    }

    pub fn get_finite_universe(&self, resolution: f64) -> Vec<f64> {
        arange(self.universe.0, self.universe.1, resolution)
    }

    /// Return the degree of membership of the input value in the FuzzySet.
    pub fn degree_of(&self, input: f64) -> f64 {
        if input >= self.universe.0 && input <= self.universe.1 {
            (self.membership_f)(input).min(1f64).max(0f64)
        } else {
            0f64
        }
    }

    /// Return a new FuzzySet with the membership function that will not exceed the input value.
    pub fn min(&self, input: f64) -> FuzzySet {
        FuzzySet::new(self.universe, minf(&self.membership_f, input))
    }

    /// Return a new FuzzySet with the membership function that is the standard union (max) of the two FuzzySets.
    /// or None if the two FuzzySets have different universes.
    pub fn std_union(&self, set: &FuzzySet) -> Option<FuzzySet> {
        if self.universe != set.universe {
            return None;
        }
        Some(FuzzySet::new(
            self.universe,
            std_unionf(&self.membership_f, &set.membership_f),
        ))
    }

    /// Return a new FuzzySet with the membership function that is the standard intersect (min) of the two FuzzySets.
    /// or None if the two FuzzySets have different universes.
    pub fn std_intersect(&self, set: &FuzzySet) -> Option<FuzzySet> {
        if self.universe != set.universe {
            return None;
        }
        Some(FuzzySet::new(
            self.universe,
            std_intersectf(&self.membership_f, &set.membership_f),
        ))
    }

    pub fn centroid_defuzz(&self, resolution: f64) -> f64 {
        let universe = self.get_finite_universe(resolution);
        let (mf_sum, mf_weighted_sum) = universe
            .par_iter()
            .map(|x| ((self.membership_f)(*x), (self.membership_f)(*x) * x))
            .reduce(|| (0.0, 0.0), |acc, v| (acc.0 + v.0, acc.1 + v.1));

        if mf_sum == 0.0 {
            return 0.0;
        }
        mf_weighted_sum / mf_sum
    }
}

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;

    use super::*;
    use crate::shape::triangle;

    #[test]
    #[should_panic]
    fn test_invalid_universe() {
        let _ = FuzzySet::new((10f64, 0f64), triangle(5f64, 0.8f64, 3f64));
    }

    #[test]
    fn test_chain_union() {
        let s1 = FuzzySet::new((0f64, 10f64), triangle(5f64, 0.8f64, 3f64));
        let s2 = s1.min(0.5f64);
        let s3 = s2.min(0.2f64);
        let l = vec![s1, s2, s3];

        let res = l
            .iter()
            .fold(None, |acc, x| match acc {
                None => Some(x.clone()),
                Some(s) => x.std_union(&s),
            })
            .unwrap();

        assert_eq!(res.degree_of(5f64), 0.8);
    }

    #[test]
    fn test_centroid_defuzz() {
        let s1 = FuzzySet::new((0f64, 10f64), triangle(5f64, 0.8f64, 3f64));
        assert!(approx_eq!(
            f64,
            s1.centroid_defuzz(0.01),
            5f64,
            epsilon = 1e-6
        ));
    }

    #[test]
    fn test_min() {
        let s1 = FuzzySet::new((0f64, 10f64), triangle(5f64, 0.8f64, 3f64));
        let s2 = s1.min(0.5f64);

        assert_eq!(s1.degree_of(5.0f64), 0.8);
        assert_eq!(s2.degree_of(5.0f64), 0.5);
    }

    #[test]
    fn test_degree() {
        let s1 = FuzzySet::new((0f64, 10f64), triangle(5f64, 0.8f64, 3f64));

        assert_eq!(s1.degree_of(11.0f64), 0.0);
        assert_eq!(s1.degree_of(5.0f64), 0.8);
        assert_eq!(s1.degree_of(3.5f64), 0.4);
        assert_eq!(s1.degree_of(0.0f64), 0.0);
        assert_eq!(s1.degree_of(-1.0f64), 0.0);
    }

    #[test]
    fn test_degree_out_of_range() {
        let s1 = FuzzySet::new((0f64, 10f64), triangle(5f64, 0.8f64, 20f64));
        assert_eq!(s1.degree_of(11.0f64), 0.0);
        assert_eq!(s1.degree_of(-1.0f64), 0.0);
    }
}
