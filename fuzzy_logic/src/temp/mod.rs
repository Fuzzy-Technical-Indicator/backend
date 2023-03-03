pub fn trapezoidal(a: f64, b: f64, c: f64, d: f64, e: f64) -> impl Fn(f64) -> f64 {
    move |x| {
        if x >= a && x < b {
            return ((x - a) * e) / (b - a);
        } else if x >= b && x <= c {
            return e;
        } else if x > c && x <= d {
            return e * (1.0 - (x - c).abs() / (d - c));
        }
        0.0
    }
}

pub fn triangle(a: f64, b: f64, s: f64) -> impl Fn(f64) -> f64 {
    move |x| {
        if (a - s) <= x && x <= (a + s) {
            return b * (1.0 - (x - a).abs() / s);
        }
        0.0
    }
}

// Generic is the way, every function we use is a closure that is trait object.

/// This implementation is somewhat weird? TODO
pub struct FuzzySet<F: Fn(f64) -> f64> {
    pub universe: Vec<f64>, // a finite set
    pub membership_f: F,    // a function
}

impl<F: Fn(f64) -> f64> FuzzySet<F> {
    pub fn new(universe: &Vec<f64>, fuzzy_f: F) -> Self {
        FuzzySet {
            universe: universe.clone(),
            membership_f: fuzzy_f,
        }
    }

    /// Return the degree of membership of the input value in the FuzzySet.
    pub fn degree_of(&self, input: f64) -> f64 {
        let result = (self.membership_f)(input);
        result.min(1f64).max(0f64)
    }

    /// Return a new FuzzySet with the minimum of the two membership functions,
    /// but the lifetime of new FuzzySet is the same as the original.   
    pub fn min(&self, input: f64) -> FuzzySet<impl Fn(f64) -> f64 + '_> {
        FuzzySet::new(&self.universe, minf(&self.membership_f, input))
    }


    pub fn std_union<'a>(
        &'a self,
        set: &'a FuzzySet<F>,
    ) -> Option<FuzzySet<impl Fn(f64) -> f64 + 'a>> {
        if self.universe != set.universe {
            return None;
        }
        Some(FuzzySet::new(
            &self.universe,
            std_unionf(&self.membership_f, &set.membership_f),
        ))
    }

    pub fn std_intersect<'a>(
        &'a self,
        set: &'a FuzzySet<F>,
    ) -> Option<FuzzySet<impl Fn(f64) -> f64 + 'a>> {
        if self.universe != set.universe {
            return None;
        }
        Some(FuzzySet::new(
            &self.universe,
            std_intersectf(&self.membership_f, &set.membership_f),
        ))
    }

    pub fn centroid_defuzz(&self) -> f64 {
        let bot_sum = self
            .universe
            .iter()
            .fold(0.0, |s, v| s + (self.membership_f)(*v));

        if bot_sum == 0.0 {
            return 0.0;
        }

        let top_sum = self
            .universe
            .iter()
            .fold(0.0, |s, x| s + ((self.membership_f)(*x) * x));
        top_sum / bot_sum
    }
}

fn minf<F: Fn(f64) -> f64 + Copy>(mf: F, input: f64) -> impl Fn(f64) -> f64 {
    move |x: f64| -> f64 { input.min((mf)(x)) }
}

fn std_unionf<F: Fn(f64) -> f64 + Copy>(mf1: F, mf2: F) -> impl Fn(f64) -> f64 {
    move |x: f64| -> f64 { (mf1)(x).max((mf2)(x)) }
}

fn std_intersectf<F: Fn(f64) -> f64 + Copy>(mf1: F, mf2: F) -> impl Fn(f64) -> f64 {
    move |x: f64| -> f64 { (mf1)(x).min((mf2)(x)) }
}

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;

    use crate::set::arange;

    use super::*;

    #[test]
    fn t() {
        let f1 = triangle(5f64, 0.8f64, 3f64);
        let m = 0.5f64;

        let f2 = &f1;

        let f3 = minf(&f2, m);
    }

    #[test]
    fn test_centroid_defuzz() {
        let s1 = FuzzySet::new(&arange(0f64, 10f64, 0.01), triangle(5f64, 0.8f64, 3f64));
        assert!(approx_eq!(f64, s1.centroid_defuzz(), 5f64, epsilon = 1e-6));
    }

    #[test]
    fn test_min() {
        let s1 = FuzzySet::new(&arange(0f64, 10f64, 0.01), triangle(5f64, 0.8f64, 3f64));
        let s2 = s1.min(0.5f64);

        assert_eq!(s1.degree_of(5.0f64), 0.8);
        assert_eq!(s2.degree_of(5.0f64), 0.5);
    }

    #[test]
    fn test_degree() {
        let s1 = FuzzySet::new(&arange(0f64, 10f64, 0.01), triangle(5f64, 0.8f64, 3f64));

        assert_eq!(s1.degree_of(11.0f64), 0.0);
        assert_eq!(s1.degree_of(5.0f64), 0.8);
        assert_eq!(s1.degree_of(3.5f64), 0.4);
        assert_eq!(s1.degree_of(0.0f64), 0.0);
        assert_eq!(s1.degree_of(-1.0f64), 0.0);
    }
}
