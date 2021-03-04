#![feature(btree_retain)]
#![allow(non_camel_case_types)]

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use std::cmp::Ordering;
use pgx::*;

pg_module_magic!();

/// A vector version data type. It allows to track causality of events.
#[derive(PostgresType, Debug, Clone, Hash, Serialize, Deserialize)]
#[allow(non_camel_case_types)] // we don't want PascalCase in Postgres qualified names
pub struct vectime(BTreeMap<String, i64>);

/// Increment a partial counter at a given `id` by given `delta`. `delta` must not be less than 1.
#[pg_extern]
pub fn increment(mut vt: vectime, id: String, delta: i64) -> vectime {
    if delta <= 0 {
        return vt
    }
    let e = vt.0.entry(id).or_default();
    *e = *e + delta;
    vt
}

/// Returns a merged result of two vector clocks - sum in this context is a vectime with maximum
/// values of pairwise entries of provided inputs. In case when entry existing in one input was
/// absent in the other, it's counted as 0.
#[pg_operator(commutator)]
#[opname(||)]
#[commutator(||)]
pub fn max(mut left: vectime, right: vectime) -> vectime {
    for (key, value) in right.0 {
        let e = left.0.entry(key).or_default();
        *e = (*e).max(value);
    }
    left
}

extension_sql!(r#"
create aggregate max (vectime) (
    sfunc = max,
    stype = vectime,
    initcond = '{}'
);
"#);

/// Returns a sum of all values stored within vectime `timestamp`.
/// Can be used to implement eg. Grow-only counter.
#[pg_extern]
pub fn valueof(timestamp: vectime) -> i64 {
    timestamp.0.values().sum()
}

impl PartialOrd for vectime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut a = self.0.iter();
        let mut b = other.0.iter();
        let mut result = Some(Ordering::Equal);
        let mut e1 = a.next();
        let mut e2 = b.next();
        while result != None {
            match (e1, e2) {
                (None, None) => break,
                (None, Some(_)) => {
                    match result {
                        Some(Ordering::Greater) => result = None,
                        Some(Ordering::Equal) => result = Some(Ordering::Less),
                        _ => {},
                    }
                    break;
                },
                (Some(_), None) => {
                    match result {
                        Some(Ordering::Less) => result = None,
                        Some(Ordering::Equal) => result = Some(Ordering::Greater),
                        _ => {},
                    }
                    break;
                },
                (Some((k1, v1)), Some((k2, v2))) => {
                    match k1.cmp(k2) {
                        Ordering::Equal => {
                            result =
                                match v1.partial_cmp(v2) {
                                    Some(Ordering::Greater) if result == Some(Ordering::Less) => {
                                        None
                                    },
                                    Some(Ordering::Less) if result == Some(Ordering::Greater) => {
                                        None
                                    },
                                    other if result == Some(Ordering::Equal) => other,
                                    _ => result,
                                };

                            e1 = a.next(); // A B
                            e2 = b.next(); // A C
                        },
                        Ordering::Less | Ordering::Greater => {
                            result = None;
                        },
                    }
                }
            }
        }
        result
    }
}

impl Default for vectime {
    fn default() -> Self {
        vectime(BTreeMap::new())
    }
}

impl PartialEq for vectime {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use crate::{vectime, increment, max};

    fn vtime(a: i64, b:i64, c: i64) -> vectime {
        let ts = vectime::default();
        let ts = increment(ts, "A".to_string(), a);
        let ts = increment(ts, "B".to_string(), b);
        let ts = increment(ts, "C".to_string(), c);
        ts
    }

    #[test]
    fn vtime_partial_cmp() {
        let cases = vec![
            (vtime(0,0,0), vtime(0,0,0), Some(Ordering::Equal)),
            (vtime(1,2,3), vtime(1,2,3), Some(Ordering::Equal)),
            (vtime(1,2,3), vtime(1,2,0), Some(Ordering::Greater)),
            (vtime(1,3,3), vtime(1,2,3), Some(Ordering::Greater)),
            (vtime(1,0,0), vtime(1,2,0), Some(Ordering::Less)),
            (vtime(1,2,2), vtime(1,2,3), Some(Ordering::Less)),
            (vtime(1,2,3), vtime(3,2,1), None),
            (vtime(1,0,1), vtime(1,1,0), None),
        ];

        for (left, right, expected) in cases {
            assert_eq!(left.partial_cmp(&right), expected);
        }
    }

    #[test]
    fn vtime_max() {

        fn assert_max(left: vectime, right: vectime, expected: vectime) {
            assert_eq!(max(left, right), expected);
        }

        assert_max(vtime(0, 0, 0), vtime(0, 0, 0), vtime(0, 0, 0));
        assert_max(vtime(2, 2, 3), vtime(1, 2, 0), vtime(2, 2, 3));
        assert_max(vtime(1, 3, 3), vtime(1, 2, 4), vtime(1, 3, 4));
        assert_max(vtime(1, 0, 1), vtime(1, 1, 0), vtime(1, 1, 1));
    }
}