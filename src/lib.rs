#![feature(btree_retain)]
#![allow(non_camel_case_types)]

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use std::cmp::Ordering;
use pgx::*;
use pgx::pg_sys::{GISTENTRY, StrategyNumber, Oid, GistEntryVector, Datum, GIST_SPLITVEC};
use std::ops::{Deref, DerefMut};

pg_module_magic!();

/// A vector version data type. It allows to track causality of events.
#[derive(PostgresType, PostgresEq, Debug, Clone, Hash, Serialize, Deserialize)]
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

/// Returns a partial counter value at the given key.
/// Returns 0 if key didn't exist in given `timestamp`.
#[pg_extern]
pub fn valueat(timestamp: vectime, key: String) -> i64 {
    *timestamp.0.get(&key).unwrap_or(&0)
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

impl Eq for vectime { }

/* Note on Support functions for GiST index

   Postgres has 5 required and 2 optional functions that must be defined in order to support GiST
   indexing:

   1. same
   2. consistent
   3. union
   4. penalty
   5. picksplit
   6. compress
   7. decompress

   See also: https://www.postgresql.org/docs/10/gist-extensibility.html */

const STRATEGY_NUMBER_CONCURRENT: StrategyNumber = 3;
const STRATEGY_NUMBER_EQUAL: StrategyNumber = 6;
const STRATEGY_NUMBER_GREATER: StrategyNumber = 7;
const STRATEGY_NUMBER_LESS: StrategyNumber = 8;

const PENALTY_CONCURRENT: f32 = 1.0;
const PENALTY_EQUAL: f32 = 0.0;
const PENALTY_GREATER: f32 = 2.0;
const PENALTY_LESS: f32 = 3.0;

impl vectime {
    unsafe fn payload_len(datum: Datum) -> usize {
        let varlena = pg_sys::pg_detoast_datum_packed(datum as *mut pg_sys::varlena);
        varsize_any_exhdr(varlena)
    }
}

/// Given an index entry p and a query value q, this function determines whether the index entry is
/// “consistent” with the query; that is, could the predicate “indexed_column indexable_operator q”
/// be true for any row represented by the index entry? For a leaf index entry this is equivalent to
/// testing the indexable condition, while for an internal tree node this determines whether it is
/// necessary to scan the subtree of the index represented by the tree node. When the result is true,
/// a recheck flag must also be returned. This indicates whether the predicate is certainly true or
/// only possibly true. If `recheck = false` then the index has tested the predicate condition exactly,
/// whereas if `recheck = true` the row is only a candidate match. In that case the system will
/// automatically evaluate the indexable_operator against the actual row value to see if it is really
/// a match. This convention allows GiST to support both lossless and lossy index structures.
///
/// `entry.key` contains the current `vectime` being checked.
/// `query` is the query being performed
/// `strategy` is the number of operator in operator class
#[pg_extern]
pub fn consistent(entry: Internal<GISTENTRY>, query: vectime, strategy: i16, subtype: Oid, mut recheck: Internal<bool>) -> bool {

    let exact = recheck.0.deref_mut();
    let value = unsafe { vectime::from_datum(entry.0.key, false, 0) };

    if let Some(value) = value {
        // all cases served by this func are exact
        *exact = true;

        match strategy as StrategyNumber {
            STRATEGY_NUMBER_CONCURRENT => value.partial_cmp(&query) == None,
            STRATEGY_NUMBER_EQUAL => value == query,
            STRATEGY_NUMBER_GREATER => value > query,
            STRATEGY_NUMBER_LESS => value < query,
            _ => panic!("vectime - strategy type not supported: {}", strategy)
        }
    } else {
        false
    }
}

/// This method consolidates information in the tree. Given a set of entries, this function generates
/// a new index entry that represents all the given entries.
#[pg_extern]
pub fn union(args: Internal<GistEntryVector>) -> vectime {
    let v = args.0.deref();
    unsafe {
        let entries = v.vector.as_slice(v.n as usize);
        if entries.len() == 1 {
            vectime::from_datum(entries[0].key, false, 0).unwrap_or_default()
        } else {
            let mut result = vectime::default();
            for entry in entries {
                let value = vectime::from_datum(entry.key, false, 0).unwrap_or_default();
                result = max(result, value);
            }
            result
        }
    }
}

/// Converts the data item into a format suitable for physical storage in an index page.
#[pg_extern]
pub fn compress(entry: Internal<GISTENTRY>) -> Datum {
    //TODO: at the moment we don't compress these, eventually vector time can be compressed by
    // putting keys into separate space and leaving sequence numbers as an array

    entry.0.into_datum().unwrap()
}

/// The reverse of the `compress` method. Converts the index representation of the data item into
/// a format that can be manipulated by the other GiST methods in the operator class.
#[pg_extern]
pub fn decompress(entry: Internal<GISTENTRY>) -> Datum {
    //TODO: at the moment we don't compress these, eventually vector time can be compressed by
    // putting keys into separate space and leaving sequence numbers as an array

    entry.0.into_datum().unwrap()
}

/// Returns a value indicating the “cost” of inserting the new entry into a particular branch of the
/// tree. Items will be inserted down the path of least penalty in the tree. Values returned by
/// penalty should be non-negative. If a negative value is returned, it will be treated as zero.
#[pg_extern]
pub fn penalty(origin: Internal<GISTENTRY>, new_entry: Internal<GISTENTRY>, mut penalty: Internal<f32>) -> f32 {
    let old = unsafe { vectime::from_datum(origin.0.key, false, 0).unwrap_or_default() };
    let new = unsafe { vectime::from_datum(new_entry.0.key, false, 0).unwrap_or_default() };
    let p  = penalty.0.deref_mut();

    *p = match old.partial_cmp(&new) {
        None => PENALTY_CONCURRENT,
        Some(Ordering::Equal) => PENALTY_EQUAL,
        Some(Ordering::Greater) => PENALTY_GREATER,
        Some(Ordering::Less) => PENALTY_LESS,
    };

    *p
}

/// When an index page split is necessary, this function decides which entries on the page are to
/// stay on the old page, and which are to move to the new page.
#[pg_extern]
pub fn picksplit(entry: Internal<GistEntryVector>, mut split: Internal<GIST_SPLITVEC>) -> Datum {
    let v = unsafe { entry.0.vector.as_slice(entry.0.n as usize) };


    unimplemented!()
}

/* End of support for GiST Index */

#[pg_operator(immutable, parallel_safe)]
#[opname(?#)]
pub fn intersects(t1: vectime, t2: vectime) -> bool {
    t1.partial_cmp(&t2).is_none()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(~=)]
pub fn same(t1: vectime, t2: vectime) -> bool {
    t1.partial_cmp(&t2) == Some(Ordering::Equal)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(@>)]
pub fn contains(t1: vectime, t2: vectime) -> bool {
    t1.partial_cmp(&t2) == Some(Ordering::Greater)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(<@)]
pub fn contained(t1: vectime, t2: vectime) -> bool {
    t1.partial_cmp(&t2) == Some(Ordering::Less)
}


extension_sql!(r#"
create operator class vectime_ops
    default for type vectime using gist as
        function    8   contained(vectime, vectime),
        function    7   contains(vectime, vectime),
        function    6   same(vectime, vectime),
        function    3   intersects(vectime, vectime);
"#);

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