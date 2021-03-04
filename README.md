# pgrdt

A bunch of utilities for building conflict-free replicated data types build as an extension written in Rust. It's build on top of [pgx](https://github.com/zombodb/pgx). In order to build and run the extension, make sure that your Linux machine has postgres devel libraries in place:

```bash
cargo install cargo-pgx
cargo pgx run pg13
```

## vectime

`vectime` is a postgres data type that is expected to work as vector clock / vector version. It exposes a number of functions and operations. Example:

```sql
-- initialize pgrdt
create extension pgrdt;

-- initialize sample table
create table snapshots(
    id bigserial primary key, 
    ts vectime, 
    author text
);
insert into snapshots(ts, author) values('{"A":1}', 'Alice'), ('{"B":1}', 'Bob');

-- increment timestamp value
update snapshots set 
    ts = increment(ts, 'A', 1)
where author = 'Alice';

-- merge all existing vectimes
select max(ts) from snapshots;

-- merge two vectimes side by side
select ts || '{"C":1}'::vectime from snapshots;

-- get sum of all vectime values - equivalent of g-counter value
select valueof(ts) from snapshots;
```

A `vectime` alone is enough to provide a counter semantics. Example - a Positive-Negative Counter:

```sql
create table counters(
    name text not null, 
    inc vectime not null default '{}',
    dec vectime not null default '{}'
);
insert into counters(name, inc, dec) values('orders');
-- increment value of orders
update counters set inc = increment(inc, 'A') where name = 'orders';
-- decrement value of orders
update counters set dec = increment(dec, 'A') where name = 'orders';
-- get sum of all ordes using PN-Counter semantics
select valueof(inc) - valueof(dec) from counters;
```