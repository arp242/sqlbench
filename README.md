sqlbench runs benchmarks on an SQL database. Right now this works for PostgreSQL
and SQLite.

You can install it with `go install zgo.at/sqlbench@latest`; by default it
required cgo and a C compiler to build for SQLite, but if you're only interested
in PostgreSQL you can use `CGO_ENABLED=0 go install zgo.at/sqlbench@latest`.

---

See the output of `sqlbench -h` for detailed help and all flags, but in brief,
to run the testdata:

    % sqlbench -concurrent 4 \
        -db     sqlite+/tmp/test.sqlite3 \
        -setup  testdata/schema.sql \
        -setup  cpu_usage:testdata/data.csv \
        -params testdata/params.csv \
        -query  'select * from cpu_usage where host=? and ts>=? and ts<=?'

    Wall time:      9 ms
    Run time:      27 ms
    Min:            0 ms
    Max:            1 ms
    Median:         0 ms
    Mean:           0 ms

    Distribution:
        ≤ 0 ms → 194  ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ 97.0%
        ≤ 0 ms →   3   1.5%
        ≤ 1 ms →   1   0.5%
        ≤ 1 ms →   2   1.0%

        Wall time:     66 ms
        Run time:     251 ms
        Min:            1 ms
        Max:           34 ms
        Median:         1 ms
        Mean:           1 ms

The database will be set up with the SQL schema in `testdata/schema.sql`,
populated with the CSV file `testdata/data.csv`, and it will run the query once
for every entry in `testdata/params.csv`. `-concurrent 4` tells sqlbench to run
four queries concurrently.

You can use PostgreSQL with e.g. `-db postgres+dbname=test`:

    % sqlbench -concurrent 4 \
        -db     postgres+dbname=test \
        -setup  testdata/schema.sql \
        -setup  cpu_usage:testdata/data.csv \
        -params testdata/params.csv \
        -query  'select * from cpu_usage where host=? and ts>=? and ts<=?'

    Wall time:     58 ms
    Run time:     218 ms
    Min:            1 ms
    Max:           15 ms
    Median:         1 ms
    Mean:           1 ms

    Distribution:
        ≤  2 ms → 194  ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ 97.0%
        ≤ 13 ms →   1   0.5%
        ≤ 14 ms →   1   0.5%
        ≤ 15 ms →   4  ▬ 2.0%
