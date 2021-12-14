package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"zgo.at/errors"
	"zgo.at/zdb"
	_ "zgo.at/zdb/drivers/go-sqlite3"
	_ "zgo.at/zdb/drivers/pq"
	"zgo.at/zli"
	"zgo.at/zstd/zfilepath"
	"zgo.at/zstd/zstring"
	"zgo.at/zstd/zsync"
	"zgo.at/zstd/ztime"
)

var usage = `
sqlbench runs benchmarks on an SQL database.
https://github.com/arp242/sqlbench/

To run a benchmark at least two pieces of information are needed:

    -query       The query to test.
    -params      A list of parameters to run the query with.

It will run the query for every entry of -params. You can optionally setup any
table(s) with -setup. For example, to run the testdata:

    % sqlbench \
        -db     sqlite+/tmp/test.sqlite3 \
        -setup  testdata/schema.sql \
        -setup  cpu_usage:testdata/data.csv \
        -params testdata/params.csv \
        -query 'select * from cpu_usage where host=? and ts>=? and ts<=?'

data flags:

    -s, -setup [file]   setup the database; any *.sql files are run as-is, and
                        any .csv data files need to be prefixed with the table
                        name as "tbl:file.csv".

                        this flag can be added more than once, and are run in
                        order; for example to create "mytable" with schema.sql
                        and then insert data from data.csv:

                            % sqlbench -setup schema.sql -setup mytable:data.csv

    -q, -query [str]    query to run; can use "?" as placeholders for the
                        parameters.

    -p, -params [file]  a csv file with parameters, one per line. this will read
                        from stdin if [file] is "-" (which is the default). can
                        be added more than once to load from multiple csv files.

other flags:

    -h, -help           Display this help.

    -v, -verbose        Print the query as it's run; add twice to also print the
                        query plan.

    -d, -db             Database connection string, as engine+connect, e.g.
                        postgres+dbname=mydb. Default is "postgres", which uses
                        the PG* environment variables.

    -w, -warmup         Run the dataset once before we actually measure anything.

    -c, -concurrent [n] Run [n] queries concurrently. Default is 1.

    -r, -repeat [n]     Repeat the list of parameters [n] times. Default is 1.

    -f, -failfast       Immediately exit on the first error.
`[1:]

// testhook to use a different recorder, so we can insert "fake" times, rather
// than the actual times (queries are still run, just with the return value of
// this recorded).
var testHook interface {
	zdb.MetricRecorder
	Reset()
	Queries() []struct {
		Query string
		Times ztime.Durations
	}
}

var testWall time.Duration

func main() {
	f := zli.NewFlags(os.Args)
	var (
		setup  = f.StringList(nil, "setup", "s")
		query  = f.String("", "query", "q")
		params = f.StringList([]string{"-"}, "params", "p")

		help       = f.Bool(false, "help", "h")
		verbose    = f.IntCounter(0, "verbose", "v")
		dbConnect  = f.String("postgres+", "db", "d")
		warmup     = f.Bool(false, "warmup", "w")
		concurrent = f.Int(1, "concurrent", "c")
		repeat     = f.Int(1, "repeat", "r")
		failfast   = f.Bool(false, "f", "failfast")
	)
	err := f.Parse()
	if err != nil {
		zli.F(err)
		return
	}
	if help.Bool() || f.Shift() == "help" {
		fmt.Fprint(zli.Stdout, usage)
		zli.Exit(0)
		return
	}

	if query.String() == "" {
		zli.Fatalf("-query must be set")
		return
	}

	paramList, err := readParams(query.String(), params.Strings(), verbose.Int() >= 1)
	if err != nil {
		zli.F(err)
		return
	}

	db, err := connectDB(dbConnect.String(), setup.Strings(), verbose.Int() >= 1)
	if err != nil {
		zli.F(err)
		return
	}

	var met interface {
		zdb.MetricRecorder
		Reset()
		Queries() []struct {
			Query string
			Times ztime.Durations
		}
	} = zdb.NewMetricsMemory(0)
	if testHook != nil {
		met = testHook
	}
	db = zdb.NewMetricsDB(db, met)
	switch {
	case verbose.Int() >= 2:
		db = zdb.NewLogDB(db, os.Stderr, zdb.DumpQuery|zdb.DumpExplain, "")
	case verbose.Int() >= 1:
		db = zdb.NewLogDB(db, os.Stderr, zdb.DumpQuery, "")
	}

	var (
		ctx     = context.Background()
		errs    = errors.NewGroup(10)
		run     = zsync.NewAtMost(concurrent.Int())
		started = time.Now()
	)

	runOnce := func() {
		for _, args := range paramList {
			func(args []interface{}) {
				run.Run(func() {
					err := db.Exec(ctx, query.String(), args...)
					if errs.Append(err) && failfast.Bool() {
						zli.F(err)
						return
					}
				})
			}(args)
		}
	}

	if warmup.Bool() {
		runOnce()
		run.Wait()
		if errs.Len() > 0 {
			zli.F(errs)
			return
		}
		started = time.Now()
		met.Reset()
	}

	for n := 0; n < repeat.Int(); n++ {
		runOnce()
	}
	run.Wait()

	if errs.Len() > 0 {
		fmt.Fprint(zli.Stderr, errs)
	}

	printReport(met.Queries()[0].Times, time.Now().Sub(started), len(paramList), repeat.Int())
	zli.Exit(0)
}

func printReport(times ztime.Durations, wall time.Duration, n, repeat int) {
	if testWall > 0 {
		wall = testWall
	}
	p := func(d time.Duration) string {
		return ztime.DurationAs(d.Round(time.Millisecond), time.Millisecond)
	}

	fmt.Fprintf(zli.Stdout, "Ran %d queries in total", n*repeat)
	if repeat > 1 {
		fmt.Fprintf(zli.Stdout, " (%d × %d parameters)", repeat, n)
	}
	fmt.Fprintln(zli.Stdout)
	fmt.Fprintf(zli.Stdout, "  Wall time: %6s ms\n", p(wall))
	fmt.Fprintf(zli.Stdout, "  Run time:  %6s ms\n", p(times.Sum()))
	fmt.Fprintf(zli.Stdout, "  Min:       %6s ms\n", p(times.Min()))
	fmt.Fprintf(zli.Stdout, "  Max:       %6s ms\n", p(times.Max()))
	fmt.Fprintf(zli.Stdout, "  Median:    %6s ms\n", p(times.Median()))
	fmt.Fprintf(zli.Stdout, "  Mean:      %6s ms\n", p(times.Mean()))

	fmt.Fprintln(zli.Stdout, "\n  Distribution:")
	dist := times.Distrubute(4)
	var (
		widthDur, widthNum int
		widthBar           = 50.0
	)
	for _, d := range dist {
		if l := len(p(d.Min())); l > widthDur {
			widthDur = l
		}
		if l := len(strconv.Itoa(d.Len())); l > widthNum {
			widthNum = l
		}
	}

	format := fmt.Sprintf("    ≤ %%%ds ms → %%%dd  %%s %%.1f%%%%\n", widthDur, widthNum)
	l := float64(times.Len())
	for _, h := range dist {
		r := int(widthBar / (l / float64(h.Len())))
		perc := float64(h.Len()) / l * 100
		fmt.Fprintf(zli.Stdout, format, p(h.Max()), h.Len(), strings.Repeat("▬", r), perc)
	}
}

func readParams(query string, params []string, verbose bool) ([][]interface{}, error) {
	all := make([][]interface{}, 0, 128)
	for _, p := range params {
		if verbose && p != "-" {
			fmt.Printf("Reading parameters from %q …", p)
		}

		fp, err := zli.InputOrFile(p, false)
		if err != nil {
			return nil, err
		}
		defer fp.Close()

		cr, err := csv.NewReader(fp).ReadAll()
		if err != nil {
			return nil, err
		}
		fp.Close()

		// Convert [][]string to [][]interface{}, as that's what's needed for
		// the db query. Kinda sucks we need to copy the lot :-/
		rows := make([][]interface{}, 0, len(cr))
		for _, r := range cr[1:] {
			row := make([]interface{}, 0, len(r))
			for _, r2 := range r {
				row = append(row, r2)
			}
			rows = append(rows, row)
		}
		all = append(all, rows...)
		if verbose {
			fmt.Printf(" %d parameters read\n", len(rows))
		}
	}
	return all, nil
}

func connectDB(dbConnect string, setup []string, verbose bool) (zdb.DB, error) {
	db, err := zdb.Connect(context.Background(), zdb.ConnectOptions{
		Connect: dbConnect,
		Create:  true,
	})
	if err != nil {
		return nil, err
	}

	for _, s := range setup {
		_, ext := zfilepath.SplitExt(s)
		switch ext {
		default:
			if verbose {
				fmt.Printf("Running -setup flag %q as SQL …", s)
			}

			d, err := os.ReadFile(s)
			if err != nil {
				return nil, err
			}

			err = db.Exec(context.Background(), string(d))
			if err != nil {
				return nil, fmt.Errorf("running %q: %w", s, err)
			}
		case "csv":
			tbl, file := zstring.Split2(s, ":")
			if file == "" {
				return nil, fmt.Errorf("wrong value for -setup: %q: csv files need to be as 'tablename:file.csv'", s)
			}

			if verbose {
				fmt.Printf("Running -setup flag %q as CSV for table %q …", file, tbl)
			}

			fp, err := os.Open(file)
			if err != nil {
				return nil, err
			}
			defer fp.Close()

			c := csv.NewReader(fp)

			header, err := c.Read()
			if err != nil {
				return nil, err
			}

			// psql's \copy works by opening a fd and then passing that off to
			// "copy [..] from stdin" with the fd set as stdn. We can't really
			// do that here, since we don't really have a way to set stdin.
			//
			// So we use "insert (..) values (..)". This also has the advantage
			// that it works with SQLite, but is about twice as slow with
			// testdata/data.csv :-(
			//
			// Look at using pq.CopyIn():
			//   https://github.com/lib/pq/blob/b2901c7/doc.go#L163
			// Or pgx.CopyFrom():
			//   https://github.com/jackc/pgx/blob/0d20d12/doc.go#L273
			//
			// This should really be supported by zdb's BulkInsert() (assuming
			// it's actually faster).
			bi := zdb.NewBulkInsert(zdb.WithDB(context.Background(), db), tbl, header)
			for {
				row, err := c.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}

				iRow := make([]interface{}, 0, len(row))
				for _, r := range row {
					iRow = append(iRow, r)
				}
				bi.Values(iRow...)
			}
			err = bi.Finish()
			if err != nil {
				return nil, err
			}
		}
		if verbose {
			fmt.Println(" Okay")
		}
	}

	return db, nil
}
