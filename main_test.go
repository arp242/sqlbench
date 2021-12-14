package main

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"zgo.at/zli"
	"zgo.at/zstd/ztest"
	"zgo.at/zstd/ztime"
)

type testMetrics struct {
	mu      *sync.Mutex
	metrics ztime.Durations
	record  func(*ztime.Durations, time.Duration)
}

func (m *testMetrics) Record(d time.Duration, query string, params []interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record(&m.metrics, d)
}

func (m *testMetrics) Reset() {}

func (m *testMetrics) Queries() []struct {
	Query string
	Times ztime.Durations
} {
	return []struct {
		Query string
		Times ztime.Durations
	}{
		{"XXX", m.metrics},
	}
}

// Actually, what if we supported "test+[..]" as a connection string in zdb?
// That won't actually connect to a database, and we can add a testDB "wrapper"
// to return mocked versions, or something. We can use an (expanded version of)
// the test driver 🤔
func TestCmd(t *testing.T) {
	testWall = 50 * time.Millisecond
	tmpdb := t.TempDir() + "/test.sqlite3"

	tests := []struct {
		name  string
		flags []string
		want  string
	}{
		{
			"works",
			[]string{
				"-db", "sqlite+" + tmpdb,
				"-setup", "testdata/schema.sql",
				"-setup", "cpu_usage:testdata/data.csv",
				"-query", "select * from cpu_usage where host=? and ts>=? and ts<=?",
				"-params", "testdata/params.csv",
				"-concurrent", strconv.Itoa(runtime.NumCPU()),
			}, `
				Ran 200 queries in total
				  Wall time:     50 ms
				  Run time:    3900 ms
				  Min:            0 ms
				  Max:           39 ms
				  Median:        20 ms
				  Mean:          20 ms

				  Distribution:
				    ≤  9 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 19 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 29 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 39 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
			`,
		},
		{
			"re-uses existing data",
			[]string{
				"-db", "sqlite+" + tmpdb,
				"-query", "select * from cpu_usage where host=? and ts>=? and ts<=?",
				"-params", "testdata/params.csv",
				"-concurrent", strconv.Itoa(runtime.NumCPU()),
			}, `
				Ran 200 queries in total
				  Wall time:     50 ms
				  Run time:    3900 ms
				  Min:            0 ms
				  Max:           39 ms
				  Median:        20 ms
				  Mean:          20 ms

				  Distribution:
				    ≤  9 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 19 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 29 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 39 ms → 50  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
			`,
		},

		{
			"repeat",
			[]string{
				"-db", "sqlite+" + tmpdb,
				"-query", "select * from cpu_usage where host=? and ts>=? and ts<=?",
				"-params", "testdata/params.csv",
				"-concurrent", strconv.Itoa(runtime.NumCPU()),
				"-repeat", "2",
			}, `
				Ran 400 queries in total (2 × 200 parameters)
				  Wall time:     50 ms
				  Run time:   15800 ms
				  Min:            0 ms
				  Max:           79 ms
				  Median:        40 ms
				  Mean:          40 ms

				  Distribution:
				    ≤ 19 ms → 100  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 39 ms → 100  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 59 ms → 100  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
				    ≤ 79 ms → 100  ▬▬▬▬▬▬▬▬▬▬▬▬ 25.0%
			`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var i, j int
			testHook = &testMetrics{
				mu:      new(sync.Mutex),
				metrics: ztime.NewDurations(0),
				record: func(m *ztime.Durations, d time.Duration) {
					if i > 0 && i%5 == 0 {
						j++
					}
					i++
					m.Append(time.Duration(j) * time.Millisecond)
				},
			}

			exit, errOut, out := zli.Test(t)
			func() {
				defer exit.Recover()

				os.Args = append([]string{""}, tt.flags...)
				main()
			}()

			exit.Want(t, 0)
			if errOut.String() != "" {
				t.Fatalf("stderr output:\n%s", errOut)
			}

			tt.want = ztest.NormalizeIndent(tt.want)
			if d := ztest.Diff(out.String(), tt.want); d != "" {
				t.Error(d)
			}
		})
	}
}

func TestUsage(t *testing.T) {
	if strings.Contains(usage, "\t") {
		t.Error("usage contains tabs")
	}
}
