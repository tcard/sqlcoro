package sqlcoro_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/tcard/coro"
	"github.com/tcard/sqlcoro"
	"github.com/tcard/sqler"
)

func Example() {
	// rows would actually be a valid *sql.Rows, wrapped as a sqler.Rows.
	rows := &exampleRows{
		{13, "foo"},
		{42, "bar"},
	}

	// You'll typically want to tie the iterator's lifetime to its consumer's this
	// way, but it's not necessary to do so. See coro documentation for details.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coroOption := coro.KillOnContextDone(ctx)

	nextRow := sqlcoro.IterateRows(rows, coroOption)

	var row sqler.Row
	var err error
	for nextRow(&row, &err) {
		var id int
		var name string

		err := row.Scan(&id, &name)
		if err != nil {
			_ = err // handle
			return
		}

		fmt.Println("ID:", id, "Name:", name)
	}
	if err != nil {
		_ = err // handle
	}

	// Output:
	// ID: 13 Name: foo
	// ID: 42 Name: bar
}

type exampleRows [][]interface{}

type exampleRow struct {
	id   int
	name string
}

func (r *exampleRows) Next() bool {
	return len(*r) > 0
}

func (r *exampleRows) Scan(dest ...interface{}) error {
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf((*r)[0][i]))
	}
	*r = (*r)[1:]
	return nil
}

func (r *exampleRows) Err() error {
	return nil
}

func (r *exampleRows) Close() error {
	return nil
}

func (*exampleRows) ColumnTypes() ([]*sql.ColumnType, error) { panic("not provided") }
func (*exampleRows) Columns() ([]string, error)              { panic("not provided") }
func (*exampleRows) NextResultSet() bool                     { panic("not provided") }

func TestIterateRows(t *testing.T) {
	for _, c := range []struct {
		name     string
		rows     int
		err      error
		closeErr error
	}{{
		name: "with no error",
		rows: 3,
	}, {
		name: "with scanning error",
		rows: 3,
		err:  errors.New("scanning"),
	}, {
		name:     "with close error",
		rows:     3,
		closeErr: errors.New("closing"),
	}, {
		name:     "with both errors",
		rows:     3,
		err:      errors.New("scanning"),
		closeErr: errors.New("closing"),
	}, {
		name: "no rows, no error",
		rows: 0,
	}, {
		name: "no rows, scanning error",
		rows: 0,
		err:  errors.New("scanning"),
	}} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			var opt1Called, opt2Called bool

			rows := &fakeRows{rows: c.rows, err: c.err}
			nextRow := sqlcoro.IterateRows(rows, func(*coro.Options) {
				opt1Called = true
			}, func(*coro.Options) {
				opt2Called = true
			})

			var row sqler.Row
			var err error
			consumed := 0
			for nextRow(&row, &err) {
				consumed++

				if expected, got := rows, row; expected != got {
					t.Errorf("expected %v, got %v", expected, got)
				}
			}

			if expected, got := c.rows, consumed; expected != got {
				t.Errorf("expected %v, got %v", expected, got)
			}

			expectedErr := c.err
			if expectedErr == nil {
				expectedErr = c.closeErr
			}
			if expected, got := expectedErr, err; expected != got {
				t.Errorf("expected %v, got %v", expected, got)
			}

			if !rows.closed {
				t.Error("expected Close() to be called")
			}

			if !opt1Called {
				t.Error("expected option 1 to be passed to coro.New")
			}

			if !opt2Called {
				t.Error("expected option 2 to be passed to coro.New")
			}
		})
	}
}

type fakeRows struct {
	rows     int
	err      error
	closeErr error

	consumed int
	closed   bool
}

func (r *fakeRows) Next() bool {
	r.consumed++
	return r.consumed <= r.rows
}

func (r *fakeRows) Scan(dest ...interface{}) error {
	return errors.New("no-op")
}

func (r *fakeRows) Err() error {
	if r.consumed <= r.rows {
		panic("called Err before Next() returned false")
	}
	return r.err
}

func (r *fakeRows) Close() error {
	if r.consumed <= r.rows {
		panic("called Close before Next() returned false")
	}
	r.closed = true
	return r.closeErr
}

func (*fakeRows) ColumnTypes() ([]*sql.ColumnType, error) { panic("not provided") }
func (*fakeRows) Columns() ([]string, error)              { panic("not provided") }
func (*fakeRows) NextResultSet() bool                     { panic("not provided") }
