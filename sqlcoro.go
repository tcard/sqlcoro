// Package sqlcoro provides a database/sql rows iterator based on
// github.com/tcard/coro. Under the hood, it calls Next, Err and Close on a Rows
// appropriately and returns any error that those may produce.
package sqlcoro

import (
	"github.com/tcard/coro"
	"github.com/tcard/sqler"
)

type NextFunc func(yielded *sqler.Row, returned *error) (alive bool)

func IterateRows(rows sqler.Rows, options ...coro.SetOption) NextFunc {
	var err error
	var yielded sqler.Row
	f := func(yield func(sqler.Row)) (err error) {
		defer func() {
			closeErr := rows.Close()
			if err == nil {
				err = closeErr
			}
		}()
		for rows.Next() {
			yield(rows)
		}
		return rows.Err()
	}
	next := coro.New(
		func(yield func()) {
			err = f(func(v sqler.Row) {
				yielded = v
				yield()
			})
		},
		options...,
	)
	return func(into *sqler.Row, returned *error) bool {
		alive := next()
		if alive {
			*into = yielded
		}
		if err != nil && returned != nil {
			*returned = err
		}
		return alive
	}
}
