// Package errs provides common errors and method to handle them.
package errs

import (
	"fmt"
	"runtime"
)

// WrapErr fits the error in a chain, reports source file and provides optional description.
func WrapErr(e error, desc ...string) error {
	if e == nil {
		return nil
	}
	var d string
	if len(desc) > 0 {
		d = desc[0] + " "
	}
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		return fmt.Errorf("undefined call %s-> %w", d, e)
	}
	return fmt.Errorf("%s:%d %s-> %w", file, line, d, e)
}
