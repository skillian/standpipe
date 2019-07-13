package standpipe

import "fmt"

// Repr gets a Pythonic string representation of the value which is easier for
// me to read while debugging.
func Repr(v interface{}) string {
	return fmt.Sprintf("<%T at %p>", v, v)
}
