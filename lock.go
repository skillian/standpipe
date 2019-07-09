package standpipe

import (
	"runtime"
	"sync"

	"github.com/skillian/errors"
	"github.com/skillian/logging"
)

var (
	logger = logging.GetLogger("standpipe")
)

// Mutex wraps a sync.Mutex to log information about when the mutex is being
// locked and unlocked.
type Mutex sync.Mutex

// Lock the mutex.
func (m *Mutex) Lock() {
	logger.Debug2("locking %v from %v", m, getCallerName(1))
	((*sync.Mutex)(m)).Lock()
}

// Unlock the mutex.
func (m *Mutex) Unlock() {
	logger.Debug2("unlocking %v from %v", m, getCallerName(1))
	((*sync.Mutex)(m)).Unlock()
}

func getCallerName(skip int) string {
	var pcs [1]uintptr
	n := runtime.Callers(2+skip, pcs[:])
	if n != len(pcs) {
		panic(errors.Errorf("unexpected result from runtime.Callers"))
	}
	f := runtime.FuncForPC(pcs[0])
	if f == nil {
		panic(errors.Errorf("failed to get func for PC %p", pcs[0]))
	}
	return f.Name()
}
