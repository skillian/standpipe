package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"

	"github.com/skillian/logging"

	"github.com/skillian/argparse"
	"github.com/skillian/errors"
	"github.com/skillian/standpipe"
)

var (
	parser = argparse.MustNewArgumentParser(
		argparse.Description(
			"standpipe to cache output from one command before " +
				"piping it into a slower command."))

	cacheFile = parser.MustAddArgument(
		argparse.OptionStrings("-f", "--cache-file"),
		argparse.Action("store"),
		argparse.Default(standpipe.ReadWriteSeekClosererFunc(createTempFile)),
		argparse.Type(func(v string) (interface{}, error) {
			return standpipe.ReadWriteSeekClosererFunc(
				func() (standpipe.ReadWriteSeekCloser, error) {
					return os.Create(v)
				}), nil
		}),
		argparse.Help("Custom cache file name.  If not used, a temp "+
			"file is created instead."))

	pageSize = parser.MustAddArgument(
		argparse.OptionStrings("-s", "--page-size"),
		argparse.Action("store"),
		argparse.Default(32768),
		argparse.Type(argparse.Int),
		argparse.Help("Page size within the standpipe file. Pages "+
			"are updated in random locations within the standpipe "+
			"file so to reduce the amount of seeking, this value "+
			"should be as large as possible.  There are two pages "+
			"always kept in memory at a time:  One for reading "+
			"and one for writing, so this value is a balancing "+
			"act between reduced seeks and memory usage"))

	logLevel = parser.MustAddArgument(
		argparse.OptionStrings("--log-level"),
		argparse.Action("store"),
		argparse.Type(func(v string) (interface{}, error) {
			level, ok := logging.ParseLevel(v)
			if !ok {
				return nil, errors.Errorf(
					"invalid log level: %q", v)
			}
			logger.SetLevel(level)
			return level, nil
		}),
		argparse.Help("Specify a custom logging level (useful for "+
			"debugging)."))

	logger = logging.GetLogger("standpipe")
)

func main() {
	args, err := parser.ParseArgs()
	panicOnError(err)

	rwscer := args.MustGet(cacheFile).(standpipe.ReadWriteSeekCloserer)

	rwsc, err := rwscer.ReadWriteSeekCloser()
	panicOnError(err)

	rc, wc, err := standpipe.NewV1Pipe(rwsc, args.MustGet(pageSize).(int))
	panicOnError(err)

	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		for range sigs {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
			panicOnError(rc.Close())
			panicOnError(wc.Close())
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(2)

	errs := make(chan error, 2)

	go writePipe(&wg, wc, os.Stdin, errs)
	go readPipe(&wg, os.Stdout, rc, errs)

	go func() {
		for err := range errs {
			panicOnError(err)
		}
	}()

	wg.Wait()
}

func doCopy(wg *sync.WaitGroup, name string, w io.Writer, r io.Reader, errs chan<- error) {
	defer wg.Done()
	logger.Debug1("Starting %s...", name)
	_, err := io.Copy(w, r)
	logger.Info1("Done %s.", name)
	if err != nil {
		errs <- err
	}
}

func readPipe(wg *sync.WaitGroup, w io.Writer, r *standpipe.V1PipeReader, errs chan<- error) {
	doCopy(wg, "pipe reader", w, r, errs)
	if err := r.Close(); err != nil {
		errs <- err
	}
}

func writePipe(wg *sync.WaitGroup, w *standpipe.V1PipeWriter, r io.Reader, errs chan<- error) {
	doCopy(wg, "pipe writer", w, r, errs)
	if err := w.Close(); err != nil {
		errs <- err
	}
}

func createTempFile() (standpipe.ReadWriteSeekCloser, error) {
	return ioutil.TempFile("", "standpipe_*.dat")
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func init() {
	h := new(logging.ConsoleHandler)
	h.SetLevel(logging.DebugLevel)
	h.SetFormatter(logging.FormatterFunc(logFormat))
	logger.AddHandler(h)
}

func logFormat(event *logging.Event) string {
	return fmt.Sprintf(event.Msg, event.Args...) + "\n"
}
