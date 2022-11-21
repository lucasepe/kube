package logs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/rest"
)

type Record struct {
	timestamp time.Time
	level     string
	source    string
	msg       string
}

func (r Record) Time() time.Time {
	return r.timestamp
}

func (r Record) Level() string {
	return r.level
}

func (r Record) Source() string {
	return r.source
}

func (r Record) Msg() string {
	return r.msg
}

func (r Record) String() string {
	return strings.Join([]string{
		r.timestamp.Format(time.RFC3339),
		r.level,
		r.source,
		r.msg,
	}, " ")

}

func newRecord(dat []byte) Record {
	rec := Record{}
	parts := strings.Split(string(dat), "\t")
	if len(parts) >= 3 {
		idx := strings.Index(parts[0], " ")
		if idx != -1 {
			parts[0] = parts[0][0:idx]
		}
		rec.timestamp, _ = time.Parse(time.RFC3339, parts[0])
		rec.level = parts[1]
		rec.source = parts[2]
		rec.msg = strings.TrimSpace(strings.Join(parts[3:], " "))
	}
	return rec
}

func defaultRecordHandler(rec Record) error {
	fmt.Fprintln(os.Stdout, rec.String())
	return nil
}

// defaultRequestConsumeFn reads the data from request, and creates a Record for each line.
// It buffers data from requests until the newline or io.EOF
// occurs in the data, so it doesn't interleave logs sub-line
// when running concurrently.
//
// A successful read returns err == nil, not err == io.EOF.
// Because the function is defined to read from request until io.EOF, it does
// not treat an io.EOF as an error to be reported.
func defaultRequestConsumeFn(request rest.ResponseWrapper, fn func(Record) error) error {
	readCloser, err := request.Stream(context.TODO())
	if err != nil {
		return err
	}
	defer readCloser.Close()

	r := bufio.NewReader(readCloser)
	for {
		dat, err := r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}

		rec := newRecord(dat)
		if rec.timestamp.IsZero() {
			continue
		}

		if err := fn(newRecord(dat)); err != nil {
			return err
		}
	}
}
