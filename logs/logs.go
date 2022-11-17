package logs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/lucasepe/kube/scheme"
	kubeutil "github.com/lucasepe/kube/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

const (
	defaultPodLogsTimeout = 20 * time.Second
)

type Opts struct {
	Namespace     string
	PodName       string
	AllContainers bool
	Options       runtime.Object
	Out           io.Writer

	ConsumeRequestFn func(rest.ResponseWrapper, io.Writer) error

	// PodLogOptions
	SinceTime                    string
	Since                        time.Duration
	Follow                       bool
	Previous                     bool
	Timestamps                   bool
	IgnoreLogErrors              bool
	LimitBytes                   int64
	Tail                         int64
	Container                    string
	InsecureSkipTLSVerifyBackend bool

	Selector             string
	MaxFollowConcurrency int
	Prefix               bool

	Object        runtime.Object
	GetPodTimeout time.Duration
	LogsForObject LogsForObjectFunc

	containerNameFromRefSpecRegexp *regexp.Regexp
}

func (o *Opts) toLogOptions() (*corev1.PodLogOptions, error) {
	logOptions := &corev1.PodLogOptions{
		Container:                    o.Container,
		Follow:                       o.Follow,
		Previous:                     o.Previous,
		Timestamps:                   o.Timestamps,
		InsecureSkipTLSVerifyBackend: o.InsecureSkipTLSVerifyBackend,
	}

	if len(o.SinceTime) > 0 {
		t, err := kubeutil.ParseRFC3339(o.SinceTime, metav1.Now)
		if err != nil {
			return nil, err
		}

		logOptions.SinceTime = &t
	}

	if o.LimitBytes != 0 {
		logOptions.LimitBytes = &o.LimitBytes
	}

	if o.Since != 0 {
		// round up to the nearest second
		sec := int64(o.Since.Round(time.Second).Seconds())
		logOptions.SinceSeconds = &sec
	}

	if len(o.Selector) > 0 && o.Tail == -1 {
		selectorTail := int64(10)
		logOptions.TailLines = &selectorTail
	} else if o.Tail != -1 {
		logOptions.TailLines = &o.Tail
	}

	return logOptions, nil
}

func (o *Opts) complete(f kubeutil.Factory) error {
	o.containerNameFromRefSpecRegexp =
		regexp.MustCompile(`spec\.(?:initContainers|containers|ephemeralContainers){(.+)}`)

	//if len(o.Container) == 0 {
	//	o.AllContainers = true
	//}

	if o.LimitBytes < 0 {
		o.LimitBytes = 0
	}

	if o.Since < 0 {
		o.Since = 0
	}

	if o.GetPodTimeout <= 0 {
		o.GetPodTimeout = defaultPodLogsTimeout
	}

	if o.Tail <= 0 {
		o.Tail = -1
	}

	if o.Out == nil {
		o.Out = os.Stdout
	}

	if o.ConsumeRequestFn == nil {
		o.ConsumeRequestFn = DefaultConsumeRequest
	}

	if o.MaxFollowConcurrency <= 0 {
		o.MaxFollowConcurrency = 5
	}

	var err error
	o.Options, err = o.toLogOptions()
	if err != nil {
		return err
	}

	o.LogsForObject = logsForObject

	if o.Object == nil {
		builder := f.NewBuilder().
			WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
			NamespaceParam(o.Namespace).DefaultNamespace().
			SingleResourceType()
		if o.PodName != "" {
			builder.ResourceNames("pods", o.PodName)
		}
		if o.Selector != "" {
			builder.ResourceTypes("pods").LabelSelectorParam(o.Selector)
		}
		infos, err := builder.Do().Infos()
		if err != nil {
			return err
		}
		if o.Selector == "" && len(infos) != 1 {
			return errors.New("expected a resource")
		}
		o.Object = infos[0].Object
		if o.Selector != "" && len(o.Object.(*corev1.PodList).Items) == 0 {
			return fmt.Errorf("no resources found in %s namespace", o.Namespace)
		}
	}

	return nil
}

func Do(f kubeutil.Factory, o Opts) error {
	if err := o.complete(f); err != nil {
		return err
	}

	requests, err := o.LogsForObject(f, o.Object, o.Options, o.GetPodTimeout, o.AllContainers)
	if err != nil {
		return err
	}

	if o.Follow && len(requests) > 1 {
		if len(requests) > o.MaxFollowConcurrency {
			return fmt.Errorf(
				"you are attempting to follow %d log streams, but maximum allowed concurrency is %d, use --max-log-requests to increase the limit",
				len(requests), o.MaxFollowConcurrency,
			)
		}

		return o.parallelConsumeRequest(requests)
	}

	return o.sequentialConsumeRequest(requests)
}

func (o Opts) parallelConsumeRequest(requests map[corev1.ObjectReference]rest.ResponseWrapper) error {
	reader, writer := io.Pipe()

	wg := &sync.WaitGroup{}
	wg.Add(len(requests))

	for objRef, request := range requests {
		go func(objRef corev1.ObjectReference, request rest.ResponseWrapper) {
			defer wg.Done()
			out := o.addPrefixIfNeeded(objRef, writer)
			if err := o.ConsumeRequestFn(request, out); err != nil {
				if !o.IgnoreLogErrors {
					writer.CloseWithError(err)

					// It's important to return here to propagate the error via the pipe
					return
				}

				fmt.Fprintf(writer, "error: %v\n", err)
			}

		}(objRef, request)
	}

	go func() {
		wg.Wait()
		writer.Close()
	}()

	_, err := io.Copy(o.Out, reader)
	return err
}

func (o Opts) sequentialConsumeRequest(requests map[corev1.ObjectReference]rest.ResponseWrapper) error {
	for objRef, request := range requests {
		out := o.addPrefixIfNeeded(objRef, o.Out)
		if err := o.ConsumeRequestFn(request, out); err != nil {
			if !o.IgnoreLogErrors {
				return err
			}

			fmt.Fprintf(o.Out, "error: %v\n", err)
		}
	}

	return nil
}

func (o Opts) addPrefixIfNeeded(ref corev1.ObjectReference, writer io.Writer) io.Writer {
	if !o.Prefix || ref.FieldPath == "" || ref.Name == "" {
		return writer
	}

	// We rely on ref.FieldPath to contain a reference to a container
	// including a container name (not an index) so we can get a container name
	// without making an extra API request.
	var containerName string
	containerNameMatches := o.containerNameFromRefSpecRegexp.FindStringSubmatch(ref.FieldPath)
	if len(containerNameMatches) == 2 {
		containerName = containerNameMatches[1]
	}

	prefix := fmt.Sprintf("[pod/%s/%s] ", ref.Name, containerName)
	return &prefixingWriter{
		prefix: []byte(prefix),
		writer: writer,
	}
}

// DefaultConsumeRequest reads the data from request and writes into
// the out writer. It buffers data from requests until the newline or io.EOF
// occurs in the data, so it doesn't interleave logs sub-line
// when running concurrently.
//
// A successful read returns err == nil, not err == io.EOF.
// Because the function is defined to read from request until io.EOF, it does
// not treat an io.EOF as an error to be reported.
func DefaultConsumeRequest(request rest.ResponseWrapper, out io.Writer) error {
	readCloser, err := request.Stream(context.TODO())
	if err != nil {
		return err
	}
	defer readCloser.Close()

	r := bufio.NewReader(readCloser)
	for {
		bytes, err := r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}

		if _, err := out.Write(bytes); err != nil {
			return err
		}
	}
}

type prefixingWriter struct {
	prefix []byte
	writer io.Writer
}

func (pw *prefixingWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Perform an "atomic" write of a prefix and p to make sure that it doesn't interleave
	// sub-line when used concurrently with io.PipeWrite.
	n, err := pw.writer.Write(append(pw.prefix, p...))
	if n > len(p) {
		// To comply with the io.Writer interface requirements we must
		// return a number of bytes written from p (0 <= n <= len(p)),
		// so we are ignoring the length of the prefix here.
		return len(p), err
	}
	return n, err
}
