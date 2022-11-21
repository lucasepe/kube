package logs

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/lucasepe/kube/scheme"
	kubeutil "github.com/lucasepe/kube/util"
	"golang.org/x/sync/errgroup"
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

	RecordHandler func(Record) error

	// PodLogOptions
	SinceTime                    string
	Since                        time.Duration
	Follow                       bool
	Previous                     bool
	IgnoreLogErrors              bool
	LimitBytes                   int64
	Tail                         int64
	Container                    string
	InsecureSkipTLSVerifyBackend bool

	Selector             string
	MaxFollowConcurrency int

	Object        runtime.Object
	GetPodTimeout time.Duration
	LogsForObject LogsForObjectFunc

	containerNameFromRefSpecRegexp *regexp.Regexp
	requestConsumeFn               func(rest.ResponseWrapper, func(rec Record) error) error
}

func (o *Opts) toLogOptions() (*corev1.PodLogOptions, error) {
	logOptions := &corev1.PodLogOptions{
		Container:                    o.Container,
		Follow:                       o.Follow,
		Previous:                     o.Previous,
		Timestamps:                   true,
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

	o.requestConsumeFn = defaultRequestConsumeFn

	o.LogsForObject = logsForObject

	if len(o.Container) == 0 {
		o.AllContainers = true
	}

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

	if o.MaxFollowConcurrency <= 0 {
		o.MaxFollowConcurrency = 5
	}

	if o.RecordHandler == nil {
		o.RecordHandler = defaultRecordHandler
	}

	var err error
	o.Options, err = o.toLogOptions()
	if err != nil {
		return err
	}

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
				"attempting to follow %d log streams, but maximum allowed concurrency is %d",
				len(requests), o.MaxFollowConcurrency,
			)
		}

		return o.parallelConsumeRequest(requests)
	}

	return o.sequentialConsumeRequest(requests)
}

func (o Opts) parallelConsumeRequest(requests map[corev1.ObjectReference]rest.ResponseWrapper) error {
	g := new(errgroup.Group)

	for _, request := range requests {
		req := request
		g.Go(func() error {
			return o.requestConsumeFn(req, o.RecordHandler)
		})
	}

	return g.Wait()
}

func (o Opts) sequentialConsumeRequest(requests map[corev1.ObjectReference]rest.ResponseWrapper) error {
	for _, request := range requests {
		err := o.requestConsumeFn(request, o.RecordHandler)
		if err != nil {
			return err
		}
	}

	return nil
}
