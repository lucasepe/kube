package events

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	kubeutil "github.com/lucasepe/kube/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeresource "k8s.io/cli-runtime/pkg/resource"
)

// Opts is a set of options that allows you to list events.
type Opts struct {
	Namespace     string
	AllNamespaces bool
	FilterTypes   []string

	ForGVK  schema.GroupVersionKind
	ForName string
}

func Do(f kubeutil.Factory, o Opts) ([]corev1.Event, error) {
	if err := o.complete(f); err != nil {
		return nil, err
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	return o.run(f)
}

func (o *Opts) complete(f kubeutil.Factory) error {
	var err error
	if len(o.Namespace) == 0 {
		o.Namespace, _, err = f.ToRawKubeConfigLoader().Namespace()
		if err != nil {
			return err
		}
	}

	return err
}

func (o *Opts) validate() error {
	for _, val := range o.FilterTypes {
		if !strings.EqualFold(val, "Normal") && !strings.EqualFold(val, "Warning") {
			return fmt.Errorf("valid types are Normal or Warning")
		}
	}

	return nil
}

// run retrieves events
func (o *Opts) run(f kubeutil.Factory) ([]corev1.Event, error) {
	ctx := context.TODO()
	namespace := o.Namespace
	if o.AllNamespaces {
		namespace = ""
	}

	listOptions := metav1.ListOptions{Limit: kubeutil.DefaultChunkSize}
	if len(o.ForGVK.Kind) > 0 {
		listOptions.FieldSelector = fields.AndSelectors(
			fields.OneTermEqualSelector("involvedObject.apiVersion", o.ForGVK.GroupVersion().String()),
			fields.OneTermEqualSelector("involvedObject.kind", o.ForGVK.Kind),
		).String()
	}

	if len(o.ForName) > 0 {
		listOptions.FieldSelector = fields.OneTermEqualSelector("involvedObject.name", o.ForName).String()
	}

	fmt.Println("==>", listOptions.FieldSelector)
	cli, err := f.KubernetesClientSet()
	if err != nil {
		return nil, err
	}

	e := cli.CoreV1().Events(namespace)
	el := &corev1.EventList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EventList",
			APIVersion: "v1",
		},
	}
	err = runtimeresource.FollowContinue(&listOptions,
		func(options metav1.ListOptions) (runtime.Object, error) {
			newEvents, err := e.List(ctx, options)
			if err != nil {
				return nil, runtimeresource.EnhanceListError(err, options, "events")
			}
			el.Items = append(el.Items, newEvents.Items...)
			return newEvents, nil
		})

	if err != nil {
		return nil, err
	}

	var filteredEvents []corev1.Event
	for _, e := range el.Items {
		if !o.filteredEventType(e.Type) {
			continue
		}
		if e.GetObjectKind().GroupVersionKind().Empty() {
			e.SetGroupVersionKind(schema.GroupVersionKind{
				Version: "v1",
				Kind:    "Event",
			})
		}
		filteredEvents = append(filteredEvents, e)
	}

	el.Items = filteredEvents

	if len(el.Items) == 0 {
		if o.AllNamespaces {
			return nil, fmt.Errorf("no events found")
		}
		return nil, fmt.Errorf("no events found in %s namespace", o.Namespace)
	}

	sort.Sort(SortableEvents(el.Items))

	return el.Items, nil
}

// filteredEventType checks given event can be printed
// by comparing it in filtered event flag.
// If --event flag is not set by user, this function allows
// all events to be printed.
func (o *Opts) filteredEventType(et string) bool {
	if len(o.FilterTypes) == 0 {
		return true
	}

	for _, t := range o.FilterTypes {
		if strings.EqualFold(t, et) {
			return true
		}
	}

	return false
}

// SortableEvents implements sort.Interface for []api.Event by time
type SortableEvents []corev1.Event

func (list SortableEvents) Len() int {
	return len(list)
}

func (list SortableEvents) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list SortableEvents) Less(i, j int) bool {
	return eventTime(list[i]).Before(eventTime(list[j]))
}

// Return the time that should be used for sorting, which can come from
// various places in corev1.Event.
func eventTime(event corev1.Event) time.Time {
	if event.Series != nil {
		return event.Series.LastObservedTime.Time
	}
	if !event.LastTimestamp.Time.IsZero() {
		return event.LastTimestamp.Time
	}
	return event.EventTime.Time
}
