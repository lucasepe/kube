package events

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	kubeutil "github.com/lucasepe/kube/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeresource "k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/kubernetes"
)

// EventsOptions is a set of options that allows you to list events.  This is the object reflects the
// runtime needs of an events command, making the logic itself easy to unit test.
type EventsOpts struct {
	Namespace     string
	AllNamespaces bool
	FilterTypes   []string

	ForGVK    schema.GroupVersionKind
	ForName   string
	ForObject string

	client *kubernetes.Clientset
}

func Do(f kubeutil.Factory, o EventsOpts) ([]corev1.Event, error) {
	if err := o.complete(f); err != nil {
		return nil, err
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	return o.run()
}

func (o *EventsOpts) complete(f kubeutil.Factory) error {
	var err error
	o.Namespace, _, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	if o.ForObject != "" {
		mapper, err := f.ToRESTMapper()
		if err != nil {
			return err
		}

		var found bool
		o.ForGVK, o.ForName, found, err = decodeResourceTypeName(mapper, o.ForObject)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("forObject must be in resource/name form")
		}
	}

	return err
}

func (o *EventsOpts) validate() error {
	for _, val := range o.FilterTypes {
		if !strings.EqualFold(val, "Normal") && !strings.EqualFold(val, "Warning") {
			return fmt.Errorf("valid types are Normal or Warning")
		}
	}

	return nil
}

// run retrieves events
func (o *EventsOpts) run() ([]corev1.Event, error) {
	ctx := context.TODO()
	namespace := o.Namespace
	if o.AllNamespaces {
		namespace = ""
	}
	listOptions := metav1.ListOptions{Limit: kubeutil.DefaultChunkSize}
	if o.ForName != "" {
		listOptions.FieldSelector = fields.AndSelectors(
			fields.OneTermEqualSelector("involvedObject.kind", o.ForGVK.Kind),
			fields.OneTermEqualSelector("involvedObject.name", o.ForName)).String()
	}

	e := o.client.CoreV1().Events(namespace)
	el := &corev1.EventList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EventList",
			APIVersion: "v1",
		},
	}
	err := runtimeresource.FollowContinue(&listOptions,
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
func (o *EventsOpts) filteredEventType(et string) bool {
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

// decodeResourceTypeName handles type/name resource formats and returns a resource tuple
// (empty or not), whether it successfully found one, and an error
func decodeResourceTypeName(mapper meta.RESTMapper, s string) (gvk schema.GroupVersionKind, name string, found bool, err error) {
	if !strings.Contains(s, "/") {
		return
	}
	seg := strings.Split(s, "/")
	if len(seg) != 2 {
		err = fmt.Errorf("arguments in resource/name form may not have more than one slash")
		return
	}
	resource, name := seg[0], seg[1]

	var gvr schema.GroupVersionResource
	gvr, err = mapper.ResourceFor(schema.GroupVersionResource{Resource: resource})
	if err != nil {
		return
	}
	gvk, err = mapper.KindFor(gvr)
	if err != nil {
		return
	}
	found = true
	return
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
