package apiresources

import (
	"sort"
	"strings"

	kubeutil "github.com/lucasepe/kube/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Opts is the start of the data required to perform the operation.
type Opts struct {
	SortBy     string
	APIGroup   string
	Namespaced bool
	Verbs      []string
	Cached     bool
	Categories []string
}

// GroupResource contains the APIGroup and APIResource
type GroupResource struct {
	APIGroup        string
	APIGroupVersion string
	APIResource     metav1.APIResource
}

func Do(f kubeutil.Factory, o Opts) ([]GroupResource, error) {
	discoveryClient, err := f.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}

	if !o.Cached {
		// Always request fresh data from the server
		discoveryClient.Invalidate()
	}

	if o.SortBy == "" {
		o.SortBy = "name"
	}

	errs := []error{}
	lists, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		errs = append(errs, err)
	}

	resources := []GroupResource{}

	for _, list := range lists {
		if len(list.APIResources) == 0 {
			continue
		}

		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}

		for _, res := range list.APIResources {
			if len(res.Verbs) == 0 {
				continue
			}
			// filter apiGroup
			if len(o.APIGroup) > 0 && o.APIGroup != gv.Group {
				continue
			}
			// filter namespaced
			if o.Namespaced && o.Namespaced != res.Namespaced {
				continue
			}
			// filter to resources that support the specified verbs
			if len(o.Verbs) > 0 && !sets.NewString(res.Verbs...).HasAll(o.Verbs...) {
				continue
			}
			// filter to resources that belong to the specified categories
			if len(o.Categories) > 0 && !sets.NewString(res.Categories...).HasAll(o.Categories...) {
				continue
			}

			resources = append(resources, GroupResource{
				APIGroup:        gv.Group,
				APIGroupVersion: gv.String(),
				APIResource:     res,
			})
		}
	}

	sort.Stable(sortableResource{resources, o.SortBy})

	if len(errs) > 0 {
		return nil, utilerrors.NewAggregate(errs)
	}

	return resources, nil
}

type sortableResource struct {
	resources []GroupResource
	sortBy    string
}

func (s sortableResource) Len() int { return len(s.resources) }
func (s sortableResource) Swap(i, j int) {
	s.resources[i], s.resources[j] = s.resources[j], s.resources[i]
}
func (s sortableResource) Less(i, j int) bool {
	ret := strings.Compare(s.compareValues(i, j))
	if ret > 0 {
		return false
	} else if ret == 0 {
		return strings.Compare(s.resources[i].APIResource.Name, s.resources[j].APIResource.Name) < 0
	}
	return true
}

func (s sortableResource) compareValues(i, j int) (string, string) {
	switch s.sortBy {
	case "name":
		return s.resources[i].APIResource.Name, s.resources[j].APIResource.Name
	case "kind":
		return s.resources[i].APIResource.Kind, s.resources[j].APIResource.Kind
	}
	return s.resources[i].APIGroup, s.resources[j].APIGroup
}
