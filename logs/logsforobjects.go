package logs

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/reference"

	//"k8s.io/kubectl/pkg/cmd/util/podcmd"
	"github.com/lucasepe/kube/scheme"
	kubeutil "github.com/lucasepe/kube/util"
)

// LogsForObjectFunc is a function type that can tell you how to get logs for a runtime.object
type LogsForObjectFunc func(restClientGetter genericclioptions.RESTClientGetter, object, options runtime.Object, timeout time.Duration, allContainers bool) (map[corev1.ObjectReference]rest.ResponseWrapper, error)

func logsForObject(restClientGetter genericclioptions.RESTClientGetter, object, options runtime.Object, timeout time.Duration, allContainers bool) (map[corev1.ObjectReference]rest.ResponseWrapper, error) {
	clientConfig, err := restClientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := corev1client.NewForConfig(clientConfig)
	if err != nil {
		return nil, err
	}
	return logsForObjectWithClient(clientset, object, options, timeout, allContainers)
}

// this is split for easy test-ability
func logsForObjectWithClient(clientset corev1client.CoreV1Interface, object, options runtime.Object, timeout time.Duration, allContainers bool) (map[corev1.ObjectReference]rest.ResponseWrapper, error) {
	opts, ok := options.(*corev1.PodLogOptions)
	if !ok {
		return nil, errors.New("provided options object is not a PodLogOptions")
	}

	switch t := object.(type) {
	case *corev1.PodList:
		ret := make(map[corev1.ObjectReference]rest.ResponseWrapper)
		for i := range t.Items {
			currRet, err := logsForObjectWithClient(clientset, &t.Items[i], options, timeout, allContainers)
			if err != nil {
				return nil, err
			}
			for k, v := range currRet {
				ret[k] = v
			}
		}
		return ret, nil

	case *corev1.Pod:
		// if allContainers is true, then we're going to locate all containers and then iterate through them. At that point, "allContainers" is false
		if !allContainers {
			currOpts := new(corev1.PodLogOptions)
			if opts != nil {
				opts.DeepCopyInto(currOpts)
			}

			if currOpts.Container == "" {
				// Default to the first container name(aligning behavior with `kubectl exec').
				currOpts.Container = t.Spec.Containers[0].Name
				// TODO: if len(t.Spec.Containers) > 1 || len(t.Spec.InitContainers) > 0 || len(t.Spec.EphemeralContainers) > 0 {
				//	fmt.Fprintf(os.Stderr, "Defaulted container %q out of: %s\n", currOpts.Container, kubeutil.AllContainerNames(t))
				//}
			}

			container, fieldPath := kubeutil.FindContainerByName(t, currOpts.Container)
			if container == nil {
				return nil, fmt.Errorf("container %s is not valid for pod %s", currOpts.Container, t.Name)
			}
			ref, err := reference.GetPartialReference(scheme.Scheme, t, fieldPath)
			if err != nil {
				return nil, fmt.Errorf("unable to construct reference to '%#v': %w", t, err)
			}

			ret := make(map[corev1.ObjectReference]rest.ResponseWrapper, 1)
			ret[*ref] = clientset.Pods(t.Namespace).GetLogs(t.Name, currOpts)
			return ret, nil
		}

		ret := make(map[corev1.ObjectReference]rest.ResponseWrapper)
		for _, c := range t.Spec.InitContainers {
			currOpts := opts.DeepCopy()
			currOpts.Container = c.Name
			currRet, err := logsForObjectWithClient(clientset, t, currOpts, timeout, false)
			if err != nil {
				return nil, err
			}
			for k, v := range currRet {
				ret[k] = v
			}
		}
		for _, c := range t.Spec.Containers {
			currOpts := opts.DeepCopy()
			currOpts.Container = c.Name
			currRet, err := logsForObjectWithClient(clientset, t, currOpts, timeout, false)
			if err != nil {
				return nil, err
			}
			for k, v := range currRet {
				ret[k] = v
			}
		}
		for _, c := range t.Spec.EphemeralContainers {
			currOpts := opts.DeepCopy()
			currOpts.Container = c.Name
			currRet, err := logsForObjectWithClient(clientset, t, currOpts, timeout, false)
			if err != nil {
				return nil, err
			}
			for k, v := range currRet {
				ret[k] = v
			}
		}

		return ret, nil
	}

	namespace, selector, err := kubeutil.SelectorsForObject(object)
	if err != nil {
		return nil, fmt.Errorf("cannot get the logs from %T: %v", object, err)
	}

	sortBy := func(pods []*corev1.Pod) sort.Interface { return kubeutil.ByLogging(pods) }
	pod, numPods, err := kubeutil.GetFirstPod(clientset, namespace, selector.String(), timeout, sortBy)
	if err != nil {
		return nil, err
	}
	if numPods > 1 {
		fmt.Fprintf(os.Stderr, "Found %v pods, using pod/%v\n", numPods, pod.Name)
	}

	return logsForObjectWithClient(clientset, pod, options, timeout, allContainers)
}
