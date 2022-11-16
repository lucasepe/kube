package kube

import (
	kubeutil "github.com/lucasepe/kube/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type Opts struct {
	ChunkSize      int64
	Resources      []string
	LabelSelector  string
	FieldSelector  string
	AllNamespaces  bool
	Namespace      string
	Subresource    string
	IgnoreNotFound bool
}

func Do(f kubeutil.Factory, o Opts) ([]*unstructured.Unstructured, error) {
	if o.ChunkSize <= 0 {
		o.ChunkSize = kubeutil.DefaultChunkSize
	}

	objs := []*unstructured.Unstructured{}

	r := f.NewBuilder().
		Unstructured().
		NamespaceParam(o.Namespace).DefaultNamespace().AllNamespaces(o.AllNamespaces).
		//FilenameParam(o.ExplicitNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.LabelSelector).
		FieldSelectorParam(o.FieldSelector).
		Subresource(o.Subresource).
		RequestChunksOf(o.ChunkSize).
		ResourceTypeOrNameArgs(true, o.Resources...).
		ContinueOnError().
		Latest().
		Flatten().
		Do()

	if o.IgnoreNotFound {
		r.IgnoreErrors(apierrors.IsNotFound)
	}
	if err := r.Err(); err != nil {
		return objs, err
	}

	infos, err := r.Infos()
	if err != nil {
		return objs, err
	}

	for _, info := range infos {
		objs = append(objs, info.Object.(*unstructured.Unstructured))
	}

	return objs, nil
}
