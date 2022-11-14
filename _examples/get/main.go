package main

import (
	"fmt"
	"log"
	"os"

	"github.com/lucasepe/kube/events"
	kubeutil "github.com/lucasepe/kube/util"
)

func main() {
	f := kubeutil.NewFactory("", os.Getenv("KUBECONFIG"))

	all, err := events.Do(f, events.Opts{})
	if err != nil {
		log.Fatal(err)
	}

	for _, el := range all {
		fmt.Printf("%v\n", el)
	}
}
