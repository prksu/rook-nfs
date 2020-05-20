package main

import (
	"fmt"
	"os"

	"github.com/prksu/rook-nfs/pkg/apis"
	"github.com/prksu/rook-nfs/pkg/nfs"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
)

const (
	provisionerNameKey = "PROVISIONER_NAME"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = apis.AddToScheme(scheme)
}

func main() {
	provisionerName := os.Getenv(provisionerNameKey)
	if provisionerName == "" {
		fmt.Printf("Environment variable %s is not set! Please set it.", provisionerNameKey)
		os.Exit(1)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		fmt.Printf("Failed to create config: %v", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Failed to create client: %v", err)
		os.Exit(1)
	}

	client, err := crclient.New(config, crclient.Options{Scheme: scheme})
	if err != nil {
		fmt.Println("Failed to create controller-runtime client")
		os.Exit(1)
	}

	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		fmt.Printf("Error getting server version: %v", err)
		os.Exit(1)
	}

	provisioner := &nfs.Provisioner{
		Client: client,
	}

	controller.NewProvisionController(clientset, provisionerName, provisioner, serverVersion.GitVersion).Run(wait.NeverStop)
}
