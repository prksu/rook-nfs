package nfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"

	"github.com/prksu/rook-nfs/pkg/apis/nfs.rook.io/v1alpha1"
)

const (
	mountPath = "/persistentvolumes"
)

type Provisioner struct {
	Client crclient.Client
}

var _ controller.Provisioner = &Provisioner{}

func (p *Provisioner) Provision(options controller.ProvisionOptions) (*v1.PersistentVolume, error) {
	if options.PVC.Spec.Selector != nil {
		return nil, fmt.Errorf("claim Selector is not supported")
	}

	sc, err := p.storageClassForPVC(options.PVC)
	if err != nil {
		return nil, err
	}

	serverName, present := sc.Parameters["nfsServerName"]
	if !present {
		return nil, errors.Errorf("NFS share Path not found in the storageclass: %v", sc.GetName())
	}

	serverNamespace, present := sc.Parameters["nfsServerNamespace"]
	if !present {
		return nil, errors.Errorf("NFS share Path not found in the storageclass: %v", sc.GetName())
	}

	exportName, present := sc.Parameters["exportName"]
	if !present {
		return nil, errors.Errorf("NFS share Path not found in the storageclass: %v", sc.GetName())
	}

	nfsserver := &v1alpha1.NFSServer{}
	if err := p.Client.Get(context.Background(), crclient.ObjectKey{Namespace: serverNamespace, Name: serverName}, nfsserver); err != nil {
		return nil, err
	}

	nfsserversvc := &v1.Service{}
	if err := p.Client.Get(context.Background(), crclient.ObjectKey{Namespace: serverNamespace, Name: serverName}, nfsserversvc); err != nil {
		return nil, err
	}

	var path string
	for _, export := range nfsserver.Spec.Exports {
		if export.Name == exportName {
			path = export.PersistentVolumeClaim.ClaimName
		}
	}

	dir := strings.Join([]string{options.PVC.Namespace, options.PVC.Name, options.PVName}, "-")
	fullPath := filepath.Join(mountPath, path, dir)
	if err := os.MkdirAll(fullPath, 0777); err != nil {
		return nil, errors.New("unable to create directory to provision new pv: " + err.Error())
	}
	os.Chmod(fullPath, 0777)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: *options.StorageClass.ReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			MountOptions:                  options.StorageClass.MountOptions,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				NFS: &v1.NFSVolumeSource{
					Server:   nfsserversvc.Spec.ClusterIP,
					Path:     "/" + filepath.Join(path, dir),
					ReadOnly: false,
				},
			},
		},
	}

	return pv, nil
}

func (p *Provisioner) Delete(volume *v1.PersistentVolume) error {
	return nil
}

func (p *Provisioner) storageClassForPVC(pvc *v1.PersistentVolumeClaim) (*storagev1.StorageClass, error) {
	if p.Client == nil {
		return nil, fmt.Errorf("Cannot get kube client")
	}
	className := helper.GetPersistentVolumeClaimClass(pvc)
	if className == "" {
		return nil, fmt.Errorf("Volume has no storage class")
	}

	sc := &storagev1.StorageClass{}
	if err := p.Client.Get(context.Background(), crclient.ObjectKey{Name: className}, sc); err != nil {
		return nil, err
	}

	return sc, nil
}
