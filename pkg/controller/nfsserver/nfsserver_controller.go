package nfsserver

import (
	"context"
	"fmt"
	"strings"

	"github.com/coreos/pkg/capnslog"
	nfsv1alpha1 "github.com/prksu/rook-nfs/pkg/apis/nfs.rook.io/v1alpha1"
	"github.com/rook/rook/pkg/operator/k8sutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	controllerName   = "nfs-operator"
	nfsConfigMapPath = "/nfs-ganesha/config"
	nfsPort          = 2049
	rpcPort          = 111
)

var logger = capnslog.NewPackageLogger("github.com/rook/rook", controllerName)

// Add creates a new NFSServer Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileNFSServer{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("nfsserver-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource NFSServer
	if err = c.Watch(&source.Kind{Type: &nfsv1alpha1.NFSServer{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileNFSServer implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileNFSServer{}

// ReconcileNFSServer reconciles a NFSServer object
type ReconcileNFSServer struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a NFSServer object and makes changes based on the state read
// and what is in the NFSServer.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileNFSServer) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	logger.Info("Reconciling NFSServer")

	ctx := context.Background()
	// Fetch the NFSServer instance
	instance := &nfsv1alpha1.NFSServer{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateNFSServerConfigMap(ctx, instance); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateNFSServer(ctx, instance); err != nil {
		return reconcile.Result{}, err
	}

	logger.Info("Skip reconcile: Already updated")
	return reconcile.Result{}, nil
}

func (r *ReconcileNFSServer) createOrUpdateNFSServerConfigMap(ctx context.Context, cr *nfsv1alpha1.NFSServer) error {
	var exportsList []string

	id := 10
	for _, export := range cr.Spec.Exports {
		claimName := export.PersistentVolumeClaim.ClaimName
		var accessType string
		// validateNFSServerSpec guarantees `access` will be one of these values at this point
		switch strings.ToLower(export.Server.AccessMode) {
		case "readwrite":
			accessType = "RW"
		case "readonly":
			accessType = "RO"
		case "none":
			accessType = "None"
		}

		nfsGaneshaConfig := `
EXPORT {
	Export_Id = ` + fmt.Sprintf("%v", id) + `;
	Path = /` + claimName + `;
	Pseudo = /` + claimName + `;
	Protocols = 4;
	Transports = TCP;
	Sectype = sys;
	Access_Type = ` + accessType + `;
	Squash = ` + strings.ToLower(export.Server.Squash) + `;
	FSAL {
		Name = VFS;
	}
}`

		exportsList = append(exportsList, nfsGaneshaConfig)
		id++
	}

	nfsGaneshaAdditionalConfig := `
NFS_Core_Param {
	fsid_device = true;
}
`

	exportsList = append(exportsList, nfsGaneshaAdditionalConfig)
	configdata := make(map[string]string)
	configdata[cr.Name] = strings.Join(exportsList, "\n")
	cm := newConfigMapForNFSServer(cr)
	cmop, err := controllerutil.CreateOrUpdate(ctx, r.client, cm, func() error {
		if err := controllerutil.SetOwnerReference(cr, cm, r.scheme); err != nil {
			return err
		}

		cm.Data = configdata
		return nil
	})

	if err != nil {
		return err
	}

	logger.Infof("Reconciling NFSServer ConfigMap - Operation.Result - %s", cmop)
	return nil
}

func (r *ReconcileNFSServer) createOrUpdateNFSServer(ctx context.Context, cr *nfsv1alpha1.NFSServer) error {
	svc := newServiceForNFSServer(cr)
	svcop, err := controllerutil.CreateOrUpdate(ctx, r.client, svc, func() error {
		if !svc.ObjectMeta.CreationTimestamp.IsZero() {
			return nil
		}

		if err := controllerutil.SetControllerReference(cr, svc, r.scheme); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	logger.Infof("Reconciling NFSServer Service - Operation.Result - %s", svcop)

	sts := newStatefulSetForNFSServer(cr)
	stsop, err := controllerutil.CreateOrUpdate(ctx, r.client, sts, func() error {
		if sts.ObjectMeta.CreationTimestamp.IsZero() {
			sts.Spec.Selector = &metav1.LabelSelector{
				MatchLabels: newLabels(cr),
			}
		}

		if err := controllerutil.SetControllerReference(cr, sts, r.scheme); err != nil {
			return err
		}

		volumes := []v1.Volume{
			{
				Name: cr.Name,
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: cr.Name,
						},
						Items: []v1.KeyToPath{
							{
								Key:  cr.Name,
								Path: cr.Name,
							},
						},
					},
				},
			},
		}
		volumeMounts := []v1.VolumeMount{
			{
				Name:      cr.Name,
				MountPath: nfsConfigMapPath,
			},
		}
		for _, export := range cr.Spec.Exports {
			shareName := export.Name
			claimName := export.PersistentVolumeClaim.ClaimName
			volumes = append(volumes, v1.Volume{
				Name: shareName,
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
						ClaimName: claimName,
					},
				},
			})

			volumeMounts = append(volumeMounts, v1.VolumeMount{
				Name:      shareName,
				MountPath: "/" + claimName,
			})
		}

		sts.Spec.Template.Spec.Volumes = volumes
		sts.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts

		return nil
	})

	if err != nil {
		return err
	}

	logger.Infof("Reconciling NFSServer StatefulSet - Operation.Result - %s", stsop)
	return nil
}

func newLabels(cr *nfsv1alpha1.NFSServer) map[string]string {
	return map[string]string{
		k8sutil.AppAttr: cr.Name,
	}
}

func newConfigMapForNFSServer(cr *nfsv1alpha1.NFSServer) *v1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    newLabels(cr),
		},
	}
}

func newServiceForNFSServer(cr *nfsv1alpha1.NFSServer) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    newLabels(cr),
		},
		Spec: v1.ServiceSpec{
			Selector: newLabels(cr),
			Type:     v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{
					Name:       "nfs",
					Port:       int32(nfsPort),
					TargetPort: intstr.FromInt(int(nfsPort)),
				},
				{
					Name:       "rpc",
					Port:       int32(rpcPort),
					TargetPort: intstr.FromInt(int(rpcPort)),
				},
			},
		},
	}
}

func newStatefulSetForNFSServer(cr *nfsv1alpha1.NFSServer) *appsv1.StatefulSet {
	replicas := int32(cr.Spec.Replicas)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    newLabels(cr),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: cr.Name,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cr.Name,
					Namespace: cr.Namespace,
					Labels:    newLabels(cr),
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							ImagePullPolicy: "IfNotPresent",
							Name:            cr.Name,
							Image:           "rook/nfs:master",
							Args:            []string{"nfs", "server", "--ganeshaConfigPath=" + nfsConfigMapPath + "/" + cr.Name},
							Ports: []v1.ContainerPort{
								{
									Name:          "nfs-port",
									ContainerPort: int32(nfsPort),
								},
								{
									Name:          "rpc-port",
									ContainerPort: int32(rpcPort),
								},
							},
							SecurityContext: &v1.SecurityContext{
								Capabilities: &v1.Capabilities{
									Add: []v1.Capability{
										"SYS_ADMIN",
										"DAC_READ_SEARCH",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
