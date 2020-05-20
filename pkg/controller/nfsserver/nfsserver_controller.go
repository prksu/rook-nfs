package nfsserver

import (
	"context"
	"fmt"
	"path/filepath"
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
	"k8s.io/apimachinery/pkg/types"
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

	owner := &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &nfsv1alpha1.NFSServer{},
	}

	if err := c.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, owner); err != nil {
		return err
	}

	if err := c.Watch(&source.Kind{Type: &corev1.Service{}}, owner); err != nil {
		return err
	}

	if err := c.Watch(&source.Kind{Type: &appsv1.StatefulSet{}}, owner); err != nil {
		return err
	}

	if err := c.Watch(&source.Kind{Type: &appsv1.Deployment{}}, owner); err != nil {
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

	if err := r.createOrUpdateConfigMap(ctx, instance); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateService(ctx, instance); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateStatefulSet(ctx, instance); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateProvisioner(ctx, instance); err != nil {
		return reconcile.Result{}, err
	}

	// Pod already exists - don't requeue
	logger.Info("Skip reconcile: Pod already exists")
	return reconcile.Result{}, nil
}

func (r *ReconcileNFSServer) createOrUpdateConfigMap(ctx context.Context, cr *nfsv1alpha1.NFSServer) error {
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

	exportsList = append(exportsList, `NFS_Core_Param {
	fsid_device = true;
}`)

	configdata := make(map[string]string)
	configdata[cr.Name] = strings.Join(exportsList, "\n")
	cm := newConfigMapForCR(cr)
	_, err := controllerutil.CreateOrUpdate(ctx, r.client, cm, func() error {
		if err := controllerutil.SetOwnerReference(cr, cm, r.scheme); err != nil {
			return err
		}

		cm.Data = configdata
		return nil
	})

	return err
}

func (r *ReconcileNFSServer) createOrUpdateService(ctx context.Context, cr *nfsv1alpha1.NFSServer) error {
	svc := newServiceForCR(cr)
	_, err := controllerutil.CreateOrUpdate(ctx, r.client, svc, func() error {
		if err := controllerutil.SetControllerReference(cr, svc, r.scheme); err != nil {
			return err
		}

		return nil
	})

	return err
}

func (r *ReconcileNFSServer) createOrUpdateStatefulSet(ctx context.Context, cr *nfsv1alpha1.NFSServer) error {
	sts := newStatefulSetForCR(cr)
	_, err := controllerutil.CreateOrUpdate(ctx, r.client, sts, func() error {
		if err := controllerutil.SetControllerReference(cr, sts, r.scheme); err != nil {
			return err
		}

		return nil
	})

	return err
}

func (r *ReconcileNFSServer) createOrUpdateProvisioner(ctx context.Context, cr *nfsv1alpha1.NFSServer) error {
	serversvc := &corev1.Service{}
	if err := r.client.Get(context.Background(), types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, serversvc); err != nil {
		return err
	}

	provisionerdep := newDeploymentForProvisioner(cr)
	_, err := controllerutil.CreateOrUpdate(ctx, r.client, provisionerdep, func() error {

		if provisionerdep.ObjectMeta.CreationTimestamp.IsZero() {
			provisionerdep.Spec.Selector = &metav1.LabelSelector{
				MatchLabels: newLabels(cr),
			}
		}

		if err := controllerutil.SetControllerReference(cr, provisionerdep, r.scheme); err != nil {
			return err
		}

		var volumes []v1.Volume
		var volumeMounts []v1.VolumeMount
		for _, export := range cr.Spec.Exports {
			shareName := export.Name
			claimName := export.PersistentVolumeClaim.ClaimName
			volumes = append(volumes, v1.Volume{
				Name: shareName,
				VolumeSource: v1.VolumeSource{
					NFS: &v1.NFSVolumeSource{
						Server: serversvc.Spec.ClusterIP,
						Path:   "/" + claimName,
					},
				},
			})

			volumeMounts = append(volumeMounts, v1.VolumeMount{
				Name:      shareName,
				MountPath: filepath.Join("/persistentvolumes", claimName),
			})
		}

		provisionerdep.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
		provisionerdep.Spec.Template.Spec.Volumes = volumes

		return nil
	})

	return err
}

func newLabels(cr *nfsv1alpha1.NFSServer) map[string]string {
	return map[string]string{
		k8sutil.AppAttr: cr.Name,
	}
}

func newDeploymentForProvisioner(cr *nfsv1alpha1.NFSServer) *appsv1.Deployment {
	name := cr.Name + "-provisioner"
	provisionerName := "nfs.rook.io/" + name
	replicas := int32(1)
	defaultTerminationGracePeriodSeconds := int64(30)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
			Labels:    newLabels(cr),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: newLabels(cr),
				},
				Spec: v1.PodSpec{
					ServiceAccountName: "rook-nfs-provisioner",
					Containers: []corev1.Container{
						{
							Name:                     "rook-nfs-provisioner",
							Image:                    "docker.io/ahmadnurus/rook-nfs-provisioner",
							ImagePullPolicy:          corev1.PullIfNotPresent,
							Ports:                    []corev1.ContainerPort{},
							TerminationMessagePath:   "/dev/termination-log",
							TerminationMessagePolicy: corev1.TerminationMessageReadFile,
							Env: []corev1.EnvVar{
								{
									Name:  "PROVISIONER_NAME",
									Value: provisionerName,
								},
							},
						},
					},
					RestartPolicy:                 corev1.RestartPolicyAlways,
					TerminationGracePeriodSeconds: &defaultTerminationGracePeriodSeconds,
					DNSPolicy:                     corev1.DNSClusterFirst,
					SecurityContext:               &corev1.PodSecurityContext{},
					SchedulerName:                 corev1.DefaultSchedulerName,
				},
			},
		},
	}
}

func newVolumeMountForProvisioner(cr *nfsv1alpha1.NFSServer) []v1.VolumeMount {
	var volumeMounts []v1.VolumeMount
	volumeMounts = append(volumeMounts, v1.VolumeMount{
		Name: "nfs-root",
	})
	return volumeMounts
}

func newConfigMapForCR(cr *nfsv1alpha1.NFSServer) *v1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    newLabels(cr),
		},
	}
}

func newServiceForCR(cr *nfsv1alpha1.NFSServer) *v1.Service {
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

func newStatefulSetForCR(cr *nfsv1alpha1.NFSServer) *appsv1.StatefulSet {
	replicas := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    newLabels(cr),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: newLabels(cr),
			},
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
							VolumeMounts: newVolumeMounts(cr),
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
					Volumes: newVolumes(cr),
				},
			},
		},
	}
}

func newVolumes(cr *nfsv1alpha1.NFSServer) []v1.Volume {
	var volumes []v1.Volume
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
	}

	volumes = append(volumes, v1.Volume{
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
	})

	return volumes
}

func newVolumeMounts(cr *nfsv1alpha1.NFSServer) []v1.VolumeMount {
	var volumeMounts []v1.VolumeMount
	for _, export := range cr.Spec.Exports {
		shareName := export.Name
		claimName := export.PersistentVolumeClaim.ClaimName
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      shareName,
			MountPath: "/" + claimName,
		})
	}

	volumeMounts = append(volumeMounts, v1.VolumeMount{
		Name:      cr.Name,
		MountPath: nfsConfigMapPath,
	})

	return volumeMounts
}
