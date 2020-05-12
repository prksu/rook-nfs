package v1alpha1

import (
	rookv1 "github.com/rook/rook/pkg/apis/rook.io/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AllowedClientsSpec represents the client specs for accessing the NFS export
type AllowedClientsSpec struct {
	// Name of the clients group
	Name string `json:"name,omitempty"`

	// The clients that can access the share
	// Values can be hostname, ip address, netgroup, CIDR network address, or all
	Clients []string `json:"clients,omitempty"`

	// Reading and Writing permissions for the client to access the NFS export
	// Valid values are "ReadOnly", "ReadWrite" and "none"
	// Gets overridden when ServerSpec.accessMode is specified
	AccessMode string `json:"accessMode,omitempty"`

	// Squash options for clients
	// Valid values are "none", "rootid", "root", and "all"
	// Gets overridden when ServerSpec.squash is specified
	Squash string `json:"squash,omitempty"`
}

// ServerSpec represents the spec for configuring the NFS server
type ServerSpec struct {
	// Reading and Writing permissions on the export
	// Valid values are "ReadOnly", "ReadWrite" and "none"
	AccessMode string `json:"accessMode,omitempty"`

	// This prevents the root users connected remotely from having root privileges
	// Valid values are "none", "rootid", "root", and "all"
	Squash string `json:"squash,omitempty"`

	// The clients allowed to access the NFS export
	AllowedClients []AllowedClientsSpec `json:"allowedClients,omitempty"`
}

// ExportsSpec represents the spec of NFS exports
type ExportsSpec struct {
	// Name of the export
	Name string `json:"name,omitempty"`

	// The NFS server configuration
	Server ServerSpec `json:"server,omitempty"`

	// PVC from which the NFS daemon gets storage for sharing
	PersistentVolumeClaim v1.PersistentVolumeClaimVolumeSource `json:"persistentVolumeClaim,omitempty"`
}

// NFSServerSpec defines the desired state of NFSServer
type NFSServerSpec struct {
	// The annotations-related configuration to add/set on each Pod related object.
	Annotations rookv1.Annotations `json:"annotations,omitempty"`

	// Replicas of the NFS daemon
	Replicas int `json:"replicas,omitempty"`

	// The parameters to configure the NFS export
	Exports []ExportsSpec `json:"exports,omitempty"`
}

// NFSServerStatus defines the observed state of NFSServer
type NFSServerStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book-v1.book.kubebuilder.io/beyond_basics/generating_crd.html
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NFSServer is the Schema for the nfsservers API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=nfsservers,scope=Namespaced
type NFSServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NFSServerSpec   `json:"spec,omitempty"`
	Status NFSServerStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NFSServerList contains a list of NFSServer
type NFSServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NFSServer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NFSServer{}, &NFSServerList{})
}
