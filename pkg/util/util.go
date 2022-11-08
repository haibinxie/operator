package util

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/go-version"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	apiyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/constants"
)

// Reasons for controller events
const (
	// FailedPlacementReason is added to an event when operator can't schedule a Pod to a specified node.
	FailedPlacementReason = "FailedPlacement"
	// FailedStoragePodReason is added to an event when the status of a Pod of a cluster is 'Failed'.
	FailedStoragePodReason = "FailedStoragePod"
	// FailedSyncReason is added to an event when the status of the cluster could not be synced.
	FailedSyncReason = "FailedSync"
	// FailedValidationReason is added to an event when operator validations fail.
	FailedValidationReason = "FailedValidation"
	// FailedComponentReason is added to an event when setting up or removing a component fails.
	FailedComponentReason = "FailedComponent"
	// UpdatePausedReason is added to an event when operator pauses update of the storage cluster.
	UpdatePausedReason = "UpdatePaused"
	// ClusterOnlineReason is added to an event when a cluster comes online
	ClusterOnlineReason = "ClusterOnline"
	// MigrationPendingReason is added to an event when the migration is in pending state.
	MigrationPendingReason = "MigrationPending"
	// MigrationCompletedReason is added to an event when the migration is completed.
	MigrationCompletedReason = "MigrationCompleted"
	// MigrationFailed is added to an event when the migration fails.
	MigrationFailedReason = "MigrationFailed"

	// MigrationDryRunCompletedReason is added to an event when dry run is completed
	MigrationDryRunCompletedReason = "MigrationDryRunCompleted"
	// MigrationDryRunFailedReason is added to an event when dry run fails.
	MigrationDryRunFailedReason = "MigrationDryRunFailed"

	// DefaultImageRegistry is the default registry when no registry is provided
	DefaultImageRegistry = "docker.io"

	// StorkSchedulerName is the default scheduler for px-csi-ext pods
	StorkSchedulerName = "stork"
)

var (
	// commonDockerRegistries is a map of commonly used Docker registries
	commonDockerRegistries = map[string]bool{
		"docker.io":                   true,
		"quay.io":                     true,
		"index.docker.io":             true,
		"registry-1.docker.io":        true,
		"registry.connect.redhat.com": true,
		"registry.k8s.io":             true,
	}

	// podTopologySpreadConstraintKeys is a list of topology keys considered for pod spread constraints
	podTopologySpreadConstraintKeys = []string{
		"topology.kubernetes.io/region",
		"topology.kubernetes.io/zone",
	}
)

// AddDefaultRegistryToImage adds default registry to image.
func AddDefaultRegistryToImage(image string) string {
	if image == "" {
		return ""
	}

	for k := range commonDockerRegistries {
		if strings.HasPrefix(image, k) || strings.HasPrefix(image, "gcr.io") || strings.HasPrefix(image, "k8s.gcr.io") {
			return image
		}
	}

	return DefaultImageRegistry + "/" + image
}

func getMergedCommonRegistries(cluster *corev1.StorageCluster) map[string]bool {
	val, ok := cluster.Annotations[constants.AnnotationCommonImageRegistries]

	if !ok {
		return commonDockerRegistries
	}

	mergedCommonRegistries := make(map[string]bool)

	for _, v := range strings.Split(strings.TrimSpace(val), ",") {
		mergedCommonRegistries[v] = true
	}

	for k, v := range commonDockerRegistries {
		mergedCommonRegistries[k] = v
	}

	return mergedCommonRegistries
}

// GetImageURN returns the complete image name based on the registry and repo
func GetImageURN(cluster *corev1.StorageCluster, image string) string {
	if image == "" {
		return ""
	}

	registryAndRepo := cluster.Spec.CustomImageRegistry
	mergedCommonRegistries := getMergedCommonRegistries(cluster)
	preserveFullCustomImageRegistry := cluster.Spec.PreserveFullCustomImageRegistry

	omitRepo := false
	if strings.HasSuffix(registryAndRepo, "//") {
		omitRepo = true
	}

	registryAndRepo = strings.TrimRight(registryAndRepo, "/")
	if registryAndRepo == "" {
		// no registry/repository specifed, return image
		return AddDefaultRegistryToImage(image)
	}

	imgParts := strings.Split(image, "/")
	if len(imgParts) > 1 {
		// advance imgParts to swallow the common registry
		if _, present := mergedCommonRegistries[imgParts[0]]; present {
			imgParts = imgParts[1:]
		}
	}

	if !preserveFullCustomImageRegistry {
		// if we have '/' in the registryAndRepo, return <registry/repository/><only-image>
		// else (registry only) -- return <registry/><image-with-repository>
		if strings.Contains(registryAndRepo, "/") || omitRepo {
			// advance to the last element, skipping image's repository
			imgParts = imgParts[len(imgParts)-1:]
		}
	}

	return registryAndRepo + "/" + path.Join(imgParts...)
}

// GetImageMajorVersion returns the major version for a given image.
// This allows you to make decisions based on the major version.
func GetImageMajorVersion(image string) int {
	if !strings.Contains(image, ":") {
		return -1
	}

	parts := strings.Split(image, ":")
	tag := parts[len(parts)-1]
	if tag == "" {
		return -1
	}

	ver, err := version.NewVersion(tag)
	if err != nil {
		return -1
	}

	return ver.Segments()[0]
}

// HasPullSecretChanged checks if the imagePullSecret in the cluster is the only one
// in the given list of pull secrets
func HasPullSecretChanged(
	cluster *corev1.StorageCluster,
	existingPullSecrets []v1.LocalObjectReference,
) bool {
	return len(existingPullSecrets) > 1 ||
		(len(existingPullSecrets) == 1 &&
			cluster.Spec.ImagePullSecret != nil && existingPullSecrets[0].Name != *cluster.Spec.ImagePullSecret) ||
		(len(existingPullSecrets) == 0 &&
			cluster.Spec.ImagePullSecret != nil && *cluster.Spec.ImagePullSecret != "")
}

// HaveTolerationsChanged checks if the tolerations in the cluster are same as the
// given list of tolerations
func HaveTolerationsChanged(
	cluster *corev1.StorageCluster,
	existingTolerations []v1.Toleration,
) bool {
	if cluster.Spec.Placement == nil {
		return len(existingTolerations) != 0
	}
	return !reflect.DeepEqual(cluster.Spec.Placement.Tolerations, existingTolerations)
}

// DeepEqualObject compare two objects
func DeepEqualObject(obj1, obj2 interface{}) error {
	if !reflect.DeepEqual(obj1, obj2) {
		return fmt.Errorf("two objects are different, first object %+v, second object %+v", obj1, obj2)
	}
	return nil
}

// DeepEqualObjects compares two arrays of objects
func DeepEqualObjects(
	objs1, objs2 []interface{},
	funcGetKey func(obj interface{}) string,
	funcDeepEqualObject func(obj1, obj2 interface{}) error) error {

	map1 := make(map[string]interface{})
	map2 := make(map[string]interface{})
	for _, obj := range objs1 {
		map1[funcGetKey(obj)] = obj
	}
	for _, obj := range objs2 {
		map2[funcGetKey(obj)] = obj
	}

	var msg string
	for k, v := range map1 {
		v2, ok := map2[k]

		if !ok {
			msg += fmt.Sprintf("object \"%s\" exists in first array but does not exist in second array.\n", k)
		} else if err := funcDeepEqualObject(v, v2); err != nil {
			msg += err.Error()
			msg += "\n"
		}
	}

	for k := range map2 {
		if _, ok := map1[k]; !ok {
			msg += fmt.Sprintf("object \"%s\" exists in second array but does not exist in first array.\n", k)
		}
	}

	if msg != "" {
		return fmt.Errorf(msg)
	}
	return nil
}

// DeepEqualPodTemplate compares if two pod template specs are same.
func DeepEqualPodTemplate(t1, t2 *v1.PodTemplateSpec) (bool, error) {
	// DeepDerivative will return true if first argument is nil, hence check the length of volumes.
	// The reason we don't2 use deepEqual for volumes is k8s API server may add defaultMode to it.
	if !equality.Semantic.DeepDerivative(t1.Spec.Containers, t2.Spec.Containers) {
		return false, fmt.Errorf("containers not equal, first: %+v, second: %+v", t1.Spec.Containers, t2.Spec.Containers)
	}

	if !(len(t1.Spec.Volumes) == len(t2.Spec.Volumes) &&
		equality.Semantic.DeepDerivative(t1.Spec.Volumes, t2.Spec.Volumes)) {
		return false, fmt.Errorf("volumes not equal, first: %+v, second: %+v", t1.Spec.Volumes, t2.Spec.Volumes)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.ImagePullSecrets, t2.Spec.ImagePullSecrets) {
		return false, fmt.Errorf("image pull secrets not equal, first: %+v, second: %+v", t1.Spec.ImagePullSecrets, t2.Spec.ImagePullSecrets)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.Affinity, t2.Spec.Affinity) {
		return false, fmt.Errorf("affinity not equal, first: %+v, second: %+v", t1.Spec.Affinity, t2.Spec.Affinity)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.Tolerations, t2.Spec.Tolerations) {
		return false, fmt.Errorf("tolerations not equal, first: %+v, second: %+v", t1.Spec.Tolerations, t2.Spec.Tolerations)
	}

	if !equality.Semantic.DeepEqual(t1.Spec.ServiceAccountName, t2.Spec.ServiceAccountName) {
		return false, fmt.Errorf("service account name not equal, first: %s, second: %s", t1.Spec.ServiceAccountName, t2.Spec.ServiceAccountName)
	}

	return true, nil
}

// HasNodeAffinityChanged checks if the nodeAffinity in the cluster is same as the
// node affinity in the given affinity
func HasNodeAffinityChanged(
	cluster *corev1.StorageCluster,
	existingAffinity *v1.Affinity,
) bool {
	if cluster.Spec.Placement == nil {
		return existingAffinity != nil && existingAffinity.NodeAffinity != nil
	} else if existingAffinity == nil {
		return cluster.Spec.Placement.NodeAffinity != nil
	}
	return !reflect.DeepEqual(cluster.Spec.Placement.NodeAffinity, existingAffinity.NodeAffinity)
}

// HasSchedulerStateChanged checks if the stork has been enabled/disabled in the StorageCluster
func HasSchedulerStateChanged(
	cluster *corev1.StorageCluster,
	previousSchedulerName string,
) bool {
	if cluster.Spec.Stork == nil {
		return v1.DefaultSchedulerName != previousSchedulerName
	}
	currentSchedulerName := v1.DefaultSchedulerName
	if cluster.Spec.Stork.Enabled {
		currentSchedulerName = StorkSchedulerName
	}
	return currentSchedulerName != previousSchedulerName
}

// ExtractVolumesAndMounts returns a list of Kubernetes volumes and volume mounts from the
// given StorageCluster volume specs
func ExtractVolumesAndMounts(volumeSpecs []corev1.VolumeSpec) ([]v1.Volume, []v1.VolumeMount) {
	volumes := make([]v1.Volume, 0)
	volumeMounts := make([]v1.VolumeMount, 0)

	// Set volume defaults. Makes it easier to compare with
	// actual deployment volumes to see if they have changed.
	for i := range volumeSpecs {
		if volumeSpecs[i].ConfigMap != nil {
			defaultMode := v1.ConfigMapVolumeSourceDefaultMode
			volumeSpecs[i].ConfigMap.DefaultMode = &defaultMode
		} else if volumeSpecs[i].Secret != nil {
			defaultMode := v1.SecretVolumeSourceDefaultMode
			volumeSpecs[i].Secret.DefaultMode = &defaultMode
		} else if volumeSpecs[i].Projected != nil {
			defaultMode := v1.ProjectedVolumeSourceDefaultMode
			volumeSpecs[i].Projected.DefaultMode = &defaultMode
		} else if volumeSpecs[i].HostPath != nil {
			hostPathType := v1.HostPathUnset
			volumeSpecs[i].HostPath.Type = &hostPathType
		}
	}

	for _, volumeSpec := range volumeSpecs {
		volumes = append(volumes, v1.Volume{
			Name:         volumeSpec.Name,
			VolumeSource: volumeSpec.VolumeSource,
		})
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:             volumeSpec.Name,
			MountPath:        volumeSpec.MountPath,
			MountPropagation: volumeSpec.MountPropagation,
			ReadOnly:         volumeSpec.ReadOnly,
		})
	}

	return volumes, volumeMounts
}

// IsPartialSecretRef is a helper method that checks if a SecretRef is partially specified (i.e. only one of the needed cert name and key specified)
func IsPartialSecretRef(sref *corev1.SecretRef) bool {
	if sref == nil {
		return false
	}
	x := len(sref.SecretName) > 0
	y := len(sref.SecretKey) > 0
	// X xor Y -> (X || Y) && !(X && Y)
	return (x || y) && !(x && y)
}

// GetCustomAnnotations returns custom annotations for different StorageCluster components from spec
func GetCustomAnnotations(
	cluster *corev1.StorageCluster,
	k8sObjKind string,
	componentName string,
) map[string]string {
	if cluster.Spec.Metadata == nil || cluster.Spec.Metadata.Annotations == nil {
		return nil
	}
	// Use kind/component to locate the custom annotation, e.g. deployment/stork
	key := fmt.Sprintf("%s/%s", k8sObjKind, componentName)
	if annotations, ok := cluster.Spec.Metadata.Annotations[key]; ok && len(annotations) != 0 {
		return annotations
	}
	return nil
}

// GetCustomLabels returns custom labels for different StorageCluster components from spec
func GetCustomLabels(
	cluster *corev1.StorageCluster,
	k8sObjKind string,
	componentName string,
) map[string]string {
	if cluster.Spec.Metadata == nil || cluster.Spec.Metadata.Labels == nil {
		return nil
	}
	// Use kind/component to locate the custom labels, e.g. service/portworx-api
	key := fmt.Sprintf("%s/%s", k8sObjKind, componentName)
	if labels, ok := cluster.Spec.Metadata.Labels[key]; ok && len(labels) != 0 {
		return labels
	}
	return nil
}

// ComponentsPausedForMigration returns true if the daemonset migration is going on and
// the components are waiting for storage pods to migrate first
func ComponentsPausedForMigration(cluster *corev1.StorageCluster) bool {
	_, migrating := cluster.Annotations[constants.AnnotationMigrationApproved]
	componentsPaused, err := strconv.ParseBool(cluster.Annotations[constants.AnnotationPauseComponentMigration])
	return migrating && err == nil && componentsPaused
}

// HaveTopologySpreadConstraintsChanged checks if the deployment has pod topology spread constraints changed
func HaveTopologySpreadConstraintsChanged(
	updatedTopologySpreadConstraints []v1.TopologySpreadConstraint,
	existingTopologySpreadConstraints []v1.TopologySpreadConstraint,
) bool {
	return !reflect.DeepEqual(updatedTopologySpreadConstraints, existingTopologySpreadConstraints)
}

// GetTopologySpreadConstraints returns pod topology spread constraints spec
func GetTopologySpreadConstraints(
	k8sClient client.Client,
	labels map[string]string,
) ([]v1.TopologySpreadConstraint, error) {
	nodeList := &v1.NodeList{}
	err := k8sClient.List(context.TODO(), nodeList)
	if err != nil {
		return nil, err
	}
	return GetTopologySpreadConstraintsFromNodes(nodeList, labels)
}

// GetTopologySpreadConstraintsFromNodes returns pod topology spread constraints spec
func GetTopologySpreadConstraintsFromNodes(
	nodeList *v1.NodeList,
	labels map[string]string,
) ([]v1.TopologySpreadConstraint, error) {
	topologyKeySet := make(map[string]bool)
	for _, key := range podTopologySpreadConstraintKeys {
		for _, node := range nodeList.Items {
			if _, ok := node.Labels[key]; ok {
				topologyKeySet[key] = true
			}
		}
	}
	var keys []string
	for k := range topologyKeySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Construct topology spread constraints
	var constraints []v1.TopologySpreadConstraint
	for _, key := range keys {
		constraints = append(constraints, v1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       key,
			WhenUnsatisfiable: v1.ScheduleAnyway,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
		})
	}
	return constraints, nil
}

// ParseSpecsFromString parses objects from a string, objects are separated by "---"
func ParseSpecsFromString(str string) ([]runtime.Object, error) {
	var specs []runtime.Object
	reader := bufio.NewReader(strings.NewReader(str))
	specReader := apiyaml.NewYAMLReader(reader)

	for {
		specContents, err := specReader.Read()
		if err == io.EOF {
			break
		}

		if len(bytes.TrimSpace(specContents)) > 0 {
			obj, err := decodeSpec(specContents)
			if err != nil {
				logrus.Warnf("Error decoding spec: %v", err)
				return nil, err
			}

			specObj, err := validateSpec(obj)
			if err != nil {
				logrus.Warnf("Error parsing spec: %v", err)
				return nil, err
			}

			specs = append(specs, specObj)
		}
	}
	return specs, nil
}

func decodeSpec(specContents []byte) (runtime.Object, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
	if err != nil {
		scheme := runtime.NewScheme()
		if err := apiextensionsv1.AddToScheme(scheme); err != nil {
			return nil, err
		}
		if err := monitoringv1.AddToScheme(scheme); err != nil {
			return nil, err
		}
		if err := corev1.AddToScheme(scheme); err != nil {
			return nil, err
		}
		codecs := serializer.NewCodecFactory(scheme)
		obj, _, err = codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func validateSpec(in interface{}) (runtime.Object, error) {
	if specObj, ok := in.(*v1.Namespace); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Pod); ok {
		return specObj, nil
	} else if specObj, ok := in.(*appsv1.DaemonSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*appsv1.Deployment); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Service); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.ConfigMap); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Secret); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.ServiceAccount); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.Role); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.RoleBinding); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.ClusterRole); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.ClusterRoleBinding); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apiextensionsv1.CustomResourceDefinition); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storagev1.StorageClass); ok {
		return specObj, nil
	} else if specObj, ok := in.(*monitoringv1.ServiceMonitor); ok {
		return specObj, nil
	} else if specObj, ok := in.(*monitoringv1.PrometheusRule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*monitoringv1.Prometheus); ok {
		return specObj, nil
	} else if specObj, ok := in.(*monitoringv1.Alertmanager); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.PersistentVolumeClaim); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.StorageCluster); ok {
		return specObj, nil
	}
	return nil, fmt.Errorf("unsupported object: %v", reflect.TypeOf(in))
}

func objectMatch(genericObj, targetObj client.Object) (bool, error) {
	if genericObj == nil || targetObj == nil {
		return false, fmt.Errorf("nil object")
	}
	if genericObj.GetObjectKind().GroupVersionKind().Kind != targetObj.GetObjectKind().GroupVersionKind().Kind {
		return false, nil
	}

	// Only compare name if it's not empty, otherwise compare labels.
	if genericObj.GetName() != "" {
		ns1 := genericObj.GetNamespace()
		ns2 := targetObj.GetNamespace()
		if ns1 == "" {
			ns1 = "default"
		}
		if ns2 == "" {
			ns2 = "default"
		}

		return genericObj.GetName() == targetObj.GetName() && ns1 == ns2, nil
	}

	genericLabels := genericObj.GetLabels()
	if len(genericLabels) == 0 {
		return false, fmt.Errorf("generic config object should have name or labels: %v", genericObj)
	}

	objLabels := targetObj.GetLabels()
	if objLabels == nil {
		return false, nil
	}

	// If any generic label does not match, return false
	for k, v := range genericLabels {
		v2, ok := objLabels[k]
		if !ok || v != v2 {
			return false, nil
		}
	}

	return true, nil
}

// GetAndApplyGenericConfig gets generic config from configMaps and applies to a given object
func GetAndApplyGenericConfig(k8sClient client.Client, obj *client.Object) error {
	cmList := &v1.ConfigMapList{}
	// list across all namespaces.
	if err := k8sClient.List(context.TODO(), cmList, &client.ListOptions{}); err != nil {
		return err
	}

	// Parse generic config objects
	for _, cm := range cmList.Items {
		if strings.HasPrefix(strings.ToLower(cm.Name), "genericconfig-") {
			for _, val := range cm.Data {
				err := ApplyGenericConfig(val, obj)
				if err != nil {
					logrus.WithError(err).Warningf("failed to apply generic config from configMap %s", cm.GetName())
					return err
				}
			}
		}
	}

	return nil
}

// ApplyGenericConfig applies generic config to a given object
func ApplyGenericConfig(genericConfig string, obj *client.Object) error {
	genericObjs, err := ParseSpecsFromString(genericConfig)
	if err != nil {
		return err
	}

	// For each generic config object, apply to the object if it matches the object name or
	// object label selector
	objKind := (*obj).GetObjectKind().GroupVersionKind().Kind
	for _, tmpObj := range genericObjs {
		genericObj := tmpObj.(client.Object)
		match, err := objectMatch(genericObj, (*obj))
		if err != nil {
			// log error and continue with other objects
			logrus.Errorf("failed to compare object: %v", err)
			continue
		}

		if match {
			logrus.Debugf("update object %s/%s with generic config.", objKind, (*obj).GetName())
			b, err := yaml.Marshal(genericObj)
			if err != nil {
				logrus.WithError(err).Errorf("failed to marshal generic obj")
				return err
			}

			err = yaml.Unmarshal(b, *obj)
			if err != nil {
				logrus.WithError(err).Errorf("failed to unmarshal generic obj")
				return err
			}
		}
	}

	return nil
}
