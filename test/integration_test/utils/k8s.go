package utils

import (
	"context"
	"fmt"
	"io/ioutil"
	"path"
	"reflect"
	"time"

	apiextensionsops "github.com/portworx/sched-ops/k8s/apiextensions"
	appops "github.com/portworx/sched-ops/k8s/apps"
	coreops "github.com/portworx/sched-ops/k8s/core"
	prometheusops "github.com/portworx/sched-ops/k8s/prometheus"
	rbacops "github.com/portworx/sched-ops/k8s/rbac"
	storageops "github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/task"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	oputil "github.com/libopenstorage/operator/pkg/util"
	k8sutil "github.com/libopenstorage/operator/pkg/util/k8s"
)

// ParseSpecs parses the file under testspec folder and returns all the valid k8s objects
func ParseSpecs(filename string) ([]runtime.Object, error) {
	return ParseSpecsWithFullPath(path.Join("testspec", filename))
}

// ParseSpecsWithFullPath parses the file and returns all the valid k8s objects
func ParseSpecsWithFullPath(filename string) ([]runtime.Object, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return oputil.ParseSpecsFromString(string(b))
}

// CreateObjects creates the given k8s objects
func CreateObjects(objects []runtime.Object) error {
	for _, obj := range objects {
		var err error
		if ns, ok := obj.(*v1.Namespace); ok {
			_, err = coreops.Instance().CreateNamespace(ns)
		} else if ds, ok := obj.(*appsv1.DaemonSet); ok {
			_, err = appops.Instance().CreateDaemonSet(ds, metav1.CreateOptions{})
		} else if dep, ok := obj.(*appsv1.Deployment); ok {
			_, err = appops.Instance().CreateDeployment(dep, metav1.CreateOptions{})
		} else if svc, ok := obj.(*v1.Service); ok {
			_, err = coreops.Instance().CreateService(svc)
		} else if cm, ok := obj.(*v1.ConfigMap); ok {
			_, err = coreops.Instance().CreateConfigMap(cm)
		} else if secret, ok := obj.(*v1.Secret); ok {
			_, err = coreops.Instance().CreateSecret(secret)
		} else if sa, ok := obj.(*v1.ServiceAccount); ok {
			_, err = coreops.Instance().CreateServiceAccount(sa)
		} else if role, ok := obj.(*rbacv1.Role); ok {
			_, err = rbacops.Instance().CreateRole(role)
		} else if rb, ok := obj.(*rbacv1.RoleBinding); ok {
			_, err = rbacops.Instance().CreateRoleBinding(rb)
		} else if clusterRole, ok := obj.(*rbacv1.ClusterRole); ok {
			_, err = rbacops.Instance().CreateClusterRole(clusterRole)
		} else if crb, ok := obj.(*rbacv1.ClusterRoleBinding); ok {
			_, err = rbacops.Instance().CreateClusterRoleBinding(crb)
		} else if crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition); ok {
			err = apiextensionsops.Instance().RegisterCRD(crd)
			if errors.IsAlreadyExists(err) {
				err = nil
			}
		} else if sc, ok := obj.(*storagev1.StorageClass); ok {
			_, err = storageops.Instance().CreateStorageClass(sc)
		} else if sm, ok := obj.(*monitoringv1.ServiceMonitor); ok {
			err = apiextensionsops.Instance().ValidateCRD("servicemonitors.monitoring.coreos.com", 1*time.Minute, 1*time.Second)
			if err != nil {
				return err
			}
			_, err = prometheusops.Instance().CreateServiceMonitor(sm)
		} else if pr, ok := obj.(*monitoringv1.PrometheusRule); ok {
			err = apiextensionsops.Instance().ValidateCRD("prometheusrules.monitoring.coreos.com", 1*time.Minute, 1*time.Second)
			if err != nil {
				return err
			}
			_, err = prometheusops.Instance().CreatePrometheusRule(pr)
		} else if prom, ok := obj.(*monitoringv1.Prometheus); ok {
			err = apiextensionsops.Instance().ValidateCRD("prometheuses.monitoring.coreos.com", 1*time.Minute, 1*time.Second)
			if err != nil {
				return err
			}
			_, err = prometheusops.Instance().CreatePrometheus(prom)
		} else if am, ok := obj.(*monitoringv1.Alertmanager); ok {
			err = apiextensionsops.Instance().ValidateCRD("alertmanagers.monitoring.coreos.com", 1*time.Minute, 1*time.Second)
			if err != nil {
				return err
			}
			_, err = prometheusops.Instance().CreateAlertManager(am)
		} else {
			err = fmt.Errorf("unsupported object: %v", reflect.TypeOf(obj))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteObjects deletes the given k8s objects if present
func DeleteObjects(objects []runtime.Object) error {
	for _, obj := range objects {
		var err error
		if ns, ok := obj.(*v1.Namespace); ok {
			err = coreops.Instance().DeleteNamespace(ns.Name)
		} else if ds, ok := obj.(*appsv1.DaemonSet); ok {
			err = appops.Instance().DeleteDaemonSet(ds.Name, ds.Namespace)
		} else if dep, ok := obj.(*appsv1.Deployment); ok {
			err = appops.Instance().DeleteDeployment(dep.Name, dep.Namespace)
		} else if svc, ok := obj.(*v1.Service); ok {
			err = coreops.Instance().DeleteService(svc.Name, svc.Namespace)
		} else if cm, ok := obj.(*v1.ConfigMap); ok {
			err = coreops.Instance().DeleteConfigMap(cm.Name, cm.Namespace)
		} else if secret, ok := obj.(*v1.Secret); ok {
			err = coreops.Instance().DeleteSecret(secret.Name, secret.Namespace)
		} else if sa, ok := obj.(*v1.ServiceAccount); ok {
			err = coreops.Instance().DeleteServiceAccount(sa.Name, sa.Namespace)
		} else if role, ok := obj.(*rbacv1.Role); ok {
			err = rbacops.Instance().DeleteRole(role.Name, role.Namespace)
		} else if rb, ok := obj.(*rbacv1.RoleBinding); ok {
			err = rbacops.Instance().DeleteRoleBinding(rb.Name, rb.Namespace)
		} else if clusterRole, ok := obj.(*rbacv1.ClusterRole); ok {
			err = rbacops.Instance().DeleteClusterRole(clusterRole.Name)
		} else if crb, ok := obj.(*rbacv1.ClusterRoleBinding); ok {
			err = rbacops.Instance().DeleteClusterRoleBinding(crb.Name)
		} else if crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition); ok {
			crdName := fmt.Sprintf("%s.%s", crd.Spec.Names.Plural, crd.Spec.Group)
			err = apiextensionsops.Instance().DeleteCRD(crdName)
		} else if sc, ok := obj.(*storagev1.StorageClass); ok {
			err = storageops.Instance().DeleteStorageClass(sc.Name)
		} else if sm, ok := obj.(*monitoringv1.ServiceMonitor); ok {
			err = prometheusops.Instance().DeleteServiceMonitor(sm.Name, sm.Namespace)
		} else if pr, ok := obj.(*monitoringv1.PrometheusRule); ok {
			err = prometheusops.Instance().DeletePrometheusRule(pr.Name, pr.Namespace)
		} else if prom, ok := obj.(*monitoringv1.Prometheus); ok {
			err = prometheusops.Instance().DeletePrometheus(prom.Name, prom.Namespace)
		} else if am, ok := obj.(*monitoringv1.Alertmanager); ok {
			err = prometheusops.Instance().DeleteAlertManager(am.Name, am.Namespace)
		} else {
			err = fmt.Errorf("unsupported object: %v", reflect.TypeOf(obj))
		}
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// ValidateObjectsAreTerminated validates that the objects have been terminated.
// The skip flag will not validate termination of certain objects.
func ValidateObjectsAreTerminated(objects []runtime.Object, skip bool) error {
	s := scheme.Scheme
	apiextensionsv1.AddToScheme(s)
	monitoringv1.AddToScheme(s)
	k8sClient, err := k8sutil.NewK8sClient(s)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		var err error
		if ns, ok := obj.(*v1.Namespace); ok {
			err = validateObjectIsTerminated(k8sClient, ns.Name, "", ns, skip)
		} else if ds, ok := obj.(*appsv1.DaemonSet); ok {
			err = validateObjectIsTerminated(k8sClient, ds.Name, ds.Namespace, ds, false)
		} else if dep, ok := obj.(*appsv1.Deployment); ok {
			err = validateObjectIsTerminated(k8sClient, dep.Name, dep.Namespace, dep, false)
		} else if svc, ok := obj.(*v1.Service); ok {
			err = validateObjectIsTerminated(k8sClient, svc.Name, svc.Namespace, svc, false)
		} else if cm, ok := obj.(*v1.ConfigMap); ok {
			err = validateObjectIsTerminated(k8sClient, cm.Name, cm.Namespace, cm, false)
		} else if secret, ok := obj.(*v1.Secret); ok {
			err = validateObjectIsTerminated(k8sClient, secret.Name, secret.Namespace, secret, false)
		} else if sa, ok := obj.(*v1.ServiceAccount); ok {
			err = validateObjectIsTerminated(k8sClient, sa.Name, sa.Namespace, sa, false)
		} else if role, ok := obj.(*rbacv1.Role); ok {
			err = validateObjectIsTerminated(k8sClient, role.Name, role.Namespace, role, false)
		} else if rb, ok := obj.(*rbacv1.RoleBinding); ok {
			err = validateObjectIsTerminated(k8sClient, rb.Name, rb.Namespace, rb, false)
		} else if clusterRole, ok := obj.(*rbacv1.ClusterRole); ok {
			err = validateObjectIsTerminated(k8sClient, clusterRole.Name, "", clusterRole, false)
		} else if crb, ok := obj.(*rbacv1.ClusterRoleBinding); ok {
			err = validateObjectIsTerminated(k8sClient, crb.Name, "", crb, false)
		} else if crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition); ok {
			crdName := fmt.Sprintf("%s.%s", crd.Spec.Names.Plural, crd.Spec.Group)
			err = validateObjectIsTerminated(k8sClient, crdName, "", crd, skip)
		} else if sc, ok := obj.(*storagev1.StorageClass); ok {
			err = validateObjectIsTerminated(k8sClient, sc.Name, "", sc, skip)
		} else if sm, ok := obj.(*monitoringv1.ServiceMonitor); ok {
			err = validateObjectIsTerminated(k8sClient, sm.Name, sm.Namespace, sm, false)
		} else if pr, ok := obj.(*monitoringv1.PrometheusRule); ok {
			err = validateObjectIsTerminated(k8sClient, pr.Name, pr.Namespace, pr, false)
		} else if prom, ok := obj.(*monitoringv1.Prometheus); ok {
			err = validateObjectIsTerminated(k8sClient, prom.Name, prom.Namespace, prom, false)
		} else if am, ok := obj.(*monitoringv1.Alertmanager); ok {
			err = validateObjectIsTerminated(k8sClient, am.Name, am.Namespace, am, false)
		} else {
			err = fmt.Errorf("unsupported object: %v", reflect.TypeOf(obj))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func validateObjectIsTerminated(
	k8sClient client.Client,
	name, namespace string,
	obj client.Object,
	skip bool,
) error {
	if skip {
		return nil
	}

	kind := obj.GetObjectKind().GroupVersionKind().Kind
	t := func() (interface{}, bool, error) {
		objName := types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		}
		err := k8sClient.Get(context.TODO(), objName, obj)
		if errors.IsNotFound(err) {
			return nil, false, nil
		} else if err != nil {
			return nil, true, err
		}
		return nil, true, fmt.Errorf("%s %s is still present", kind, objName.String())
	}

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 2*time.Second); err != nil {
		return err
	}
	return nil
}
