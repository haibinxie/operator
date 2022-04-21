package migration

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/libopenstorage/operator/drivers/storage/portworx/component"
	"github.com/libopenstorage/operator/drivers/storage/portworx/manifest"
	"github.com/libopenstorage/operator/drivers/storage/portworx/mock"
	pxutil "github.com/libopenstorage/operator/drivers/storage/portworx/util"
	"github.com/libopenstorage/operator/pkg/controller/storagecluster"
	testutil "github.com/libopenstorage/operator/pkg/util/test"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/client-go/tools/record"
)

func TestDryRun(t *testing.T) {
	clusterName := "px-cluster"
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "portworx",
			Namespace:       "portworx",
			UID:             types.UID("1001"),
			ResourceVersion: "100",
			SelfLink:        "portworx/portworx",
			Finalizers:      []string{"finalizer"},
			OwnerReferences: []metav1.OwnerReference{{Name: "owner"}},
			ClusterName:     "cluster-name",
			ManagedFields:   []metav1.ManagedFieldsEntry{{Manager: "manager"}},
		},
		Spec: appsv1.DaemonSetSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "test",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name:  "portworx",
							Image: "pximage",
							Args: []string{
								"-c", clusterName,
							},
						},
						{
							Name:  pxutil.TelemetryContainerName,
							Image: "telemetryImage",
						},
					},
				},
			},
		},
	}
	portworxService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxServiceName,
			Namespace: ds.Namespace,
		},
		Spec: v1.ServiceSpec{
			ClusterIP:  "1.2.3.4",
			ClusterIPs: []string{"1.2.3.4"},
		},
	}
	storkDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      storkDeploymentName,
			Namespace: ds.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Image: "storkImage",
						},
					},
				},
			},
		},
	}
	autopilotDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      component.AutopilotDeploymentName,
			Namespace: ds.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Image: "autopilotImage",
						},
					},
				},
			},
		},
	}
	csiService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csiServiceName,
			Namespace: ds.Namespace,
		},
		Spec: v1.ServiceSpec{
			ClusterIP: v1.ClusterIPNone,
		},
	}

	k8sClient := testutil.FakeK8sClient(
		ds, portworxService, storkDeployment, autopilotDeployment, csiService,
	)

	mockController := gomock.NewController(t)
	driver := testutil.MockDriver(mockController)
	ctrl := &storagecluster.Controller{
		Driver: driver,
	}
	recorder := record.NewFakeRecorder(10)
	ctrl.SetEventRecorder(recorder)
	ctrl.SetKubernetesClient(k8sClient)
	mockManifest := mock.NewMockManifest(mockController)
	manifest.SetInstance(mockManifest)
	mockManifest.EXPECT().CanAccessRemoteManifest(gomock.Any()).Return(true).AnyTimes()

	driver.EXPECT().GetStoragePodSpec(gomock.Any(), gomock.Any()).Return(ds.Spec.Template.Spec, nil).AnyTimes()
	driver.EXPECT().GetSelectorLabels().Return(nil).AnyTimes()
	driver.EXPECT().SetDefaultsOnStorageCluster(gomock.Any()).AnyTimes()
	driver.EXPECT().String().Return("mock-driver").AnyTimes()

	migrator := New(ctrl)
	go migrator.Start()

	err := wait.PollImmediate(time.Millisecond*200, time.Second*5, func() (bool, error) {
		if strings.Contains(reflect.ValueOf(<-recorder.Events).String(), "DryRun completed successfully") {
			return true, nil
		}

		return false, nil
	})
	require.NoError(t, err)
}
