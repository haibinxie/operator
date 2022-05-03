package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	ocp_configv1 "github.com/openshift/api/config/v1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	coreops "github.com/portworx/sched-ops/k8s/core"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	fakeextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/runtime"
	fakek8sclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	cluster_v1alpha1 "sigs.k8s.io/cluster-api/pkg/apis/deprecated/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	"github.com/libopenstorage/operator/drivers/storage"
	_ "github.com/libopenstorage/operator/drivers/storage/portworx"
	pxutil "github.com/libopenstorage/operator/drivers/storage/portworx/util"
	"github.com/libopenstorage/operator/pkg/apis"
	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	_ "github.com/libopenstorage/operator/pkg/log"
	"github.com/libopenstorage/operator/pkg/mock"
	testutil "github.com/libopenstorage/operator/pkg/util/test"
	"github.com/libopenstorage/operator/pkg/version"
	inttestutil "github.com/libopenstorage/operator/test/integration_test/utils"
)

const (
	flagVerbose       = "verbose"
	flagStorageCluser = "storagecluster,stc"
	flagKubeConfig    = "kubeconfig"
	flagOutputFile    = "output,o"

	defaultOutputFile = "portworxCompnents.yaml"
)

func main() {
	app := cli.NewApp()
	app.Name = "portworx-operator-dryrun"
	app.Usage = "Portworx operator dry run tool"
	app.Version = version.Version
	app.Action = execute

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  flagVerbose,
			Usage: "Enable verbose logging",
		},
		cli.StringFlag{
			Name:  flagStorageCluser,
			Usage: "[Optional] File for storage cluster spec, retrieve from k8s if it's not configured",
		},
		cli.StringFlag{
			Name:  flagKubeConfig,
			Usage: "[Optional] kubeconfig file",
		},
		cli.StringFlag{
			Name:  flagOutputFile,
			Usage: "[Optional] output file to save k8s object, default to " + defaultOutputFile,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error starting: %v", err)
	}
}

func execute(c *cli.Context) {
	verbose := c.Bool(flagVerbose)
	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if err := dryRun(c); err != nil {
		log.WithError(err).Errorf("dryrun failed")
	}
}

func dryRun(c *cli.Context) error {
	cluster, err := getStorageCluster(c)
	if err != nil {
		return err
	}

	apiextensions.SetInstance(apiextensions.New(fakeextclient.NewSimpleClientset()))
	coreops.SetInstance(coreops.New(fakek8sclient.NewSimpleClientset()))

	mockCtrl := gomock.NewController(nil)
	defer mockCtrl.Finish()

	mockNodeServer := mock.NewMockOpenStorageNodeServer(mockCtrl)
	sdkServerIP := "127.0.0.1"
	sdkServerPort := 21883
	mockSdk := mock.NewSdkServer(mock.SdkServers{
		Node: mockNodeServer,
	})
	mockSdk.StartOnAddress(sdkServerIP, strconv.Itoa(sdkServerPort))
	defer mockSdk.Stop()

	dryRunDriver, err := storage.Get(pxutil.DryRunDriverName)
	if err != nil {
		log.Fatalf("Error getting dry-run Storage driver %v", err)
	}

	dryRunClient := testutil.FakeK8sClient()
	if err = dryRunDriver.Init(dryRunClient, runtime.NewScheme(), record.NewFakeRecorder(100)); err != nil {
		log.Fatalf("Error initializing Storage driver for dry run %v", err)
	}

	err = dryRunDriver.PreInstall(cluster)
	log.WithError(err).Info("finished PreInstall")
	if err != nil {
		return err
	}

	objs, err := getAllObjects(dryRunClient, cluster.Namespace)
	if err != nil {
		log.WithError(err).Error("failed to get all objects")
	}

	outputFile := c.String(flagOutputFile)
	if outputFile == "" {
		outputFile = defaultOutputFile
	}

	var sb strings.Builder
	for _, obj := range objs {
		bytes, err := yaml.Marshal(obj)
		if err != nil {
			return err
		}

		sb.WriteString(string(bytes))
		sb.WriteString("---\n")
	}

	f, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.WithError(err).Errorf("failed to open file %s", outputFile)
		return err
	}
	defer f.Close()
	_, err = f.WriteString(sb.String())
	if err != nil {
		log.WithError(err).Errorf("failed to write file %s", outputFile)
		return err
	}

	log.Infof("wrote %d objects to file %s after dry run", len(objs), outputFile)
	return nil
}

func getStorageCluster(c *cli.Context) (*corev1.StorageCluster, error) {
	storageClusterFile := c.String(flagStorageCluser)
	if storageClusterFile != "" {
		objs, err := inttestutil.ParseSpecsWithFullPath(storageClusterFile)
		if err != nil {
			return nil, err
		}
		if len(objs) != 1 {
			return nil, fmt.Errorf("require 1 object in the config file, found objects: %+v", objs)
		}
		cluster, ok := objs[0].(*corev1.StorageCluster)
		if !ok {
			return nil, fmt.Errorf("object is not storagecluster, %+v", objs[0])
		}
		return cluster, nil
	} else {
		k8sClient, err := getK8sClient(c)
		if err != nil {
			return nil, err
		}

		nsList := &v1.NamespaceList{}
		err = k8sClient.List(context.TODO(), nsList)
		if err != nil {
			return nil, err
		}

		for _, namespace := range nsList.Items {
			log.Infof("list StorageCluster in namespace %s", namespace.Name)
			clusterList := &corev1.StorageClusterList{}
			err = k8sClient.List(context.TODO(), clusterList, &client.ListOptions{
				Namespace: namespace.Name,
			})
			if err != nil {
				log.WithError(err).Error("failed to list StorageCluster")
			}
			if len(clusterList.Items) > 0 {
				return &clusterList.Items[0], nil
			}
		}

		return nil, fmt.Errorf("could not find storagecluster object from k8s")
	}
}

func getK8sClient(c *cli.Context) (client.Client, error) {
	var err error
	var config *rest.Config
	kubeconfig := c.String(flagKubeConfig)
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}

	mgr, err := manager.New(config, manager.Options{})
	if err != nil {
		return nil, err
	}

	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Failed to add resources to the scheme: %v", err)
	}
	if err := monitoringv1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Failed to add prometheus resources to the scheme: %v", err)
	}

	if err := cluster_v1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Failed to add cluster API resources to the scheme: %v", err)
	}

	if err := ocp_configv1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Failed to add cluster API resources to the scheme: %v", err)
	}

	if err := corev1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Failed to add corev1 resources to the scheme: %v", err)
	}

	if err := v1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Fatalf("Failed to add v1 resources to the scheme: %v", err)
	}

	start := func() {
		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			log.Fatalf("Manager exited non-zero error: %v", err)
		}
	}
	go start()
	time.Sleep(time.Second)

	return mgr.GetClient(), nil
}

func getAllObjects(k8sClient client.Client, namespace string) ([]client.Object, error) {
	var objs []client.Object

	appendObjectList(k8sClient, namespace, &v1.ServiceList{}, &objs)
	appendObjectList(k8sClient, namespace, &v1.ServiceAccountList{}, &objs)
	appendObjectList(k8sClient, namespace, &v1.SecretList{}, &objs)
	appendObjectList(k8sClient, namespace, &v1.ConfigMapList{}, &objs)
	appendObjectList(k8sClient, namespace, &v1.PodList{}, &objs)
	appendObjectList(k8sClient, namespace, &v1.PersistentVolumeClaimList{}, &objs)

	appendObjectList(k8sClient, namespace, &appsv1.DeploymentList{}, &objs)
	appendObjectList(k8sClient, namespace, &appsv1.StatefulSetList{}, &objs)

	appendObjectList(k8sClient, namespace, &rbacv1.ClusterRoleList{}, &objs)
	appendObjectList(k8sClient, namespace, &rbacv1.ClusterRoleBindingList{}, &objs)
	appendObjectList(k8sClient, namespace, &rbacv1.RoleList{}, &objs)
	appendObjectList(k8sClient, namespace, &rbacv1.RoleBindingList{}, &objs)

	appendObjectList(k8sClient, namespace, &storagev1.StorageClassList{}, &objs)
	appendObjectList(k8sClient, namespace, &apiextensionsv1.CustomResourceDefinitionList{}, &objs)

	appendObjectList(k8sClient, namespace, &monitoringv1.ServiceMonitorList{}, &objs)
	appendObjectList(k8sClient, namespace, &monitoringv1.PrometheusRuleList{}, &objs)
	appendObjectList(k8sClient, namespace, &monitoringv1.PrometheusList{}, &objs)
	appendObjectList(k8sClient, namespace, &monitoringv1.AlertmanagerList{}, &objs)

	return objs, nil
}

func appendObjectList(k8sClient client.Client, namespace string, list client.ObjectList, objs *[]client.Object) error {
	err := k8sClient.List(
		context.TODO(),
		list,
		&client.ListOptions{
			Namespace: namespace,
		})
	if err != nil {
		log.WithError(err).Error("failed to list objects %s", list.GetObjectKind())
		return err
	}

	if l, ok := list.(*v1.ServiceList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*v1.ServiceAccountList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*v1.SecretList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*v1.ConfigMapList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*v1.PodList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*v1.PersistentVolumeClaimList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*appsv1.DeploymentList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*appsv1.StatefulSetList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*rbacv1.ClusterRoleList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*rbacv1.ClusterRoleBindingList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*rbacv1.RoleList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*rbacv1.RoleBindingList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*storagev1.StorageClassList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*apiextensionsv1.CustomResourceDefinitionList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else if l, ok := list.(*monitoringv1.ServiceMonitorList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, o)
		}
	} else if l, ok := list.(*monitoringv1.PrometheusRuleList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, o)
		}
	} else if l, ok := list.(*monitoringv1.PrometheusList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, o)
		}
	} else if l, ok := list.(*monitoringv1.AlertmanagerList); ok {
		for _, o := range l.Items {
			*objs = append(*objs, &o)
		}
	} else {
		msg := fmt.Sprintf("Unknown object kind %s", list.GetObjectKind())
		log.Error(msg)
		return fmt.Errorf(msg)
	}

	return nil
}
