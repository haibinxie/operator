package migration

import (
	"fmt"
	"reflect"
	"sort"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/operator/pkg/util"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
)

//const (
//	dryRunLogFile = "portworxMigrationDryRun.log"
//)
//
//func (h *Handler) writeDryRunLog(msg string) {
//	f, err := os.OpenFile(dryRunLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
//	if err != nil {
//		logrus.Warningf("failed to open log %v", err)
//		return
//	}
//
//	defer f.Close()
//	if _, err := f.WriteString(msg); err != nil {
//		logrus.Warningf("failed to write log %v", err)
//	}
//}

func (h *Handler) dryRun(cluster *corev1.StorageCluster, ds *appsv1.DaemonSet) error {
	//os.Remove(dryRunLogFile)
	//h.writeDryRunLog("")

	var msg string
	podSpec, err := h.ctrl.Driver.GetStoragePodSpec(cluster, "")
	if err != nil {
		return err
	}

	//h.writeDryRunLog("--- Start compare portworx pod spec (first: daemonset, second: operator)\n")
	err = h.deepEqualPod(&ds.Spec.Template.Spec, &podSpec)
	if err != nil {
		msg += fmt.Sprintf("Validate portworx pod failed, %v\n", err)
	}
	//h.writeDryRunLog("--- End compare portworx pod spec\n")

	if msg != "" {
		return fmt.Errorf(msg)
	}
	return nil
}

// DeepEqualPod compares two pods.
func (h *Handler) deepEqualPod(p1, p2 *v1.PodSpec) error {
	var msg string

	err := h.deepEqualVolumes(p1.Volumes, p2.Volumes)
	if err != nil {
		msg += fmt.Sprintf("Validate volumes failed: %s\n", err.Error())
	}

	err = h.deepEqualContainers(p1.Containers, p2.Containers)
	if err != nil {
		msg += fmt.Sprintf("Validate containers failed: %s\n", err.Error())
	}

	err = h.deepEqualContainers(p1.InitContainers, p2.InitContainers)
	if err != nil {
		msg += fmt.Sprintf("Validate init-containers failed: %s\n", err.Error())
	}

	if !reflect.DeepEqual(p1.ImagePullSecrets, p1.ImagePullSecrets) {
		msg += fmt.Sprintf("ImagePullSecrets are different: first %+v, second: %+v\n", p1.ImagePullSecrets, p1.ImagePullSecrets)
	}

	if msg != "" {
		return fmt.Errorf(msg)
	}
	return nil
}

func (h *Handler) deepEqualVolumes(vol1, vol2 []v1.Volume) error {
	// Use host path as key, as volume name may be different but path is the same.
	getVolumeKey := func(v interface{}) string {
		vol := v.(v1.Volume)

		if vol.HostPath != nil {
			return vol.HostPath.Path
		}

		return vol.Name
	}

	getVolumeList := func(arr []v1.Volume) []interface{} {
		var objs []interface{}
		for _, obj := range arr {
			// "crioconf" only exists with operator install.
			// "dev" only exists with daemonset install.
			if obj.Name == "crioconf" || obj.Name == "dev" {
				continue
			}
			objs = append(objs, obj)
		}
		return objs
	}

	return util.DeepEqualObjects(
		getVolumeList(vol1),
		getVolumeList(vol2),
		getVolumeKey,
		h.deepEqualVolume)
}

func (h *Handler) deepEqualVolume(obj1, obj2 interface{}) error {
	t1 := obj1.(v1.Volume)
	t2 := obj2.(v1.Volume)

	vol1 := t1.DeepCopy()
	vol2 := t2.DeepCopy()

	cleanupFields := func(vol v1.Volume) v1.Volume {
		// We dont compare name, but only the content
		// Ignore hostPath type as nil may represent default value.
		vol.Name = ""
		vol.HostPath.Type = nil
		return vol
	}

	return util.DeepEqualObject(cleanupFields(*vol1), cleanupFields(*vol2))
}

func (h *Handler) deepEqualContainers(c1, c2 []v1.Container) error {
	getContainerName := func(v interface{}) string {
		return v.(v1.Container).Name
	}
	getContainerList := func(arr []v1.Container) []interface{} {
		objs := make([]interface{}, len(arr))
		for i, obj := range arr {
			objs[i] = obj
		}
		return objs
	}

	return util.DeepEqualObjects(
		getContainerList(c1),
		getContainerList(c2),
		getContainerName,
		h.deepEqualContainer)
}

// DeepEqualContainer compare two containers
func (h *Handler) deepEqualContainer(obj1, obj2 interface{}) error {
	t1 := obj1.(v1.Container)
	t2 := obj2.(v1.Container)

	c1 := t1.DeepCopy()
	c2 := t2.DeepCopy()

	msg := ""
	if c1.Image != c2.Image {
		msg += fmt.Sprintf("image is different: %s, %s\n", c1.Image, c2.Image)
	}

	sort.Strings(c1.Command)
	sort.Strings(c2.Command)
	if !reflect.DeepEqual(c1.Command, c2.Command) {
		msg += fmt.Sprintf("command is different: %s, %s\n", t1.Command, t2.Command)
	}

	sort.Strings(c1.Args)
	sort.Strings(c2.Args)
	if !reflect.DeepEqual(c1.Args, c2.Args) {
		msg += fmt.Sprintf("args is different: %s, %s\n", t1.Args, t2.Args)
	}

	if msg != "" {
		return fmt.Errorf(msg)
	}
	return nil
}
