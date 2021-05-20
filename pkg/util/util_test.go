package util

import (
	"testing"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetImageURN(t *testing.T) {
	// TestCase: Empty image
	out := getImageURN("registry.io", "", commonDockerRegistries)
	require.Equal(t, "", out)

	// TestCase: Empty repo and registry
	out = getImageURN("", "test/image", commonDockerRegistries)
	require.Equal(t, "test/image", out)

	// TestCase: Registry without repo but image with repo
	out = getImageURN("registry.io", "test/image", commonDockerRegistries)
	require.Equal(t, "registry.io/test/image", out)

	out = getImageURN("registry.io/", "test/image", commonDockerRegistries)
	require.Equal(t, "registry.io/test/image", out)

	out = getImageURN("registry.io", "test/this/image", commonDockerRegistries)
	require.Equal(t, "registry.io/test/this/image", out)

	// TestCase: Registry and image without repo
	out = getImageURN("registry.io", "image", commonDockerRegistries)
	require.Equal(t, "registry.io/image", out)

	// TestCase: Image with common docker registries
	out = getImageURN("registry.io", "docker.io/test/image", commonDockerRegistries)
	require.Equal(t, "registry.io/test/image", out)

	out = getImageURN("registry.io", "quay.io/test/this/image", commonDockerRegistries)
	require.Equal(t, "registry.io/test/this/image", out)

	out = getImageURN("registry.io/", "index.docker.io/test/this/image", commonDockerRegistries)
	require.Equal(t, "registry.io/test/this/image", out)

	out = getImageURN("registry.io", "registry-1.docker.io/image", commonDockerRegistries)
	require.Equal(t, "registry.io/image", out)

	out = getImageURN("registry.io/", "registry.connect.redhat.com/image", commonDockerRegistries)
	require.Equal(t, "registry.io/image", out)

	// TestCase: Regsitry and image both with repo
	out = getImageURN("registry.io/repo", "test/image", commonDockerRegistries)
	require.Equal(t, "registry.io/repo/image", out)

	out = getImageURN("registry.io/repo", "test/this/image", commonDockerRegistries)
	require.Equal(t, "registry.io/repo/image", out)

	out = getImageURN("registry.io/repo/", "test/image", commonDockerRegistries)
	require.Equal(t, "registry.io/repo/image", out)

	out = getImageURN("registry.io/repo//", "test/this/image", commonDockerRegistries)
	require.Equal(t, "registry.io/repo/image", out)

	// TestCase: Regsitry with repo but image without repo
	out = getImageURN("registry.io/repo", "image", commonDockerRegistries)
	require.Equal(t, "registry.io/repo/image", out)

	out = getImageURN("registry.io/repo/subdir", "image", commonDockerRegistries)
	require.Equal(t, "registry.io/repo/subdir/image", out)

	// TestCase: Registry with empty root repo
	out = getImageURN("registry.io//", "image", commonDockerRegistries)
	require.Equal(t, "registry.io/image", out)

	out = getImageURN("registry.io//", "test/image", commonDockerRegistries)
	require.Equal(t, "registry.io/image", out)

	out = getImageURN("registry.io//", "test/this/image", commonDockerRegistries)
	require.Equal(t, "registry.io/image", out)
}

func TestGetImageMajorVersion(t *testing.T) {
	ver := GetImageMajorVersion("docker.io/test/image:v0.1.0")
	require.Equal(t, 0, ver)

	ver = GetImageMajorVersion("quay.io/test/image:v5.1.0")
	require.Equal(t, 5, ver)

	ver = GetImageMajorVersion("quay.io/test/image:5.1.0")
	require.Equal(t, 5, ver)

	ver = GetImageMajorVersion("quay.io/test/image")
	require.Equal(t, -1, ver)

	ver = GetImageMajorVersion("")
	require.Equal(t, -1, ver)

	ver = GetImageMajorVersion("quay.io/a:v999.998.997")
	require.Equal(t, 999, ver)
}

func TestPartialSecretRef(t *testing.T) {
	// happy
	obj := &corev1.SecretRef{
		SecretName: "a",
		SecretKey:  "b",
	}
	assert.False(t, IsPartialSecretRef(obj))

	// no valid secret key
	obj = &corev1.SecretRef{
		SecretName: "a",
		SecretKey:  "",
	}
	assert.True(t, IsPartialSecretRef(obj))

	obj = &corev1.SecretRef{
		SecretName: "a",
	}
	assert.True(t, IsPartialSecretRef(obj))

	// no valid secret name
	obj = &corev1.SecretRef{
		SecretName: "",
		SecretKey:  "b",
	}
	assert.True(t, IsPartialSecretRef(obj))

	obj = &corev1.SecretRef{
		SecretKey: "b",
	}
	assert.True(t, IsPartialSecretRef(obj))

	// no valid secret key or name. Return false because it's empty, not partial
	obj = &corev1.SecretRef{
		SecretName: "",
		SecretKey:  "",
	}
	assert.False(t, IsPartialSecretRef(obj))

	obj = &corev1.SecretRef{}
	assert.False(t, IsPartialSecretRef(obj))

	obj = nil
	assert.False(t, IsPartialSecretRef(obj))
}
