// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// +build kubelet

package kubelet

import (
	"context"
	"fmt"
	"strings"

	"github.com/DataDog/datadog-agent/pkg/config"
	"github.com/DataDog/datadog-agent/pkg/util/kubernetes/clustername"
	k "github.com/DataDog/datadog-agent/pkg/util/kubernetes/kubelet"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

type kubeUtilGetter func() (k.KubeUtilInterface, error)

var kubeUtilGet kubeUtilGetter = k.GetKubeUtil

// HostnameProvider builds a hostname from the kubernetes nodename and an optional cluster-name
func HostnameProvider(ctx context.Context, options map[string]interface{}) (string, error) {
	if !config.IsFeaturePresent(config.Kubernetes) {
		return "", nil
	}

	ku, err := kubeUtilGet()
	if err != nil {
		return "", err
	}
	nodeName, err := ku.GetNodename(ctx)
	if err != nil {
		return "", fmt.Errorf("couldn't fetch the host nodename from the kubelet: %s", err)
	}

	clusterName := getRFC1123CompliantClusterName(ctx, nodeName)
	if clusterName == "" {
		log.Debugf("Now using plain kubernetes nodename as an alias: no cluster name was set and none could be autodiscovered")
		return nodeName, nil
	}
	return nodeName + "-" + clusterName, nil
}

// getRFC1123CompliantClusterName returns a k8s cluster name if it exists, either directly specified or autodiscovered
// Some kubernetes cluster-names (EKS,AKS) are not RFC1123 compliant, mostly due to an `_`.
// This function replaces the invalid `_` with a valid `-`.
func getRFC1123CompliantClusterName(ctx context.Context, hostname string) string {
	clusterName := clustername.GetClusterName(ctx, hostname)
	return makeClusterNameRFC1123Compliant(clusterName)
}

func makeClusterNameRFC1123Compliant(clusterName string) string {
	finalName := clusterName
	if strings.Contains(clusterName, "_") {
		log.Warnf("hostAlias: cluster name: '%s' contains `_`, replacing it with `-` to be RFC1123 compliant", clusterName)
		finalName = strings.ReplaceAll(clusterName, "_", "-")
	}
	hasChanged := false
	for strings.HasSuffix(finalName, "-") {
		finalName = strings.TrimSuffix(finalName, "-")
		hasChanged = true
	}
	if hasChanged {
		log.Warnf("hostAlias: cluster name: '%s' ends with `-` or `_` trimming it to be RFC1123 compliant", clusterName)
	}
	return finalName
}
