// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021-present Datadog, Inc.

//go:build docker,windows
// +build docker,windows

package metrics

import (
	"time"

	"github.com/DataDog/datadog-agent/pkg/util"
	"github.com/docker/docker/api/types"
)

func convertContainerStats(stats *types.Stats) *ContainerStats {
	return &ContainerStats{
		Timestamp: time.Now(),
		CPU:       convertCPUStats(&stats.CPUStats),
		Memory:    convertMemoryStats(&stats.MemoryStats),
		IO:        convertIOStats(&stats.StorageStats),
		PID:       convertPIDStats(stats.NumProcs),
	}
}

func convertCPUStats(cpuUsage *types.CPUStats) *ContainerCPUStats {
	return &ContainerCPUStats{
		Total:  util.Float64Ptr(float64(100 * cpuStats.CPUUsage.TotalUsage)),
		System: util.Float64Ptr(float64(100 * cpuStats.CPUUsage.UsageInKernelmode)),
		User:   util.Float64Ptr(float64(100 * cpuStats.CPUUsage.UsageInUsermode)),
	}
}

func convertMemoryStats(memStats *types.MemoryStats) *ContainerMemStats {
	return &ContainerMemStats{
		UsageTotal:        util.Float64Ptr(float64(memStats.Usage)),
		Limit:             util.Float64Ptr(float64(memStats.Limit)),
		PrivateWorkingSet: util.Float64Ptr(float64(memStats.PrivateWorkingSet)),
		CommitBytes:       util.Float64Ptr(float64(memStats.Commit)),
		CommitPeakBytes:   util.Float64Ptr(float64(memStats.CommitPeak)),
	}
}

func convertIOStats(storageStats *types.StorageStats) *ContainerIOStats {
	return &ContainerIOStats{
		ReadBytes:       util.Float64Ptr(float64(storageStats.ReadSizeBytes)),
		WriteBytes:      util.Float64Ptr(float64(storageStats.WriteSizeBytes)),
		ReadOperations:  util.Float64PTR(float64(storageStats.ReadCountNormalized)),
		WriteOperations: util.Float64Ptr(float64(storageStats.WriteCountNormalized)),
	}
}

func convertPIDStats(numProcs uint32) *ContainerPIDStats {
	return &ContainerPIDStats{
		ThreadCount: util.Float64Ptr(float64(numProcs)),
	}
}
