// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021-present Datadog, Inc.

//go:build docker,linux
// +build docker,linux

package metrics

import (
	"fmt"
	"time"

	"github.com/DataDog/datadog-agent/pkg/util"
	"github.com/DataDog/datadog-agent/pkg/util/log"
	"github.com/docker/docker/api/types"
)

func convertContainerStats(stats *types.Stats) *ContainerStats {
	return &ContainerStats{
		Timestamp: time.Now(),
		CPU:       convertCPUStats(&stats.CPUStats),
		Memory:    convertMemoryStats(&stats.MemoryStats),
		IO:        convertIOStats(&stats.BlkioStats),
		PID:       convertPIDStats(&stats.PidsStats),
	}
}

func convertCPUStats(cpuStats *types.CPUStats) *ContainerCPUStats {
	return &ContainerCPUStats{
		Total:            util.Float64Ptr(float64(cpuStats.CPUUsage.TotalUsage)),
		System:           util.Float64Ptr(float64(cpuStats.CPUUsage.UsageInKernelmode)),
		User:             util.Float64Ptr(float64(cpuStats.CPUUsage.UsageInUsermode)),
		ThrottledPeriods: util.Float64Ptr(float64(cpuStats.ThrottlingData.ThrottledPeriods)),
		ThrottledTime:    util.Float64Ptr(float64(cpuStats.ThrottlingData.ThrottledTime)),
	}
}

func convertMemoryStats(memStats *types.MemoryStats) *ContainerMemStats {
	log.Infof("XXXXXXXX DEBUG DEBUG DEBUG %#v", memStats.Stats)

	return &ContainerMemStats{
		UsageTotal: util.Float64Ptr(float64(memStats.Usage)),
		Limit:      util.Float64Ptr(float64(memStats.Limit)),
	}
}

func convertIOStats(ioStats *types.BlkioStats) *ContainerIOStats {
	containerIOStats := ContainerIOStats{
		ReadBytes:       util.Float64Ptr(0),
		WriteBytes:      util.Float64Ptr(0),
		ReadOperations:  util.Float64Ptr(0),
		WriteOperations: util.Float64Ptr(0),
		Devices:         make(map[string]DeviceIOStats),
	}

	for _, blkioStatEntry := range ioStats.IoServiceBytesRecursive {
		deviceName := fmt.Sprintf("%d:%d", blkioStatEntry.Major, blkioStatEntry.Minor)
		device := containerIOStats.Devices[deviceName]
		switch blkioStatEntry.Op {
		case "read":
			device.ReadBytes = util.Float64Ptr(float64(blkioStatEntry.Value))
		case "write":
			device.WriteBytes = util.Float64Ptr(float64(blkioStatEntry.Value))
		default:
			log.Infof("XXXXXXXXXXXXX DEBUG DEBUG DEBUG %#v", blkioStatEntry.Op)
		}
		containerIOStats.Devices[deviceName] = device
	}

	for _, blkioStatEntry := range ioStats.IoServicedRecursive {
		deviceName := fmt.Sprintf("%d:%d", blkioStatEntry.Major, blkioStatEntry.Minor)
		device := containerIOStats.Devices[deviceName]
		switch blkioStatEntry.Op {
		case "Read":
			device.ReadOperations = util.Float64Ptr(float64(blkioStatEntry.Value))
		case "Write":
			device.WriteOperations = util.Float64Ptr(float64(blkioStatEntry.Value))
		default:
			log.Infof("XXXXXXXXXXXXX DEBUG DEBUG DEBUG %#v", blkioStatEntry.Op)
		}
		containerIOStats.Devices[deviceName] = device
	}

	for _, device := range containerIOStats.Devices {
		*containerIOStats.ReadBytes += *device.ReadBytes
		*containerIOStats.WriteBytes += *device.WriteBytes
		*containerIOStats.ReadOperations += *device.ReadOperations
		*containerIOStats.WriteOperations += *device.WriteOperations
	}

	return &containerIOStats
}

func convertPIDStats(pidStats *types.PidsStats) *ContainerPIDStats {
	return &ContainerPIDStats{
		ThreadCount: util.Float64Ptr(float64(pidStats.Current)),
		ThreadLimit: util.Float64Ptr(float64(pidStats.Limit)),
	}
}
