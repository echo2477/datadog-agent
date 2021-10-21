// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// +build linux

package probe

import (
	"fmt"
	"os"
	"strings"

	"github.com/DataDog/datadog-agent/pkg/ebpf"
	"github.com/DataDog/datadog-agent/pkg/ebpf/bytecode/runtime"
	"github.com/DataDog/datadog-agent/pkg/ebpf/compiler"
	"github.com/DataDog/datadog-agent/pkg/security/log"
	seclog "github.com/DataDog/datadog-agent/pkg/security/log"
	"github.com/DataDog/datadog-agent/pkg/util/kernel"
)

const testCode = `
#include <linux/kconfig.h>
#include <linux/fs.h>

int test() {
	struct inode a;
}
`

var additionalFlags = []string{
	"-D__KERNEL__",
	"-fno-stack-protector",
	"-fno-color-diagnostics",
	"-fno-unwind-tables",
	"-fno-asynchronous-unwind-tables",
	"-fno-jump-tables",
}

func computeConstantsWithRuntimeCompilation(config *ebpf.Config) error {
	seclog.Warnf("Hello Runtime compilation !")

	dirs, _, err := kernel.GetKernelHeaders(config.KernelHeadersDirs, config.KernelHeadersDownloadDir, config.AptConfigDir, config.YumReposDir, config.ZypperReposDir)
	if err != nil {
		return fmt.Errorf("unable to find kernel headers: %w", err)
	}
	comp, err := compiler.NewEBPFCompiler(dirs, config.BPFDebug)
	if err != nil {
		return fmt.Errorf("failed to create compiler: %w", err)
	}
	defer comp.Close()

	flags, _ := runtime.ComputeFlagsAndHash(additionalFlags)

	outputFile, err := os.CreateTemp("", "datadog_cws_constants_fetcher")
	if err != nil {
		return err
	}

	if err := outputFile.Close(); err != nil {
		return err
	}

	inputReader := strings.NewReader(testCode)
	if err := comp.CompileToObjectFile(inputReader, outputFile.Name(), flags); err != nil {
		return err
	}

	output, err := os.ReadFile(outputFile.Name())
	if err != nil {
		return err
	}

	log.Warnf("%v", output)

	return nil
}
