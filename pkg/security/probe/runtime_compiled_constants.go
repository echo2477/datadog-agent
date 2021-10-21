// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

// +build linux

package probe

import (
	"debug/elf"
	"fmt"
	"io"
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
#include <linux/compiler.h>
#include <linux/kconfig.h>
#include <linux/fs.h>

size_t inode_size = sizeof(struct inode);
size_t magic_super_block_offset = offsetof(struct super_block, s_magic);

// #define SEC(NAME) __attribute__((section(NAME), used))
// char _license[] SEC("license") = "MIT";
`

var additionalFlags = []string{
	"-D__KERNEL__",
	"-fno-stack-protector",
	"-fno-color-diagnostics",
	"-fno-unwind-tables",
	"-fno-asynchronous-unwind-tables",
	"-fno-jump-tables",
}

func compileConstantFetcher(config *ebpf.Config) (io.ReaderAt, error) {
	dirs, _, err := kernel.GetKernelHeaders(config.KernelHeadersDirs, config.KernelHeadersDownloadDir, config.AptConfigDir, config.YumReposDir, config.ZypperReposDir)
	if err != nil {
		return nil, fmt.Errorf("unable to find kernel headers: %w", err)
	}
	comp, err := compiler.NewEBPFCompiler(dirs, config.BPFDebug)
	if err != nil {
		return nil, fmt.Errorf("failed to create compiler: %w", err)
	}
	defer comp.Close()

	flags, _ := runtime.ComputeFlagsAndHash(additionalFlags)

	outputFile, err := os.CreateTemp("", "datadog_cws_constants_fetcher")
	if err != nil {
		return nil, err
	}

	if err := outputFile.Close(); err != nil {
		return nil, err
	}

	inputReader := strings.NewReader(testCode)
	if err := comp.CompileToObjectFile(inputReader, outputFile.Name(), flags); err != nil {
		return nil, err
	}

	return os.Open(outputFile.Name())
}

func computeConstantsWithRuntimeCompilation(config *ebpf.Config) error {
	seclog.Warnf("Hello Runtime compilation !")

	elfFile, err := compileConstantFetcher(config)
	if err != nil {
		return err
	}

	f, err := elf.NewFile(elfFile)
	if err != nil {
		return err
	}

	symbols, err := f.Symbols()
	if err != nil {
		return err
	}
	for i, sym := range symbols {
		if i == 0 {
			continue
		}

		log.Infof("%+v", sym)

		section := f.Sections[sym.Section]
		buf := make([]byte, 8)
		section.ReadAt(buf, int64(sym.Value))

		value := f.ByteOrder.Uint64(buf)
		log.Infof("value: %v", value)
	}

	return nil
}
