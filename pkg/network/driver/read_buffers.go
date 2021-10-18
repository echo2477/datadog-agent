//+build windows

package driver

/*
#include <stdlib.h>
#include <memory.h>
*/
import "C"
import (
	"unsafe"
	"syscall"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

// ReadBuffer is the type that an overlapped read returns -- the overlapped object, which must be passed
// back to the kernel after reading followed by a predictably sized chunk of bytes
type ReadBuffer struct {
	ol windows.Overlapped

	// Read buffers are used by both the HTTP and DNS filters.
	// The DNS filter requires a 1500 byte read buffer: this is the MTU of IPv6, which effectively governs
	// the maximum DNS packet size over IPv6. see https://tools.ietf.org/id/draft-madi-dnsop-udp4dns-00.html
	//
	// 1500 bytes also satisfies the requirements of the HTTP filter: each HTTP transaction is 95 bytes, and we've
	// defined our batch size to be 15 transactions, therefore, we need 1425 bytes in our read buffer.
	Data [1500]byte
}

// PrepareCompletionBuffers prepares N read buffers and return the IoCompletionPort that will be used to coordinate reads.
// danger: even though all reads will reference the returned iocp, buffers must be in-scope as long
// as reads are happening. Otherwise, the memory the kernel is writing to will be written to memory reclaimed
// by the GC
func PrepareCompletionBuffers(handle windows.Handle, count int) (iocp windows.Handle, buffers []*ReadBuffer, err error) {
	iocp, err = windows.CreateIoCompletionPort(handle, windows.Handle(0), 0, 0)
	if err != nil {
		return windows.Handle(0), nil, errors.Wrap(err, "error creating IO completion port")
	}

	buffers = make([]*ReadBuffer, count)
	for i := 0; i < count; i++ {
		buf := (*ReadBuffer)(C.malloc(C.size_t(unsafe.Sizeof(ReadBuffer{}))))
		C.memset(unsafe.Pointer(buf), 0, C.size_t(unsafe.Sizeof(ReadBuffer{})))
		buffers[i] = buf

		err = windows.ReadFile(handle, buf.Data[:], nil, &(buf.ol))
		if err != nil && err != windows.ERROR_IO_PENDING {
			_ = windows.CloseHandle(iocp)
			return windows.Handle(0), nil, errors.Wrap(err, "failed to initiate readfile")
		}
	}

	return iocp, buffers, nil
}

// GetReadBufferIfReady immediately returns a completed ReadBuffer if one is available. If none
// are available, it returns a nil buffer.
func GetReadBufferIfReady(iocp windows.Handle) (*ReadBuffer, error, uint32) {
	var bytesRead uint32
	var key uintptr // returned by GetQueuedCompletionStatus, then ignored
	var ol *windows.Overlapped

	err := windows.GetQueuedCompletionStatus(iocp, &bytesRead, &key, &ol, 0)
	if err != nil {
		if err == syscall.Errno(syscall.WAIT_TIMEOUT) {
			// this indicates that there was no queued completion status, this is fine
			return nil, nil, 0
		}

		return nil, errors.Wrap(err, "could not get queued completion status"), 0
	}

	return (*ReadBuffer)(unsafe.Pointer(ol)), nil, bytesRead
}

// GetReadBufferWhenReady blocks until a completed ReadBuffer becomes available, then returns it.
// If the iocp given is closed after GetReadBufferWhenReady is called, it will unblock & return an error. If  
// the iocp is already closed when GetReadBufferWhenReady is called, it will immediately return an error.
func GetReadBufferWhenReady(iocp windows.Handle) (*ReadBuffer, error, uint32) {
	var bytesRead uint32
	var key uintptr // returned by GetQueuedCompletionStatus, then ignored
	var ol *windows.Overlapped
	
	err := windows.GetQueuedCompletionStatus(iocp, &bytesRead, &key, &ol, syscall.INFINITE)
	if err != nil {
		return nil, errors.Wrap(err, "could not get queued completion status"), 0
	}

	return (*ReadBuffer)(unsafe.Pointer(ol)), nil, bytesRead
}

// StartNextRead takes a read buffer whose data has been read & sends it back to the driver
func StartNextRead(handle windows.Handle, usedBuf *ReadBuffer) error {
	return windows.ReadFile(handle, usedBuf.Data[:], nil, &(usedBuf.ol))
}
