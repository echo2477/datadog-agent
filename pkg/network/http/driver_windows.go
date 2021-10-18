// +build windows,npm

package http

/*
#include <stdlib.h>
#include <memory.h>
#include "../driver/ddnpmapi.h"

int size_of_http_transaction_type() {
    return sizeof(HTTP_TRANSACTION_TYPE);
}
*/
import "C"
import (
	"unsafe"
	"fmt"
	"net"
	"sync"
	"syscall"

	"github.com/DataDog/datadog-agent/pkg/network/driver"
	"golang.org/x/sys/windows"
	"github.com/DataDog/datadog-agent/pkg/util/log"
)

const (
	httpReadBufferCount = 100
)

type httpDriverInterface struct {
	driverHTTPHandle *driver.Handle
	readBuffers      []*driver.ReadBuffer
	iocp             windows.Handle

	dataChannel chan []driver.HttpTransactionType
	exit		chan bool
	eventLoopWG sync.WaitGroup
}

func newDriverInterface() (*httpDriverInterface, error) {
	d := &httpDriverInterface{}
	err := d.setupHTTPHandle()
	if err != nil {
		return nil, err
	}

	d.dataChannel = make(chan []driver.HttpTransactionType)
	d.exit = make(chan bool)
	return d, nil
}

func (di *httpDriverInterface) setupHTTPHandle() error {
	dh, err := driver.NewHandle(windows.FILE_FLAG_OVERLAPPED, driver.HTTPHandle)
	if err != nil {
		return err
	}

	filters, err := createHTTPFilters()
	if err != nil {
		return err
	}

	if err := dh.SetHTTPFilters(filters); err != nil {
		return err
	}

	iocp, buffers, err := driver.PrepareCompletionBuffers(dh.Handle, httpReadBufferCount)
	if err != nil {
		return err
	}

	di.driverHTTPHandle = dh
	di.iocp = iocp
	di.readBuffers = buffers
	return nil
}


func createHTTPFilters() ([]driver.FilterDefinition, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var filters []driver.FilterDefinition
	for _, iface := range ifaces {
		// IPv4
		filters = append(filters, driver.FilterDefinition{
			FilterVersion:  driver.Signature,
			Size:           driver.FilterDefinitionSize,
			Direction:      driver.DirectionOutbound,
			FilterLayer:    driver.LayerTransport,
			InterfaceIndex: uint64(iface.Index),
			Af:             windows.AF_INET,
			Protocol:       windows.IPPROTO_TCP,
		}, driver.FilterDefinition{
			FilterVersion:  driver.Signature,
			Size:           driver.FilterDefinitionSize,
			Direction:      driver.DirectionInbound,
			FilterLayer:    driver.LayerTransport,
			InterfaceIndex: uint64(iface.Index),
			Af:             windows.AF_INET,
			Protocol:       windows.IPPROTO_TCP,
		})

		// IPv6
		filters = append(filters, driver.FilterDefinition{
			FilterVersion:  driver.Signature,
			Size:           driver.FilterDefinitionSize,
			Direction:      driver.DirectionOutbound,
			FilterLayer:    driver.LayerTransport,
			InterfaceIndex: uint64(iface.Index),
			Af:             windows.AF_INET6,
			Protocol:       windows.IPPROTO_TCP,
		}, driver.FilterDefinition{
			FilterVersion:  driver.Signature,
			Size:           driver.FilterDefinitionSize,
			Direction:      driver.DirectionInbound,
			FilterLayer:    driver.LayerTransport,
			InterfaceIndex: uint64(iface.Index),
			Af:             windows.AF_INET6,
			Protocol:       windows.IPPROTO_TCP,
		})
	}

	return filters, nil
}


func (di *httpDriverInterface) startReadingBuffers() {
	di.eventLoopWG.Add(1)
	go func() {
		defer di.eventLoopWG.Done()

		transactionSize := uint32(C.size_of_http_transaction_type())
		for {
			select {
			case <-di.exit:
				return
			default:
			}

			buf, err, bytesRead := driver.GetReadBufferWhenReady(di.iocp) 
			if iocpIsClosedError(err) {
				// Continue looping until the exit signal is received
				continue
			}
			/*
			if err != nil {
				log.Infof("Error reading http transaction buffer: %s", err.Error())
				continue
			}
			*/

			transactionBatch := make([]driver.HttpTransactionType, driver.HttpBatchSize)
			bytesLeft := uint32(bytesRead)

			var i uint32 = 0
			for bytesLeft > transactionSize {
				transaction := (*driver.HttpTransactionType)(unsafe.Pointer(&buf.Data[i * transactionSize]))
				deepCopyTransactionData(&transactionBatch[i], transaction)

				bytesLeft -= transactionSize
				i += 1
			}

			di.dataChannel <- transactionBatch

			err = driver.StartNextRead(di.driverHTTPHandle.Handle, buf)
			if err != nil && err != windows.ERROR_IO_PENDING {
				log.Infof("Error starting next read: %s", err.Error())
			}
		}
	}()
}

func iocpIsClosedError(err error) bool {
	// ERROR_ABANDONED_WAIT_0 indicates that the iocp handle was closed during a call to
	// GetQueuedCompletionStatus. ERROR_INVALID_HANDLE indicates that the handle was closed
	// prior to the call being made.

	if err == nil {
		return
	}

	log.Infof("Error found: %s", err.Error())

	// TODO run debugger & find the error codes of the errors

	if err == syscall.Errno(windows.ERROR_ABANDONED_WAIT_0) {
		log.Infof("    IOCP was closed during call", err.Error())
	}

	if err == syscall.Errno(windows.ERROR_INVALID_HANDLE) {
		log.Infof("    IOCP was already closed", err.Error())
	}

	return true
}

func deepCopyTransactionData(dest, src *driver.HttpTransactionType) {

	// TODO find why the uint64 fields are missing

	dest.Tup.Saddr    = src.Tup.Saddr
	dest.Tup.Daddr    = src.Tup.Daddr
	dest.Tup.Sport    = src.Tup.Sport
	dest.Tup.Dport    = src.Tup.Dport 
	// dest.Tup.Pid      = src.Tup.Pid	// == Pad_cgo_0 [8]byte
	dest.Tup.Protocol = src.Tup.Protocol
	dest.Tup.Family   = src.Tup.Family

	dest.RequestMethod	    = src.RequestMethod
	// dest.RequestStarted     = src.RequestStarted  //  == Pad_cgo_0 [8]byte
	dest.ResponseStatusCode = src.ResponseStatusCode
	// dest.ResponseLastSeen   = src.responseLastSeen   //  == Pad_cgo_1 [8]byte
    dest.RequestFragment    = src.RequestFragment
}

func (di *httpDriverInterface) flushPendingTransactions() {

	// TODO create ioctl call for flushing pending transactions & call it here

	log.Infof("flushed pending transactions")
}

func (di *httpDriverInterface) close() error {
	// Must close iocp handle before sending exit signal
	err := di.closeDriverHandles()
	
	di.exit <- true
	di.eventLoopWG.Wait()
	close(di.exit)
	close(di.dataChannel)

	for _, buf := range di.readBuffers {
		C.free(unsafe.Pointer(buf))
	}
	di.readBuffers = nil
	return err
}

func (di *httpDriverInterface) closeDriverHandles() error {
	err := windows.CancelIoEx(di.driverHTTPHandle.Handle, nil)
	if err != nil && err != windows.ERROR_NOT_FOUND {
		// TODO should we return here? We should comment out PrepareCompletionBuffers & run this.
		// This should a) verify that we're checking against windows.ERROR_NOT_FOUND correctly, and
		// b) see if we end up getting stuck (which is what I'd expect... )
		// It looks like it should be safe to close the handle even if there are outstanding IO messages:
		// see https://social.msdn.microsoft.com/Forums/SQLSERVER/en-US/5d67623b-fe3f-463e-950d-7af24e3243ca/safe-to-call-closehandle-when-an-overlapped-io-is-in-progress?forum=windowsgeneraldevelopmentissues
		return fmt.Errorf("error cancelling HTTP io completion: %w", err)
	}
	err = windows.CloseHandle(di.iocp)
	if err != nil {
		return fmt.Errorf("error closing HTTP io completion handle: %w", err)
	}
	err = di.driverHTTPHandle.Close()
	if err != nil {
		return fmt.Errorf("error closing driver HTTP file handle: %w", err)
	}
	return nil
}
