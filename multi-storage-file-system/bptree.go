package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/NVIDIA/sortedmap"
)

type toAddToGlobalsStruct struct {
	inodeMap             *inodeNumberToInodeStructMapStruct                      // Key: inodeStruct.inodeNumber;                                              Value: *inodeStruct
	inodeEvictionQueue   *xTimeInodeNumberSetStruct                              // Key: tuple(inodeStruct.xTime,inodeStruct.inodeNumber);                     Value: struct{}
	physChildDirEntryMap *parentInodeNumberChildBasenameToChildInodeNumberStruct // Key: tuple(parent's inodeStruct.inodeNumber,child's inodeStruct.basename); Value: child's inodeStruct.inodeNumber
	virtChildDirEntryMap *parentInodeNumberChildBasenameToChildInodeNumberStruct // Key: tuple(parent's inodeStruct.inodeNumber,child's inodeStruct.basename); Value: child's inodeStruct.inodeNumber
}

var toAddToGlobals toAddToGlobalsStruct

func bptree_init() {
	globals.logger.Printf("[TODO] bptree_init() called")
	toAddToGlobals.inodeMap = inodeNumberToInodeStructMapStructCreate("globals.inodeMap", globals.config.inodeMapKeysPerPageMax, globals.config.inodeMapPageEvictLowLimit, globals.config.inodeMapPageEvictHighLimit, globals.config.inodeMapPageDirtyFlushTrigger, globals.config.inodeMapFlushedPerGC)
	toAddToGlobals.inodeEvictionQueue = xTimeInodeNumberSetStructCreate("globals.inodeEvictionQueue", globals.config.inodeEvictionQueueKeysPerPageMax, globals.config.inodeEvictionQueuePageEvictLowLimit, globals.config.inodeEvictionQueuePageEvictHighLimit, globals.config.inodeEvictionQueuePageDirtyFlushTrigger, globals.config.inodeEvictionQueueFlushedPerGC)
	toAddToGlobals.physChildDirEntryMap = parentInodeNumberChildBasenameToChildInodeNumberStructCreate("globals.physChildDirEntryMap", globals.config.physChildDirEntryMapKeysPerPageMax, globals.config.physChildDirEntryMapPageEvictLowLimit, globals.config.physChildDirEntryMapPageEvictHighLimit, globals.config.physChildDirEntryMapPageDirtyFlushTrigger, globals.config.physChildDirEntryMapFlushedPerGC)
	toAddToGlobals.virtChildDirEntryMap = parentInodeNumberChildBasenameToChildInodeNumberStructCreate("globals.virtChildDirEntryMap", globals.config.virtChildDirEntryMapKeysPerPageMax, globals.config.virtChildDirEntryMapPageEvictLowLimit, globals.config.virtChildDirEntryMapPageEvictHighLimit, globals.config.virtChildDirEntryMapPageDirtyFlushTrigger, globals.config.virtChildDirEntryMapFlushedPerGC)
}

// `inodeNumberToInodeStructMapStruct` is used to maintain a sortedmap.BPlusTree used
// to map an inodeStruct.inodeNumber to the corresponding inodeStruct. An instance of
// this struct is used to provide globals.inodeMap functionality.
type inodeNumberToInodeStructMapStruct struct {
	name                  string
	bpTree                sortedmap.BPlusTree // Key: inodeStruct.inodeNumber; Value: *inodeStruct
	bpTreeCache           sortedmap.BPlusTreeCache
	pageDirtyFlushTrigger uint64
	flushesSinceLastGC    uint64
	flushesSinceLastGCMax uint64
}

// `inodeNumberToInodeStructMapStructCreate` is called to instantiate a `inodeNumberToInodeStructMapStruct`.
func inodeNumberToInodeStructMapStructCreate(name string, maxKeysPerNode, evictLowLimit, evictHighLimit, pageDirtyFlushTrigger, flushesSinceLastGCMax uint64) (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) {
	inodeNumberToInodeStructMap = &inodeNumberToInodeStructMapStruct{
		name:                  name,
		bpTreeCache:           sortedmap.NewBPlusTreeCache(evictLowLimit, evictHighLimit),
		pageDirtyFlushTrigger: pageDirtyFlushTrigger,
		flushesSinceLastGC:    0,
		flushesSinceLastGCMax: flushesSinceLastGCMax,
	}
	inodeNumberToInodeStructMap.bpTree = sortedmap.NewBPlusTree(maxKeysPerNode, sortedmap.CompareUint64, inodeNumberToInodeStructMap, inodeNumberToInodeStructMap.bpTreeCache)
	return
}

// `delete` is called to delete an inode from a `inodeNumberToInodeStructMapStruct`.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) delete(inode *inodeStruct) (ok bool) {
	var (
		err error
	)

	ok, err = inodeNumberToInodeStructMap.bpTree.DeleteByKey(inode.inodeNumber)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.DeleteByKey(inode.inodeNumber) failed: %v", err)
	}

	inodeNumberToInodeStructMap.flushIfNecessary()

	return
}

// `discard` is called to discard a `inodeNumberToInodeStructMapStruct`.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) discard() {
	var (
		err error
	)

	err = inodeNumberToInodeStructMap.bpTree.Discard()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Discard() failed: %v", err)
	}
}

// `get` is called to, given an `inodeNumber`, fetch an inode from a `inodeNumberToInodeStructMapStruct`.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) get(inodeNumber uint64) (inode *inodeStruct, ok bool) {
	var (
		err          error
		inodeAsValue sortedmap.Value
	)

	inodeAsValue, ok, err = inodeNumberToInodeStructMap.bpTree.GetByKey(inodeNumber)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.GetByKey(inodeNumber) failed: %v", err)
	}

	if ok {
		inode = inodeAsValue.(*inodeStruct)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inodeAsValue.(*inodeStruct) returned !ok")
		}
	}

	return
}

// `put` is called to add an `inode` to a `inodeNumberToInodeStructMapStruct`.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) put(inode *inodeStruct) (ok bool) {
	var (
		err error
	)

	ok, err = inodeNumberToInodeStructMap.bpTree.Put(inode.inodeNumber, inode)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Put(inode.inodeNumber, inode) failed: %v", err)
	}

	inodeNumberToInodeStructMap.flushIfNecessary()

	return
}

// `touch` is called to indicate that an `inode` has been modified.`
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) touch(inode *inodeStruct) (ok bool) {
	var (
		err error
	)

	ok, err = inodeNumberToInodeStructMap.bpTree.DeleteByKey(inode.inodeNumber)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.DeleteByKey(inode.inodeNumber) failed: %v", err)
	}

	if !ok {
		return
	}

	ok, err = inodeNumberToInodeStructMap.bpTree.Put(inode.inodeNumber, inode)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Put(inode.inodeNumber, inode) failed: %v", err)
	}
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Put(inode.inodeNumber, inode) returned !ok")
	}

	inodeNumberToInodeStructMap.flushIfNecessary()

	return
}

// `flushIfNecessary` will track the number of updates (one is assumed per call to this function)
// and use the configured updates per flush to decide when to trigger a flush.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) flushIfNecessary() {
	var (
		bpTreeCacheStats *sortedmap.BPlusTreeCacheStats
		err              error
	)

	bpTreeCacheStats = inodeNumberToInodeStructMap.bpTreeCache.Stats()

	if bpTreeCacheStats.DirtyLRUItems > inodeNumberToInodeStructMap.pageDirtyFlushTrigger {
		_, _, _, err = inodeNumberToInodeStructMap.bpTree.Flush(false)
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Flush(false) failed: %v", err)
		}
		err = inodeNumberToInodeStructMap.bpTree.Prune()
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Prune() failed: %v", err)
		}

		if inodeNumberToInodeStructMap.flushesSinceLastGCMax > 0 {
			inodeNumberToInodeStructMap.flushesSinceLastGC++

			if inodeNumberToInodeStructMap.flushesSinceLastGC >= inodeNumberToInodeStructMap.flushesSinceLastGCMax {
				runtime.GC()
				debug.FreeOSMemory()

				inodeNumberToInodeStructMap.flushesSinceLastGC = 0
			}
		}
	}
}

// `DumpKey` is here to satisfy sortedmap.DumpCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		keyAsU64 uint64
		ok       bool
	)

	keyAsU64, ok = key.(uint64)
	if !ok {
		err = errors.New("key.(uint64) returned !ok")
		return
	}

	keyAsString = fmt.Sprintf("%016X", keyAsU64)

	err = nil
	return
}

// `DumpValue` is here to satisfy sortedmap.DumpCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		inode *inodeStruct
		ok    bool
	)

	inode, ok = value.(*inodeStruct)
	if !ok {
		err = errors.New("value.(*inodeStruct) returned !ok")
		return
	}

	valueAsString = fmt.Sprintf("[%p] %#v", inode, inode)

	err = nil
	return
}

// `GetNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) GetNode(objectNumber, objectOffset, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err = readNodeFile(objectNumber, objectOffset, objectLength)
	return
}

// `PutNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) PutNode(nodeByteSlice []byte) (objectNumber, objectOffset uint64, err error) {
	objectNumber, objectOffset, err = writeNodeFile(nodeByteSlice)
	return
}

// `DiscardNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) DiscardNode(objectNumber, objectOffset, objectLength uint64) (err error) {
	err = discardNodeFile(objectNumber, objectOffset, objectLength)
	return
}

// `PackKey` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsU64 uint64
		ok       bool
	)

	keyAsU64, ok = key.(uint64)
	if !ok {
		err = errors.New("key.(uint64) returned !ok")
		return
	}

	packedKey = make([]byte, 8)

	binary.BigEndian.PutUint64(packedKey, keyAsU64)

	err = nil
	return
}

// `UnpackKey` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	if len(payloadData) < 8 {
		err = errors.New("len(payloadData) < 8")
		return
	}

	key = binary.BigEndian.Uint64(payloadData[:8])
	bytesConsumed = 8

	err = nil
	return
}

// `PackValue` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		basenameAsByteSlice   []byte
		cacheMapElementKey    uint64
		cacheMapElementValue  uint64
		cacheMapLen           int
		eTagAsByteSlice       []byte
		fhSetElementKey       uint64
		fhSetLen              int
		inode                 *inodeStruct
		objectPathAsByteSlice []byte
		ok                    bool
		packedValueLen        int
		packedValuePos        int
	)

	inode, ok = value.(*inodeStruct)
	if !ok {
		err = errors.New("value.(*inodeStruct) returned !ok")
		return
	}

	objectPathAsByteSlice = []byte(inode.objectPath)
	basenameAsByteSlice = []byte(inode.basename)
	eTagAsByteSlice = []byte(inode.eTag)

	cacheMapLen = len(inode.cacheMap)
	fhSetLen = len(inode.fhSet)

	packedValueLen = 0 +
		8 + //                              inodeNumber
		4 + //                              inodeType
		8 + //                              backendNonce
		8 + //                              parentInodeNumber
		1 + //                              isVirt
		8 + len(objectPathAsByteSlice) + // objectPath
		8 + len(basenameAsByteSlice) + //   basename
		8 + //                              sizeInBackend
		8 + //                              sizeInMemory
		8 + len(eTagAsByteSlice) + //       eTag
		4 + //                              mode
		8 + //                              mTime
		8 + //                              xTime
		1 + //                              isPrefetchInProgress
		8 + (cacheMapLen * (8 + 8)) + //    cacheMap
		8 + //                              inboundCacheLineCount
		8 + //                              outboundCacheLineCount
		8 + //                              dirtyCacheLineCount
		8 + (fhSetLen * 8) + //             fhSet
		1 //                                pendingDelete

	packedValue = make([]byte, packedValueLen)

	packedValuePos = 0

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.inodeNumber)
	packedValuePos += 8

	binary.BigEndian.PutUint32(packedValue[packedValuePos:packedValuePos+4], inode.inodeType)
	packedValuePos += 4

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.backendNonce)
	packedValuePos += 8

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.parentInodeNumber)
	packedValuePos += 8

	if inode.isVirt {
		packedValue[packedValuePos] = 1
	} else {
		packedValue[packedValuePos] = 0
	}
	packedValuePos++

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(len(objectPathAsByteSlice)))
	packedValuePos += 8
	copy(packedValue[packedValuePos:packedValuePos+len(objectPathAsByteSlice)], objectPathAsByteSlice)
	packedValuePos += len(objectPathAsByteSlice)

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(len(basenameAsByteSlice)))
	packedValuePos += 8
	copy(packedValue[packedValuePos:packedValuePos+len(basenameAsByteSlice)], basenameAsByteSlice)
	packedValuePos += len(basenameAsByteSlice)

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.sizeInBackend)
	packedValuePos += 8

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.sizeInMemory)
	packedValuePos += 8

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(len(eTagAsByteSlice)))
	packedValuePos += 8
	copy(packedValue[packedValuePos:packedValuePos+len(eTagAsByteSlice)], eTagAsByteSlice)
	packedValuePos += len(eTagAsByteSlice)

	binary.BigEndian.PutUint32(packedValue[packedValuePos:packedValuePos+4], inode.mode)
	packedValuePos += 4

	if inode.mTime.IsZero() {
		binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(0))
	} else {
		binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(inode.mTime.UnixNano()))
	}
	packedValuePos += 8

	if inode.xTime.IsZero() {
		binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(0))
	} else {
		binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(inode.xTime.UnixNano()))
	}
	packedValuePos += 8

	if inode.isPrefetchInProgress {
		packedValue[packedValuePos] = 1
	} else {
		packedValue[packedValuePos] = 0
	}
	packedValuePos++

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(cacheMapLen))
	packedValuePos += 8
	for cacheMapElementKey, cacheMapElementValue = range inode.cacheMap {
		binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], cacheMapElementKey)
		packedValuePos += 8
		binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], cacheMapElementValue)
		packedValuePos += 8
	}

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.inboundCacheLineCount)
	packedValuePos += 8

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.outboundCacheLineCount)
	packedValuePos += 8

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], inode.dirtyCacheLineCount)
	packedValuePos += 8

	binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], uint64(fhSetLen))
	packedValuePos += 8
	for fhSetElementKey = range inode.fhSet {
		binary.BigEndian.PutUint64(packedValue[packedValuePos:packedValuePos+8], fhSetElementKey)
		packedValuePos += 8
	}

	if inode.pendingDelete {
		packedValue[packedValuePos] = 1
	} else {
		packedValue[packedValuePos] = 0
	}
	packedValuePos++

	if packedValueLen != packedValuePos {
		err = fmt.Errorf("packedValueLen(%v) != packedValuePos(%v)", packedValueLen, packedValuePos)
		return
	}

	err = nil
	return
}

// `UnpackValue` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	var (
		basenameAsByteSliceLen   uint64
		cacheMapElementKey       uint64
		cacheMapElementValue     uint64
		cacheMapLen              uint64
		eTagAsByteSliceLen       uint64
		fhSetElementKey          uint64
		fhSetLen                 uint64
		inode                    *inodeStruct
		mTimeAsUint64            uint64
		objectPathAsByteSliceLen uint64
		xTimeAsUint64            uint64
	)

	inode = &inodeStruct{}

	bytesConsumed = 0

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .inodeNumber", len(payloadData))
		return
	}
	inode.inodeNumber = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 4) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .inodeType", len(payloadData))
		return
	}
	inode.inodeType = binary.BigEndian.Uint32(payloadData[bytesConsumed : bytesConsumed+4])
	bytesConsumed += 4

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .backendNonce", len(payloadData))
		return
	}
	inode.backendNonce = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .parentInodeNumber", len(payloadData))
		return
	}
	inode.parentInodeNumber = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 1) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .isVirt", len(payloadData))
		return
	}
	inode.isVirt = (payloadData[bytesConsumed] == 1)
	bytesConsumed++

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .objectPath [case 1]", len(payloadData))
		return
	}
	objectPathAsByteSliceLen = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8
	if uint64(len(payloadData)) < (bytesConsumed + objectPathAsByteSliceLen) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .objectPath [case 2]", len(payloadData))
		return
	}
	inode.objectPath = string(payloadData[bytesConsumed : bytesConsumed+objectPathAsByteSliceLen])
	bytesConsumed += objectPathAsByteSliceLen

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .basename [case 1]", len(payloadData))
		return
	}
	basenameAsByteSliceLen = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8
	if uint64(len(payloadData)) < (bytesConsumed + basenameAsByteSliceLen) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .basename [case 2]", len(payloadData))
		return
	}
	inode.objectPath = string(payloadData[bytesConsumed : bytesConsumed+basenameAsByteSliceLen])
	bytesConsumed += basenameAsByteSliceLen

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .sizeInBackend", len(payloadData))
		return
	}
	inode.sizeInBackend = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .sizeInMemory", len(payloadData))
		return
	}
	inode.sizeInMemory = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .eTag [case 1]", len(payloadData))
		return
	}
	eTagAsByteSliceLen = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8
	if uint64(len(payloadData)) < (bytesConsumed + eTagAsByteSliceLen) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .eTag [case 2]", len(payloadData))
		return
	}
	inode.eTag = string(payloadData[bytesConsumed : bytesConsumed+eTagAsByteSliceLen])
	bytesConsumed += eTagAsByteSliceLen

	if uint64(len(payloadData)) < (bytesConsumed + 4) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .mode", len(payloadData))
		return
	}
	inode.mode = binary.BigEndian.Uint32(payloadData[bytesConsumed : bytesConsumed+4])
	bytesConsumed += 4

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .mTime", len(payloadData))
		return
	}
	mTimeAsUint64 = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8
	if mTimeAsUint64 == 0 {
		inode.mTime = time.Time{}
	} else {
		inode.mTime = time.Unix(0, int64(mTimeAsUint64))
	}

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .xTime", len(payloadData))
		return
	}
	xTimeAsUint64 = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8
	if xTimeAsUint64 == 0 {
		inode.xTime = time.Time{}
	} else {
		inode.xTime = time.Unix(0, int64(xTimeAsUint64))
	}

	if uint64(len(payloadData)) < (bytesConsumed + 1) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .isPrefetchInProgress", len(payloadData))
		return
	}
	inode.isPrefetchInProgress = (payloadData[bytesConsumed] == 1)
	bytesConsumed++

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .cacheMap [case 1]", len(payloadData))
		return
	}
	cacheMapLen = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8
	if uint64(len(payloadData)) < (bytesConsumed + (cacheMapLen * (8 + 8))) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .cacheMap [case 2]", len(payloadData))
		return
	}
	inode.cacheMap = make(map[uint64]uint64, cacheMapLen)
	for range cacheMapLen {
		cacheMapElementKey = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
		bytesConsumed += 8
		cacheMapElementValue = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
		bytesConsumed += 8
		inode.cacheMap[cacheMapElementKey] = cacheMapElementValue
	}

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .inboundCacheLineCount", len(payloadData))
		return
	}
	inode.inboundCacheLineCount = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .outboundCacheLineCount", len(payloadData))
		return
	}
	inode.outboundCacheLineCount = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .dirtyCacheLineCount", len(payloadData))
		return
	}
	inode.dirtyCacheLineCount = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8

	if uint64(len(payloadData)) < (bytesConsumed + 8) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .fhSet [case 1]", len(payloadData))
		return
	}
	fhSetLen = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
	bytesConsumed += 8
	if uint64(len(payloadData)) < (bytesConsumed + (fhSetLen * 8)) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .fhSet [case 2]", len(payloadData))
		return
	}
	inode.fhSet = make(map[uint64]struct{}, fhSetLen)
	for range fhSetLen {
		fhSetElementKey = binary.BigEndian.Uint64(payloadData[bytesConsumed : bytesConsumed+8])
		bytesConsumed += 8
		inode.fhSet[fhSetElementKey] = struct{}{}
	}

	if uint64(len(payloadData)) < (bytesConsumed + 1) {
		err = fmt.Errorf("len(payloadData) [%v] insufficient to decode .pendingDelete", len(payloadData))
		return
	}
	inode.pendingDelete = (payloadData[bytesConsumed] == 1)
	bytesConsumed++

	value = inode
	err = nil

	return
}

// `xTimeInodeNumberSetStruct` is used to maintain a sortedmap.BPlusTree used
// to track the tuple made up of inodeStruct.xTime and inodeStruct.inodenumber
// as a set such that it implements a time-ordered (by xTime) queue. An instance
// of this struct is used to provide globals.inodeEvictionQueue functionality.
type xTimeInodeNumberSetStruct struct {
	name                  string
	bpTree                sortedmap.BPlusTree // Key: tuple(inodeStruct.xTime,inodeStruct.inodeNumber); Value: struct{}
	bpTreeCache           sortedmap.BPlusTreeCache
	pageDirtyFlushTrigger uint64
	flushesSinceLastGC    uint64
	flushesSinceLastGCMax uint64
}

// `xTimeInodeNumberSetStructCreate` is called to instantiate a `xTimeInodeNumberSetStruct`.
func xTimeInodeNumberSetStructCreate(name string, maxKeysPerNode, evictLowLimit, evictHighLimit, pageDirtyFlushTrigger, flushesSinceLastGCMax uint64) (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) {
	xTimeInodeNumberSet = &xTimeInodeNumberSetStruct{
		name:                  name,
		bpTreeCache:           sortedmap.NewBPlusTreeCache(evictLowLimit, evictHighLimit),
		pageDirtyFlushTrigger: pageDirtyFlushTrigger,
		flushesSinceLastGC:    0,
		flushesSinceLastGCMax: flushesSinceLastGCMax,
	}
	xTimeInodeNumberSet.bpTree = sortedmap.NewBPlusTree(maxKeysPerNode, sortedmap.CompareByteSlice, xTimeInodeNumberSet, xTimeInodeNumberSet.bpTreeCache)
	return
}

// `discard` is called to discard a `inodeNumberToInodeStructMapStruct`.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) discard() {
	var (
		err error
	)

	err = xTimeInodeNumberSet.bpTree.Discard()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Discard() failed: %v", err)
	}
}

// `front` is called to find the `inode` at the front of a `inodeNumberToInodeStructMapStruct`.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) front() (xTime time.Time, inodeNumber uint64, ok bool) {
	var (
		err                         error
		xTimeInodeNumberAsByteSlice []byte
		xTimeInodeNumberAsKey       sortedmap.Key
	)

	xTimeInodeNumberAsKey, _, ok, err = xTimeInodeNumberSet.bpTree.GetByIndex(0)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] xTimeInodeNumberSet.bpTree.GetByIndex(0) failed: %v", err)
	}

	if ok {
		xTimeInodeNumberAsByteSlice, ok = xTimeInodeNumberAsKey.([]byte)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] xTimeInodeNumberAsKey.([]byte) returned !ok")
		}

		xTime, inodeNumber = byteSliceToTimeTimeUint64Tuple(xTimeInodeNumberAsByteSlice)
	}

	return
}

// `insert` is called to insert an `inode` into a `inodeNumberToInodeStructMapStruct`.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) insert(inode *inodeStruct) (ok bool) {
	var (
		err error
	)

	ok, err = xTimeInodeNumberSet.bpTree.Put(timeTimeUint64TupleToByteSlice(inode.xTime, inode.inodeNumber), struct{}{})
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] xTimeInodeNumberSet.bpTree.Put(timeTimeUint64TupleToByteSlice(inode.xTime,inode.inodeNumber), struct{}{}) failed: %v", err)
	}

	xTimeInodeNumberSet.flushIfNecessary()

	return
}

// `remove` is called to remove an `inode` from a `inodeNumberToInodeStructMapStruct`.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) remove(inode *inodeStruct) (ok bool) {
	var (
		err error
	)

	ok, err = xTimeInodeNumberSet.bpTree.DeleteByKey(timeTimeUint64TupleToByteSlice(inode.xTime, inode.inodeNumber))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] xTimeInodeNumberSet.bpTree.DeleteByKey(timeTimeUint64TupleToByteSlice(inode.xTime,inode.inodeNumber)) failed: %v", err)
	}

	xTimeInodeNumberSet.flushIfNecessary()

	return
}

// `flushIfNecessary` will track the number of updates (one is assumed per call to this function)
// and use the configured updates per flush to decide when to trigger a flush.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) flushIfNecessary() {
	var (
		bpTreeCacheStats *sortedmap.BPlusTreeCacheStats
		err              error
	)

	bpTreeCacheStats = xTimeInodeNumberSet.bpTreeCache.Stats()

	if bpTreeCacheStats.DirtyLRUItems > xTimeInodeNumberSet.pageDirtyFlushTrigger {
		_, _, _, err = xTimeInodeNumberSet.bpTree.Flush(false)
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] xTimeInodeNumberSet.bpTree.Flush(false) failed: %v", err)
		}
		err = xTimeInodeNumberSet.bpTree.Prune()
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] xTimeInodeNumberSet.bpTree.Prune() failed: %v", err)
		}

		if xTimeInodeNumberSet.flushesSinceLastGCMax > 0 {
			xTimeInodeNumberSet.flushesSinceLastGC++

			if xTimeInodeNumberSet.flushesSinceLastGC >= xTimeInodeNumberSet.flushesSinceLastGCMax {
				runtime.GC()

				xTimeInodeNumberSet.flushesSinceLastGC = 0
			}
		}
	}
}

// `byteSliceToTimeTimeUint64Tuple` is called to convert a byte slice to a time.Time uint64 tuple
// that will preserve the sort order inferred by comparing byte slices with sortedmap.CompareByteSlice().
func byteSliceToTimeTimeUint64Tuple(bs []byte) (t time.Time, u64 uint64) {
	var (
		tAsU64 uint64
	)

	if len(bs) != 16 {
		dumpStack()
		globals.logger.Fatalf("[FATAL] len(bs) != 16")
	}

	tAsU64 = binary.BigEndian.Uint64(bs[:8])
	if tAsU64 == 0 {
		t = time.Time{}
	} else {
		t = time.Unix(0, int64(tAsU64))
	}

	u64 = binary.BigEndian.Uint64(bs[8:])

	return
}

// `timeTimeUint64TupleToByteSlice` is called to convert a time.Time uint64 tuple to a byte slice
// that will preserve the sort order inferred by comparing byte slices with sortedmap.CompareByteSlice().
func timeTimeUint64TupleToByteSlice(t time.Time, u64 uint64) (bs []byte) {
	bs = make([]byte, 16)

	if t.IsZero() {
		binary.BigEndian.PutUint64(bs[:8], uint64(0))
	} else {
		binary.BigEndian.PutUint64(bs[:8], uint64(t.UnixNano()))
	}

	binary.BigEndian.PutUint64(bs[8:], u64)

	return
}

// `DumpKey` is here to satisfy sortedmap.DumpCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		keyAsByteSlice []byte
		keyTimeTime    time.Time
		keyU64         uint64
		ok             bool
	)

	keyAsByteSlice, ok = key.([]byte)
	if !ok {
		err = errors.New("key.([]byte) returned !ok")
		return
	}

	keyTimeTime, keyU64 = byteSliceToTimeTimeUint64Tuple(keyAsByteSlice)

	keyAsString = fmt.Sprintf("%s %016X", keyTimeTime.Format(time.RFC3339Nano), keyU64)

	err = nil
	return
}

// `DumpValue` is here to satisfy sortedmap.DumpCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok bool
	)

	_, ok = value.(struct{})
	if !ok {
		err = errors.New("value.(struct{}) returned !ok")
		return
	}

	valueAsString = "struct{}"

	err = nil
	return
}

// `GetNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) GetNode(objectNumber, objectOffset, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err = readNodeFile(objectNumber, objectOffset, objectLength)
	return
}

// `PutNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) PutNode(nodeByteSlice []byte) (objectNumber, objectOffset uint64, err error) {
	objectNumber, objectOffset, err = writeNodeFile(nodeByteSlice)
	return
}

// `DiscardNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) DiscardNode(objectNumber, objectOffset, objectLength uint64) (err error) {
	err = discardNodeFile(objectNumber, objectOffset, objectLength)
	return
}

// `PackKey` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		ok bool
	)

	packedKey, ok = key.([]byte)
	if !ok {
		err = errors.New(" key.([]byte) returned !ok")
		return
	}

	err = nil
	return
}

// `UnpackKey` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	if len(payloadData) < 16 {
		err = errors.New("len(payloadData) < 16")
		return
	}

	key = payloadData[:16]
	bytesConsumed = 16

	err = nil
	return
}

// `PackValue` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		ok bool
	)

	_, ok = value.(struct{})
	if !ok {
		err = errors.New("value.(struct{}) returned !ok")
		return
	}

	packedValue = make([]byte, 0)

	err = nil
	return
}

// `UnpackValue` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	value = struct{}{}
	bytesConsumed = 0

	err = nil
	return
}

// `parentInodeNumberChildBasenameToChildInodeNumberStruct` is used to maintain a
// sortedmap.BPlusTree used to map the tuple made up of a parent inodeStruct.inodeNumber
// and a child inodeStruct.basename to the child inodeStruct.inodeNumber. An instance of
// this struct is used to provide globals.{phys|virt}ChildDirEntryMap functionality.
type parentInodeNumberChildBasenameToChildInodeNumberStruct struct {
	name                  string
	bpTree                sortedmap.BPlusTree // Key: tuple(parent's inodeStruct.inodeNumber,child's inodeStruct.basename); Value: child's inodeStruct.inodeNumber
	bpTreeCache           sortedmap.BPlusTreeCache
	pageDirtyFlushTrigger uint64
	flushesSinceLastGC    uint64
	flushesSinceLastGCMax uint64
}

// `parentInodeNumberChildBasenameToChildInodeNumberStructCreate` is called to instantiate a `parentInodeNumberChildBasenameToChildInodeNumberStruct`.
func parentInodeNumberChildBasenameToChildInodeNumberStructCreate(name string, maxKeysPerNode, evictLowLimit, evictHighLimit, pageDirtyFlushTrigger, flushesSinceLastGCMax uint64) (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) {
	parentInodeNumberChildBasenameToChildInodeNumber = &parentInodeNumberChildBasenameToChildInodeNumberStruct{
		name:                  name,
		bpTreeCache:           sortedmap.NewBPlusTreeCache(evictLowLimit, evictHighLimit),
		pageDirtyFlushTrigger: pageDirtyFlushTrigger,
		flushesSinceLastGC:    0,
		flushesSinceLastGCMax: flushesSinceLastGCMax,
	}
	parentInodeNumberChildBasenameToChildInodeNumber.bpTree = sortedmap.NewBPlusTree(maxKeysPerNode, sortedmap.CompareByteSlice, parentInodeNumberChildBasenameToChildInodeNumber, parentInodeNumberChildBasenameToChildInodeNumber.bpTreeCache)
	return
}

// `delete` is called to put a `childInode` in a `parentInode's` logical `{phys|virt}ChildDirEntryMap`.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) delete(parentInode, childInode *inodeStruct) (ok bool) {
	var (
		err error
	)

	ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.DeleteByKey(uint64StringTupleToByteSlice(parentInode.inodeNumber, childInode.basename))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.DeleteByKey(uint64StringTupleToByteSlice(parentInode.inodeNumber, childInode.basename)) failed: %v", err)
	}

	parentInodeNumberChildBasenameToChildInodeNumber.flushIfNecessary()

	return
}

// `discard` is called to discard a `parentInodeNumberChildBasenameToChildInodeNumberStruct`.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) discard() {
	var (
		err error
	)

	err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Discard()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Discard() failed: %v", err)
	}
}

// `getByBasename` is called to find the `childInode` in a `parentInode's` logical `{phys|virt}ChildDirEntryMap`.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) getByBasename(parentInode *inodeStruct, childBasename string) (childInodeNumber uint64, ok bool) {
	var (
		childInodeNumberAsValue sortedmap.Value
		err                     error
	)

	childInodeNumberAsValue, ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByKey(uint64StringTupleToByteSlice(parentInode.inodeNumber, childBasename))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByKey(uint64StringTupleToByteSlice(parentInode.inodeNumber, childBasename)) failed: %v", err)
	}

	if ok {
		childInodeNumber, ok = childInodeNumberAsValue.(uint64)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] childInodeNumberAsValue.(uint64) returned !ok")
		}
	}

	return
}

// `getByIndex` is called to find the `inode` in `globals.{phys|virt}ChildDirEntryMap` given its `index`.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) getByIndex(index uint64) (childInodeNumber uint64, ok bool) {
	var (
		childInodeNumberAsValue sortedmap.Value
		err                     error
	)

	_, childInodeNumberAsValue, ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByIndex(int(index))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByIndex(int(index)) failed: %v", err)
	}

	if ok {
		childInodeNumber, ok = childInodeNumberAsValue.(uint64)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] childInodeNumberAsValue.(uint64) returned !ok")
		}
	}

	return
}

// `getIndexRange` is called to get the (inclusive) `start` and (exclusive) `limit` indices in `globals.{phys|virt}ChildDirEntryMap` for a given `parentInode`.
// Note that the returned range is "closed" on the left (start) and "open" on the right (limit).
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) getIndexRange(parentInode *inodeStruct) (start, limit uint64) {
	var (
		err        error
		limitAsInt int
		startAsInt int
	)

	startAsInt, _, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInode.inodeNumber, ""))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInode.inodeNumber, \"\")) failed: %v", err)
	}
	limitAsInt, _, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInode.inodeNumber+1, ""))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInode.inodeNumber+1, \"\")) failed: %v", err)
	}

	start = uint64(startAsInt)
	limit = uint64(limitAsInt)

	return
}

// `put` is called to put a `childInode` in a `parentInode's` logical `{phys|virt}ChildDirEntryMap`.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) put(parentInode, childInode *inodeStruct) (ok bool) {
	var (
		err error
	)

	ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Put(uint64StringTupleToByteSlice(parentInode.inodeNumber, childInode.basename), childInode.inodeNumber)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Put(uint64StringTupleToByteSlice(parentInode.inodeNumber, childInode.basename),childInode.inodeNumber) failed: %v", err)
	}

	parentInodeNumberChildBasenameToChildInodeNumber.flushIfNecessary()

	return
}

// `flushIfNecessary` will track the number of updates (one is assumed per call to this function)
// and use the configured updates per flush to decide when to trigger a flush.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) flushIfNecessary() {
	var (
		bpTreeCacheStats *sortedmap.BPlusTreeCacheStats
		err              error
	)

	bpTreeCacheStats = parentInodeNumberChildBasenameToChildInodeNumber.bpTreeCache.Stats()

	if bpTreeCacheStats.DirtyLRUItems > parentInodeNumberChildBasenameToChildInodeNumber.pageDirtyFlushTrigger {
		_, _, _, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Flush(false)
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Flush(false) failed: %v", err)
		}
		err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Prune()
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Prune() failed: %v", err)
		}

		if parentInodeNumberChildBasenameToChildInodeNumber.flushesSinceLastGCMax > 0 {
			parentInodeNumberChildBasenameToChildInodeNumber.flushesSinceLastGC++

			if parentInodeNumberChildBasenameToChildInodeNumber.flushesSinceLastGC >= parentInodeNumberChildBasenameToChildInodeNumber.flushesSinceLastGCMax {
				runtime.GC()

				parentInodeNumberChildBasenameToChildInodeNumber.flushesSinceLastGC = 0
			}
		}
	}
}

// `byteSliceToUint64StringTuple` is called to convert a byte slice to a uint64 string tuple
// that will preserve the sort order inferred by comparing byte slices with sortedmap.CompareByteSlice().
func byteSliceToUint64StringTuple(bs []byte) (u64 uint64, s string) {
	if len(bs) < 8 {
		dumpStack()
		globals.logger.Fatalf("[FATAL] len(bs) < 9")
	}

	u64 = binary.BigEndian.Uint64(bs[:8])
	s = string(bs[8:])

	return
}

// `timeTimeUint64TupleToByteSlice` is called to convert a uint64 string tuple to a byte slice
// that will preserve the sort order inferred by comparing byte slices with sortedmap.CompareByteSlice().
func uint64StringTupleToByteSlice(u64 uint64, s string) (bs []byte) {
	var (
		sAsByteSlice []byte
	)

	sAsByteSlice = []byte(s)

	bs = make([]byte, 8, 8+len(sAsByteSlice))

	binary.BigEndian.PutUint64(bs, u64)

	bs = append(bs, sAsByteSlice...)

	return
}

// `DumpKey` is here to satisfy sortedmap.DumpCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	var (
		keyAsByteSlice []byte
		keyString      string
		keyU64         uint64
		ok             bool
	)

	keyAsByteSlice, ok = key.([]byte)
	if !ok {
		err = errors.New("key.([]byte) returned !o")
		return
	}

	keyU64, keyString = byteSliceToUint64StringTuple(keyAsByteSlice)

	keyAsString = fmt.Sprintf("%016X %s", keyU64, keyString)

	err = nil
	return
}

// `DumpValue` is here to satisfy sortedmap.DumpCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	var (
		ok         bool
		valueAsU64 uint64
	)

	valueAsU64, ok = value.(uint64)
	if !ok {
		err = errors.New("value.(uint64) returned !ok")
		return
	}

	valueAsString = fmt.Sprintf("%016X", valueAsU64)

	err = nil
	return
}

// `GetNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) GetNode(objectNumber, objectOffset, objectLength uint64) (nodeByteSlice []byte, err error) {
	nodeByteSlice, err = readNodeFile(objectNumber, objectOffset, objectLength)
	return
}

// `PutNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) PutNode(nodeByteSlice []byte) (objectNumber, objectOffset uint64, err error) {
	objectNumber, objectOffset, err = writeNodeFile(nodeByteSlice)
	return
}

// `DiscardNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) DiscardNode(objectNumber, objectOffset, objectLength uint64) (err error) {
	err = discardNodeFile(objectNumber, objectOffset, objectLength)
	return
}

// `PackKey` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) PackKey(key sortedmap.Key) (packedKey []byte, err error) {
	var (
		keyAsByteSlice []byte
		ok             bool
	)

	keyAsByteSlice, ok = key.([]byte)
	if !ok {
		err = errors.New("key.([]byte) returned !ok")
		return
	}

	packedKey = make([]byte, 8, 8+len(keyAsByteSlice))

	binary.BigEndian.PutUint64(packedKey, uint64(len(keyAsByteSlice)))
	packedKey = append(packedKey, keyAsByteSlice...)

	err = nil
	return
}

// `UnpackKey` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) UnpackKey(payloadData []byte) (key sortedmap.Key, bytesConsumed uint64, err error) {
	var (
		keyLen uint64
	)

	if len(payloadData) < 8 {
		err = errors.New("len(payloadData) < 8")
		return
	}

	keyLen = binary.BigEndian.Uint64(payloadData[:8])

	bytesConsumed = 8 + keyLen
	key = payloadData[8:bytesConsumed]

	err = nil
	return
}

// `PackValue` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) PackValue(value sortedmap.Value) (packedValue []byte, err error) {
	var (
		ok         bool
		valueAsU64 uint64
	)

	valueAsU64, ok = value.(uint64)
	if !ok {
		err = errors.New("value.(uint64) returned !ok")
		return
	}

	packedValue = make([]byte, 8)

	binary.BigEndian.PutUint64(packedValue, valueAsU64)

	err = nil
	return
}

// `UnpackValue` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if len(payloadData) < 8 {
		err = errors.New("len(payloadData) < 8")
		return
	}

	value = binary.BigEndian.Uint64(payloadData[:8])
	bytesConsumed = 8

	err = nil
	return
}

// `readNodeFile` provides a generic function to read a node's existing file in its entirety.
func readNodeFile(nodeFileNumber, mustBeZeroOffset, requiredLength uint64) (nodeByteSlice []byte, err error) {
	var (
		closeErr     error
		nodeFile     *os.File
		nodeFileInfo os.FileInfo
		nodeFilePath string
	)

	if mustBeZeroOffset != 0 {
		err = fmt.Errorf("mustBeZeroOffset[%v] != 0", mustBeZeroOffset)
		return
	}

	nodeFilePath = fmt.Sprintf("%s/%016X", globals.cacheDir, nodeFileNumber)

	nodeFile, err = os.Open(nodeFilePath)
	if err != nil {
		err = fmt.Errorf("os.Open(nodeFilePath[\"%s\"]) failed: %v", nodeFilePath, err)
		return
	}

	nodeFileInfo, err = nodeFile.Stat()
	if err != nil {
		err = fmt.Errorf("nodeFile[\"%s\"].Stat() failed: %v", nodeFilePath, err)
		closeErr = nodeFile.Close()
		if closeErr != nil {
			err = fmt.Errorf("%s [and nodeFile.Close() failed: %v]", err, closeErr)
		}
		return
	}

	if uint64(nodeFileInfo.Size()) != requiredLength {
		err = fmt.Errorf("uint64(nodeFileInfo[\"%s\"].Size())[%v] != requiredLength[%v]", nodeFilePath, nodeFileInfo.Size(), requiredLength)
		closeErr = nodeFile.Close()
		if closeErr != nil {
			err = fmt.Errorf("%s [and nodeFile.Close() failed: %v]", err, closeErr)
		}
		return
	}

	nodeByteSlice = make([]byte, requiredLength)

	_, err = nodeFile.ReadAt(nodeByteSlice, int64(mustBeZeroOffset))
	if err != nil {
		err = fmt.Errorf("nodeFile[\"%s\"].ReadAt(buf, int(mustBeZeroOffset)) failed: %v", nodeFilePath, err)
		closeErr = nodeFile.Close()
		if closeErr != nil {
			err = fmt.Errorf("%s [and nodeFile.Close() failed: %v]", err, closeErr)
		}
		return
	}

	err = nodeFile.Close()
	if err != nil {
		err = fmt.Errorf("nodeFile[\"%s\"].Close() failed: %v", nodeFilePath, err)
		return
	}

	return
}

// `writeNodeFile` provides a generic function to write a node's new file in its entirety.
func writeNodeFile(nodeByteSlice []byte) (nodeFileNumber, nodeFileOffset uint64, err error) {
	var (
		closeErr     error
		nodeFile     *os.File
		nodeFilePath string
	)

	nodeFileNumber = fetchNonce()
	nodeFileOffset = 0

	nodeFilePath = fmt.Sprintf("%s/%016X", globals.cacheDir, nodeFileNumber)

	nodeFile, err = os.OpenFile(nodeFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			err = fmt.Errorf("os.OpenFile(nodeFilePath[\"%s\"], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) failed due to pre-existing file", nodeFilePath)
		} else {
			err = fmt.Errorf("os.OpenFile(nodeFilePath[\"%s\"], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) failed: %v", nodeFilePath, err)
		}
		return
	}

	_, err = nodeFile.Write(nodeByteSlice)
	if err != nil {
		err = fmt.Errorf("nodeFile[\"%s\"].Write(nodeByteSlice) failed: %v", nodeFilePath, err)
		closeErr = nodeFile.Close()
		if closeErr != nil {
			err = fmt.Errorf("%s [and nodeFile.Close() failed: %v]", err, closeErr)
		}
		return
	}

	err = nodeFile.Close()
	if err != nil {
		err = fmt.Errorf("nodeFile[\"%s\"].Close() failed: %v", nodeFilePath, err)
		return
	}

	return
}

// `discardNodeFile` provides a generic function to discard a node's existing file.
func discardNodeFile(nodeFileNumber, mustBeZeroOffset, requiredLength uint64) (err error) {
	var (
		closeErr     error
		nodeFile     *os.File
		nodeFileInfo os.FileInfo
		nodeFilePath string
	)

	if mustBeZeroOffset != 0 {
		err = fmt.Errorf("mustBeZeroOffset[%v] != 0", mustBeZeroOffset)
		return
	}

	nodeFilePath = fmt.Sprintf("%s/%016X", globals.cacheDir, nodeFileNumber)

	nodeFile, err = os.Open(nodeFilePath)
	if err != nil {
		err = fmt.Errorf("os.Open(nodeFilePath[\"%s\"]) failed: %v", nodeFilePath, err)
		return
	}

	nodeFile, err = os.OpenFile(nodeFilePath, os.O_RDWR, 0o600)
	if err != nil {
		err = fmt.Errorf("os.OpenFile(nodeFilePath[\"%s\"], os.O_RDWR, 0o600) failed: %v", nodeFilePath, err)
		return
	}

	nodeFileInfo, err = nodeFile.Stat()
	if err != nil {
		err = fmt.Errorf("nodeFile[\"%s\"].Stat() failed: %v", nodeFilePath, err)
		closeErr = nodeFile.Close()
		if closeErr != nil {
			err = fmt.Errorf("%s [and nodeFile.Close() failed: %v]", err, closeErr)
		}
		return
	}

	if uint64(nodeFileInfo.Size()) != requiredLength {
		err = fmt.Errorf("uint64(nodeFileInfo[\"%s\"].Size())[%v] != requiredLength[%v]", nodeFilePath, nodeFileInfo.Size(), requiredLength)
		closeErr = nodeFile.Close()
		if closeErr != nil {
			err = fmt.Errorf("%s [and nodeFile.Close() failed: %v]", err, closeErr)
		}
		return
	}

	err = os.Remove(nodeFilePath)
	if err != nil {
		err = fmt.Errorf("os.Remove(nodeFilePath[\"%s\"], os.O_RDWR, 0o600) failed: %v", nodeFilePath, err)
		closeErr = nodeFile.Close()
		if closeErr != nil {
			err = fmt.Errorf("%s as did nodeFile.Close(): %v", err, closeErr)
		}
		return
	}

	err = nodeFile.Close()
	if err != nil {
		err = fmt.Errorf("nodeFile[\"%s\"].Close() failed: %v", nodeFilePath, err)
		return
	}

	return
}
