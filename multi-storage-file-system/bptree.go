package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/NVIDIA/sortedmap"
	"github.com/cockroachdb/pebble/v2"
)

// `pebbleLogger` routes Pebble's internal logging through globals.logger with a
// "[PEBBLE]" prefix. Adopted from main's f8c77acc (PebbleDB integration); wired
// into bptreePageStore's pebble.Options below.
type pebbleLogger struct{}

func (pebbleLogger) Errorf(format string, args ...interface{}) {
	globals.logger.Printf("[ERROR] [PEBBLE] "+format, args...)
}

func (pebbleLogger) Fatalf(format string, args ...interface{}) {
	globals.logger.Fatalf("[FATAL] [PEBBLE] "+format, args...)
}

func (pebbleLogger) Infof(format string, args ...interface{}) {
	globals.logger.Printf("[INFO] [PEBBLE] "+format, args...)
}

type bptreePageStore struct {
	db *pebble.DB
}

var bptreePages *bptreePageStore

func newBptreePageStore(dirPath string) (*bptreePageStore, error) {
	dbPath := filepath.Join(dirPath, "pages.db")
	cacheSize := int64(globals.config.pebbleCacheSize)
	memTableSize := globals.config.pebbleMemTableSize
	l0CompactionFileThreshold := int(globals.config.pebbleL0CompactionFileThreshold)
	l0StopWritesThreshold := int(globals.config.pebbleL0StopWritesThreshold)
	cache := pebble.NewCache(cacheSize)
	// pebble.Open takes its own ref on cache; release our init ref on all return paths
	// so the cache is freed when bptreePageStore.close() shuts the DB down.
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                     cache,
		MemTableSize:              memTableSize,
		DisableWAL:                true,
		L0CompactionFileThreshold: l0CompactionFileThreshold,
		L0StopWritesThreshold:     l0StopWritesThreshold,
		Logger:                    pebbleLogger{},
	}
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("pebble.Open(%q) failed: %w", dbPath, err)
	}
	return &bptreePageStore{db: db}, nil
}

func (ps *bptreePageStore) get(objectNumber, objectLength uint64) ([]byte, error) {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, objectNumber)

	data, closer, err := ps.db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("pageStore.get(%016X) failed: %w", objectNumber, err)
	}
	defer closer.Close()

	if objectLength != uint64(len(data)) {
		return nil, fmt.Errorf("pageStore.get(%016X): objectLength[%d] != len(data)[%d]", objectNumber, objectLength, len(data))
	}

	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (ps *bptreePageStore) put(objectNumber uint64, data []byte) error {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, objectNumber)
	if err := ps.db.Set(key, data, pebble.NoSync); err != nil {
		return fmt.Errorf("pageStore.put(%016X) failed: %w", objectNumber, err)
	}
	return nil
}

func (ps *bptreePageStore) delete(objectNumber uint64) error {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, objectNumber)
	if err := ps.db.Delete(key, pebble.NoSync); err != nil {
		return fmt.Errorf("pageStore.delete(%016X) failed: %w", objectNumber, err)
	}
	return nil
}

func (ps *bptreePageStore) close() error {
	if ps == nil || ps.db == nil {
		return nil
	}
	return ps.db.Close()
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
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) delete(inodeNumber uint64) (ok bool) {
	var (
		err error
	)

	ok, err = inodeNumberToInodeStructMap.bpTree.DeleteByKey(inodeNumber)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.DeleteByKey(inodeNumber) failed: %v", err)
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

// `len` is called to report the number of inodeNumber to inodeStruct items are in a `inodeNumberToInodeStructMapStruct`.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) len() (numberOfItems int) {
	var (
		err error
	)

	numberOfItems, err = inodeNumberToInodeStructMap.bpTree.Len()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.Len() failed: %v", err)
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

	ok, err = inodeNumberToInodeStructMap.bpTree.PatchByKey(inode.inodeNumber, inode)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inodeNumberToInodeStructMap.bpTree.PatchByKey(inode.inodeNumber, inode) failed: %v", err)
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
	if objectOffset != 0 {
		err = fmt.Errorf("objectOffset(%v) != 0", objectOffset)
		return
	}
	nodeByteSlice, err = bptreePages.get(objectNumber, objectLength)
	return
}

// `PutNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) PutNode(nodeByteSlice []byte) (objectNumber, objectOffset uint64, err error) {
	objectNumber = fetchNonce()
	objectOffset = 0
	err = bptreePages.put(objectNumber, nodeByteSlice)
	return
}

// `DiscardNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for inodeNumberToInodeStructMapStruct.
func (inodeNumberToInodeStructMap *inodeNumberToInodeStructMapStruct) DiscardNode(objectNumber, objectOffset, objectLength uint64) (err error) {
	if objectOffset != 0 {
		err = fmt.Errorf("objectOffset(%v) != 0", objectOffset)
		return
	}
	err = bptreePages.delete(objectNumber)
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
	inode.basename = string(payloadData[bytesConsumed : bytesConsumed+basenameAsByteSliceLen])
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
	if objectOffset != 0 {
		err = fmt.Errorf("objectOffset(%v) != 0", objectOffset)
		return
	}
	nodeByteSlice, err = bptreePages.get(objectNumber, objectLength)
	return
}

// `PutNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) PutNode(nodeByteSlice []byte) (objectNumber, objectOffset uint64, err error) {
	objectNumber = fetchNonce()
	objectOffset = 0
	err = bptreePages.put(objectNumber, nodeByteSlice)
	return
}

// `DiscardNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for xTimeInodeNumberSetStruct.
func (xTimeInodeNumberSet *xTimeInodeNumberSetStruct) DiscardNode(objectNumber, objectOffset, objectLength uint64) (err error) {
	if objectOffset != 0 {
		err = fmt.Errorf("objectOffset(%v) != 0", objectOffset)
		return
	}
	err = bptreePages.delete(objectNumber)
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
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) delete(parentInodeNumber uint64, childInodeBasename string) (ok bool) {
	var (
		err error
	)

	ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.DeleteByKey(uint64StringTupleToByteSlice(parentInodeNumber, childInodeBasename))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.DeleteByKey(uint64StringTupleToByteSlice(parentInodeNumber, childInodeBasename)) failed: %v", err)
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
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) getByBasename(parentInodeNumber uint64, childBasename string) (info DirEntryInfo, ok bool) {
	var (
		valueAsValue sortedmap.Value
		err          error
	)

	valueAsValue, ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByKey(uint64StringTupleToByteSlice(parentInodeNumber, childBasename))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByKey(uint64StringTupleToByteSlice(parentInodeNumber, childBasename)) failed: %v", err)
	}

	if ok {
		info, ok = valueAsValue.(DirEntryInfo)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] valueAsValue.(DirEntryInfo) returned !ok")
		}
	}

	return
}

// `getByIndex` is called to find the `inode` in `globals.{phys|virt}ChildDirEntryMap` given its global `index`.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) getByIndex(index uint64) (childBasename string, info DirEntryInfo, ok bool) {
	var (
		valueAsValue sortedmap.Value
		keyAsKey     sortedmap.Key
		err          error
	)

	keyAsKey, valueAsValue, ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByIndex(int(index))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.GetByIndex(int(index)) failed: %v", err)
	}
	if !ok {
		return
	}

	keyAsByteSlice, keyOk := keyAsKey.([]byte)
	if !keyOk {
		dumpStack()
		globals.logger.Fatalf("[FATAL] keyAsKey.([]byte) returned !ok")
	}
	_, childBasename = byteSliceToUint64StringTuple(keyAsByteSlice)

	info, ok = valueAsValue.(DirEntryInfo)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] valueAsValue.(DirEntryInfo) returned !ok")
	}

	return
}

// `getIndexRange` is called to get the (inclusive) `start` and (exclusive) `limit` indices in `globals.{phys|virt}ChildDirEntryMap` for a given `parentInode`.
// Note that the returned range is "closed" on the left (start) and "open" on the right (limit).
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) getIndexRange(parentInodeNumber uint64) (start, limit uint64) {
	var (
		err        error
		limitAsInt int
		startAsInt int
	)

	startAsInt, _, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInodeNumber, ""))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInodeNumber, \"\")) failed: %v", err)
	}
	limitAsInt, _, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInodeNumber+1, ""))
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.BisectRight(uint64StringTupleToByteSlice(parentInodeNumber+1, \"\")) failed: %v", err)
	}

	start = uint64(startAsInt)
	limit = uint64(limitAsInt)

	return
}

// `put` is called to put a `childInode` in a `parentInode's` logical `{phys|virt}ChildDirEntryMap`.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) put(parentInodeNumber uint64, childInodeBasename string, childInodeNumber uint64) (ok bool) {
	var (
		err error
	)

	info := DirEntryInfo{
		InodeNumber: childInodeNumber,
	}

	ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Put(uint64StringTupleToByteSlice(parentInodeNumber, childInodeBasename), info)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Put() failed: %v", err)
	}

	parentInodeNumberChildBasenameToChildInodeNumber.flushIfNecessary()

	return
}

// `putByKeyNoFlush` is like putByKey but skips flushIfNecessary. Use during bulk ingest.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) putByKeyNoFlush(parentInodeNumber uint64, childBasename string, info DirEntryInfo) (ok bool) {
	var (
		err error
	)

	ok, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Put(uint64StringTupleToByteSlice(parentInodeNumber, childBasename), info)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Put() failed: %v", err)
	}

	return
}

// `forceFlush` triggers an immediate flush + prune of dirty pages.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) forceFlush() {
	var err error

	_, _, _, err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Flush(false)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] bpTree.Flush(false) failed: %v", err)
	}
	err = parentInodeNumberChildBasenameToChildInodeNumber.bpTree.Prune()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] bpTree.Prune() failed: %v", err)
	}
}

// `lenForParent` returns the number of children for a given parent inode.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) lenForParent(parentInodeNumber uint64) uint64 {
	start, limit := parentInodeNumberChildBasenameToChildInodeNumber.getIndexRange(parentInodeNumber)
	return limit - start
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
		globals.logger.Fatalf("[FATAL] len(bs) < 8")
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
	info, ok := value.(DirEntryInfo)
	if !ok {
		err = errors.New("value.(DirEntryInfo) returned !ok")
		return
	}

	valueAsString = fmt.Sprintf("inode=%016X type=%d size=%d mode=%o", info.InodeNumber, info.InodeType, info.Size, info.Mode)

	err = nil
	return
}

// `GetNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) GetNode(objectNumber, objectOffset, objectLength uint64) (nodeByteSlice []byte, err error) {
	if objectOffset != 0 {
		err = fmt.Errorf("objectOffset(%v) != 0", objectOffset)
		return
	}
	nodeByteSlice, err = bptreePages.get(objectNumber, objectLength)
	return
}

// `PutNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) PutNode(nodeByteSlice []byte) (objectNumber, objectOffset uint64, err error) {
	objectNumber = fetchNonce()
	objectOffset = 0
	err = bptreePages.put(objectNumber, nodeByteSlice)
	return
}

// `DiscardNode` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) DiscardNode(objectNumber, objectOffset, objectLength uint64) (err error) {
	if objectOffset != 0 {
		err = fmt.Errorf("objectOffset(%v) != 0", objectOffset)
		return
	}
	err = bptreePages.delete(objectNumber)
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
		info DirEntryInfo
		ok   bool
	)

	info, ok = value.(DirEntryInfo)
	if !ok {
		err = errors.New("value.(DirEntryInfo) returned !ok")
		return
	}

	packedValue = make([]byte, 32)

	binary.BigEndian.PutUint64(packedValue[0:8], info.InodeNumber)
	binary.BigEndian.PutUint32(packedValue[8:12], info.InodeType)
	binary.BigEndian.PutUint64(packedValue[12:20], info.Size)
	binary.BigEndian.PutUint32(packedValue[20:24], info.Mode)
	binary.BigEndian.PutUint64(packedValue[24:32], uint64(info.MTimeUnixNano))

	err = nil
	return
}

// `UnpackValue` is here to satisfy sortedmap.BPlusTreeCallbacks interface for parentInodeNumberChildBasenameToChildInodeNumberStruct.
func (parentInodeNumberChildBasenameToChildInodeNumber *parentInodeNumberChildBasenameToChildInodeNumberStruct) UnpackValue(payloadData []byte) (value sortedmap.Value, bytesConsumed uint64, err error) {
	if len(payloadData) < 32 {
		err = errors.New("len(payloadData) < 32")
		return
	}

	value = DirEntryInfo{
		InodeNumber:   binary.BigEndian.Uint64(payloadData[0:8]),
		InodeType:     binary.BigEndian.Uint32(payloadData[8:12]),
		Size:          binary.BigEndian.Uint64(payloadData[12:20]),
		Mode:          binary.BigEndian.Uint32(payloadData[20:24]),
		MTimeUnixNano: int64(binary.BigEndian.Uint64(payloadData[24:32])),
	}
	bytesConsumed = 32

	err = nil
	return
}

// --- Sharded physChildDirEntryMap ---

const dirEntryMapShardCount = 64

type dirEntryMapShard struct {
	mu                sync.Mutex
	tree              *parentInodeNumberChildBasenameToChildInodeNumberStruct
	batchesSinceFlush uint64
}

type shardedDirEntryMap struct {
	shards [dirEntryMapShardCount]dirEntryMapShard
}

func newShardedDirEntryMap(namePrefix string, maxKeysPerNode, evictLowLimit, evictHighLimit, pageDirtyFlushTrigger, flushesSinceLastGCMax uint64) *shardedDirEntryMap {
	m := &shardedDirEntryMap{}
	for i := range m.shards {
		name := fmt.Sprintf("%s.shard%02d", namePrefix, i)
		m.shards[i].tree = parentInodeNumberChildBasenameToChildInodeNumberStructCreate(
			name, maxKeysPerNode, evictLowLimit, evictHighLimit, pageDirtyFlushTrigger, flushesSinceLastGCMax)
	}
	return m
}

func (m *shardedDirEntryMap) shardFor(parentInodeNumber uint64) *dirEntryMapShard {
	return &m.shards[parentInodeNumber%dirEntryMapShardCount]
}

func (m *shardedDirEntryMap) getByBasename(parentInodeNumber uint64, childBasename string) (info DirEntryInfo, ok bool) {
	s := m.shardFor(parentInodeNumber)
	s.mu.Lock()
	info, ok = s.tree.getByBasename(parentInodeNumber, childBasename)
	s.mu.Unlock()
	return
}

func (m *shardedDirEntryMap) put(parentInodeNumber uint64, childInodeBasename string, childInodeNumber uint64) (ok bool) {
	s := m.shardFor(parentInodeNumber)
	s.mu.Lock()
	ok = s.tree.put(parentInodeNumber, childInodeBasename, childInodeNumber)
	s.mu.Unlock()
	return
}

func (m *shardedDirEntryMap) putByKeyNoFlush(parentInodeNumber uint64, childBasename string, info DirEntryInfo) (ok bool) {
	s := m.shardFor(parentInodeNumber)
	s.mu.Lock()
	ok = s.tree.putByKeyNoFlush(parentInodeNumber, childBasename, info)
	s.mu.Unlock()
	return
}

func (m *shardedDirEntryMap) delete(parentInodeNumber uint64, childInodeBasename string) (ok bool) {
	s := m.shardFor(parentInodeNumber)
	s.mu.Lock()
	ok = s.tree.delete(parentInodeNumber, childInodeBasename)
	s.mu.Unlock()
	return
}

func (m *shardedDirEntryMap) getIndexRange(parentInodeNumber uint64) (start, limit uint64) {
	s := m.shardFor(parentInodeNumber)
	s.mu.Lock()
	start, limit = s.tree.getIndexRange(parentInodeNumber)
	s.mu.Unlock()
	return
}

func (m *shardedDirEntryMap) getByIndex(parentInodeNumber, index uint64) (childBasename string, info DirEntryInfo, ok bool) {
	s := m.shardFor(parentInodeNumber)
	s.mu.Lock()
	childBasename, info, ok = s.tree.getByIndex(index)
	s.mu.Unlock()
	return
}

func (m *shardedDirEntryMap) lenForParent(parentInodeNumber uint64) uint64 {
	s := m.shardFor(parentInodeNumber)
	s.mu.Lock()
	result := s.tree.lenForParent(parentInodeNumber)
	s.mu.Unlock()
	return result
}

func (m *shardedDirEntryMap) forceFlush() {
	for i := range m.shards {
		m.shards[i].mu.Lock()
		m.shards[i].tree.forceFlush()
		m.shards[i].mu.Unlock()
	}
}

func (m *shardedDirEntryMap) discard() {
	for i := range m.shards {
		m.shards[i].tree.discard()
	}
}

func (m *shardedDirEntryMap) setPageDirtyFlushTrigger(value uint64) {
	for i := range m.shards {
		m.shards[i].tree.pageDirtyFlushTrigger = value
	}
}

func (m *shardedDirEntryMap) getPageDirtyFlushTrigger() uint64 {
	return m.shards[0].tree.pageDirtyFlushTrigger
}

func (m *shardedDirEntryMap) setFlushesSinceLastGCMax(value uint64) {
	for i := range m.shards {
		m.shards[i].tree.flushesSinceLastGCMax = value
	}
}

func (m *shardedDirEntryMap) getFlushesSinceLastGCMax() uint64 {
	return m.shards[0].tree.flushesSinceLastGCMax
}

// --- Sharded inodeMap ---

const inodeMapShardCount = 64

type inodeMapShard struct {
	mu   sync.Mutex
	tree *inodeNumberToInodeStructMapStruct
}

type shardedInodeMap struct {
	shards [inodeMapShardCount]inodeMapShard
}

func newShardedInodeMap(namePrefix string, maxKeysPerNode, evictLowLimit, evictHighLimit, pageDirtyFlushTrigger, flushesSinceLastGCMax uint64) *shardedInodeMap {
	m := &shardedInodeMap{}
	for i := range m.shards {
		name := fmt.Sprintf("%s.shard%02d", namePrefix, i)
		m.shards[i].tree = inodeNumberToInodeStructMapStructCreate(
			name, maxKeysPerNode, evictLowLimit, evictHighLimit, pageDirtyFlushTrigger, flushesSinceLastGCMax)
	}
	return m
}

func (m *shardedInodeMap) shardFor(inodeNumber uint64) *inodeMapShard {
	return &m.shards[inodeNumber%inodeMapShardCount]
}

func (m *shardedInodeMap) get(inodeNumber uint64) (inode *inodeStruct, ok bool) {
	s := m.shardFor(inodeNumber)
	s.mu.Lock()
	inode, ok = s.tree.get(inodeNumber)
	s.mu.Unlock()
	return
}

func (m *shardedInodeMap) put(inode *inodeStruct) (ok bool) {
	s := m.shardFor(inode.inodeNumber)
	s.mu.Lock()
	ok = s.tree.put(inode)
	s.mu.Unlock()
	return
}

func (m *shardedInodeMap) touch(inode *inodeStruct) (ok bool) {
	s := m.shardFor(inode.inodeNumber)
	s.mu.Lock()
	ok = s.tree.touch(inode)
	s.mu.Unlock()
	return
}

func (m *shardedInodeMap) delete(inodeNumber uint64) (ok bool) {
	s := m.shardFor(inodeNumber)
	s.mu.Lock()
	ok = s.tree.delete(inodeNumber)
	s.mu.Unlock()
	return
}

func (m *shardedInodeMap) len() (totalLen int) {
	for i := range m.shards {
		m.shards[i].mu.Lock()
		totalLen += m.shards[i].tree.len()
		m.shards[i].mu.Unlock()
	}
	return
}

func (m *shardedInodeMap) forceFlush() {
	for i := range m.shards {
		m.shards[i].mu.Lock()
		var err error
		_, _, _, err = m.shards[i].tree.bpTree.Flush(false)
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] shardedInodeMap shard %d Flush failed: %v", i, err)
		}
		err = m.shards[i].tree.bpTree.Prune()
		if err != nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] shardedInodeMap shard %d Prune failed: %v", i, err)
		}
		m.shards[i].mu.Unlock()
	}
}

func (m *shardedInodeMap) discard() {
	for i := range m.shards {
		m.shards[i].tree.discard()
	}
}

func (m *shardedInodeMap) setPageDirtyFlushTrigger(value uint64) {
	for i := range m.shards {
		m.shards[i].tree.pageDirtyFlushTrigger = value
	}
}

func (m *shardedInodeMap) getPageDirtyFlushTrigger() uint64 {
	return m.shards[0].tree.pageDirtyFlushTrigger
}

func (m *shardedInodeMap) setFlushesSinceLastGCMax(value uint64) {
	for i := range m.shards {
		m.shards[i].tree.flushesSinceLastGCMax = value
	}
}

func (m *shardedInodeMap) getFlushesSinceLastGCMax() uint64 {
	return m.shards[0].tree.flushesSinceLastGCMax
}
