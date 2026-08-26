package main

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"syscall"
	"time"
)

type writeSegment struct {
	offset uint64
	data   []byte
}

type writeRange struct {
	start uint64
	end   uint64
}

type writePart struct {
	data     []byte
	ranges   []writeRange
	filled   uint64
	length   uint64
	uploaded bool
}

type writeState struct {
	segments             []writeSegment
	parts                map[int32]writePart
	stream               s3WriteStream
	streamActive         bool
	streaming            bool
	uploadedParts        map[int32]bool
	truncateAtOpen       bool
	deferredBytesCounted uint64 // Bytes this inode currently contributes to globals.deferredWriteBytes (0 once promoted/committed)
}

func (inode *inodeStruct) ensureWriteFileLocked(backend *backendStruct, truncate bool) (err error) {
	inode.writeStateActive = true
	if truncate {
		inode.clearDeferredAccountingLocked()
		noteWriteMutationLocked(inode)
		inode.sizeInMemory = 0
		inode.writeDirty = true
		inode.writeState.truncateAtOpen = true
		inode.writeState.segments = nil
		inode.writeState.parts = nil
		inode.writeState.uploadedParts = nil
		inode.mTime = time.Now()
		inode.touch(nil)
		inode.updateParentDirEntryLocked()
	}
	// New S3 objects start in a deferred (buffered) state: no multipart upload
	// is opened until the buffered size crosses the configured threshold, so
	// small files can commit via a single PutObject. See maybePromoteToStreamLocked.
	return nil
}

func (inode *inodeStruct) writeBytesLocked(backend *backendStruct, fh *fhStruct, requestedOffset uint64, data []byte) (errno syscall.Errno) {
	var (
		offset      uint64
		offsetLimit uint64
	)

	inode.writeStateActive = true

	if fh.appendWrites {
		offset = inode.sizeInMemory
	} else {
		offset = requestedOffset
	}
	offsetLimit = offset + uint64(len(data))

	if backend.backendType == "S3" && inode.sizeInBackend == 0 {
		if inode.writeState.streaming {
			if inode.writeState.overlapsUploadedPart(offset, uint64(len(data))) {
				return syscall.ENOTSUP
			}
			inode.addWritePartLocked(offset, data)
		} else {
			inode.addWriteSegmentLocked(offset, data)
		}
	} else {
		inode.addWriteSegmentLocked(offset, data)
	}

	if offsetLimit > inode.sizeInMemory {
		inode.sizeInMemory = offsetLimit
	}
	if backend.backendType == "S3" && inode.sizeInBackend == 0 && !inode.writeState.streaming {
		inode.maybePromoteToStreamLocked(backend)
	}
	if inode.writeState.streaming {
		inode.queueCompleteStreamPartsLocked()
	}
	inode.writeDirty = true
	noteWriteMutationLocked(inode)
	inode.mTime = time.Now()
	inode.touch(nil)
	inode.updateParentDirEntryLocked()
	return 0
}

func (inode *inodeStruct) addWriteSegmentLocked(offset uint64, data []byte) {
	inode.writeState.segments = append(inode.writeState.segments, writeSegment{
		offset: offset,
		data:   append([]byte(nil), data...),
	})
}

// `shouldPromoteDeferredLocked` reports whether a deferred (buffered) new S3
// object should be promoted to a streaming multipart upload. Promotion happens
// once the buffered size reaches the per-backend threshold, or when the global
// deferred-write budget would be exceeded. A threshold of 0 promotes on the
// first byte (deferral disabled).
func (inode *inodeStruct) shouldPromoteDeferredLocked(backend *backendStruct) bool {
	if inode.sizeInMemory >= backend.multipartUploadThreshold {
		return true
	}
	if globals.config.writeDeferralMaxBytes != 0 {
		projected := globals.deferredWriteBytes - inode.writeState.deferredBytesCounted + inode.sizeInMemory
		if projected > globals.config.writeDeferralMaxBytes {
			return true
		}
	}
	return false
}

// `maybePromoteToStreamLocked` promotes a deferred new S3 object to a streaming
// multipart upload when warranted; otherwise it keeps buffering and updates the
// global deferred-write accounting.
func (inode *inodeStruct) maybePromoteToStreamLocked(backend *backendStruct) {
	if inode.shouldPromoteDeferredLocked(backend) {
		inode.promoteToStreamLocked(backend)
	}
	if !inode.writeState.streaming {
		inode.setDeferredBytesLocked(inode.sizeInMemory)
	}
}

// `promoteToStreamLocked` opens the S3 multipart upload for a new object and
// converts any already-buffered segments into part buffers. Unlike the deferred
// buffering path, this intentionally proceeds even when segments are present.
func (inode *inodeStruct) promoteToStreamLocked(backend *backendStruct) {
	inode.writeStateActive = true
	state := &inode.writeState
	if state.streaming || backend.backendType != "S3" || inode.sizeInBackend != 0 {
		return
	}
	if err := state.stream.init(backend, inode.objectPath); err != nil {
		globals.logger.Printf("[WARN] unable to start S3 multipart write stream for %q; using in-memory dirty ranges: %v", inode.objectPath, err)
		return
	}
	state.streamActive = true
	state.streaming = true
	if state.uploadedParts == nil {
		state.uploadedParts = make(map[int32]bool)
	}
	if state.parts == nil {
		state.parts = make(map[int32]writePart)
	}
	segments := state.segments
	state.segments = nil
	for _, segment := range segments {
		inode.addWritePartLocked(segment.offset, segment.data)
	}
	inode.clearDeferredAccountingLocked()
}

// `setDeferredBytesLocked` records this inode's current contribution to the
// global deferred-write budget. Caller must hold globals.Lock().
func (inode *inodeStruct) setDeferredBytesLocked(n uint64) {
	globals.deferredWriteBytes = globals.deferredWriteBytes - inode.writeState.deferredBytesCounted + n
	inode.writeState.deferredBytesCounted = n
}

// `clearDeferredAccountingLocked` removes this inode's contribution from the
// global deferred-write budget. Idempotent. Caller must hold globals.Lock().
func (inode *inodeStruct) clearDeferredAccountingLocked() {
	if inode.writeState.deferredBytesCounted == 0 {
		return
	}
	if globals.deferredWriteBytes >= inode.writeState.deferredBytesCounted {
		globals.deferredWriteBytes -= inode.writeState.deferredBytesCounted
	} else {
		globals.deferredWriteBytes = 0
	}
	inode.writeState.deferredBytesCounted = 0
}

func (inode *inodeStruct) addWritePartLocked(offset uint64, data []byte) {
	state := &inode.writeState
	partSize := state.stream.partSize
	dataOffset := uint64(0)
	for dataOffset < uint64(len(data)) {
		absoluteOffset := offset + dataOffset
		partIndex := absoluteOffset / partSize
		partNumber := int32(partIndex + 1)
		partOffset := absoluteOffset - partIndex*partSize
		copyLen := uint64(len(data)) - dataOffset
		if partOffset+copyLen > partSize {
			copyLen = partSize - partOffset
		}

		part := state.parts[partNumber]
		if part.data == nil {
			part.data = make([]byte, partSize)
			part.length = partSize
		}
		copy(part.data[partOffset:partOffset+copyLen], data[dataOffset:dataOffset+copyLen])
		part.addRange(partOffset, partOffset+copyLen)
		state.parts[partNumber] = part
		dataOffset += copyLen
	}
}

func (inode *inodeStruct) readWriteFileLocked(backend *backendStruct, offset uint64, size uint32) (data []byte, err error) {
	var (
		limit uint64
	)

	if !inode.writeStateActive || offset >= inode.sizeInMemory || size == 0 {
		return make([]byte, 0), nil
	}

	limit = offset + uint64(size)
	if limit > inode.sizeInMemory {
		limit = inode.sizeInMemory
	}

	data = make([]byte, limit-offset)

	if inode.sizeInBackend > 0 && !inode.writeState.truncateAtOpen && offset < inode.sizeInBackend {
		readLimit := limit
		if readLimit > inode.sizeInBackend {
			readLimit = inode.sizeInBackend
		}
		for cacheLineNumber := offset / globals.config.cacheLineSize; cacheLineNumber*globals.config.cacheLineSize < readLimit; cacheLineNumber++ {
			readOutput, readErr := readFileWrapper(backend.context, &readFileInputStruct{
				filePath:        inode.objectPath,
				offsetCacheLine: cacheLineNumber,
				ifMatch:         inode.eTag,
			})
			if readErr != nil {
				return nil, readErr
			}
			cacheLineOffset := cacheLineNumber * globals.config.cacheLineSize
			overlayBytes(data, offset, writeSegment{offset: cacheLineOffset, data: readOutput.buf})
		}
	}
	for _, segment := range inode.writeState.segments {
		overlayBytes(data, offset, segment)
	}
	return
}

func overlayBytes(dst []byte, dstOffset uint64, segment writeSegment) {
	dstLimit := dstOffset + uint64(len(dst))
	segLimit := segment.offset + uint64(len(segment.data))
	if segLimit <= dstOffset || segment.offset >= dstLimit {
		return
	}
	copyStart := max(dstOffset, segment.offset)
	copyLimit := min(dstLimit, segLimit)
	copy(dst[copyStart-dstOffset:copyLimit-dstOffset], segment.data[copyStart-segment.offset:copyLimit-segment.offset])
}

func (inode *inodeStruct) flushWriteFileLocked(backend *backendStruct) (err error) {
	var (
		writeOutput *writeFileOutputStruct
	)

	if !inode.writeStateActive || !inode.writeDirty {
		return nil
	}

	if inode.writeState.streaming {
		inode.queueAllStreamPartsLocked()
		writeOutput, err = inode.writeState.stream.complete(nil, inode.sizeInMemory)
		if err != nil {
			return err
		}
		inode.applyWriteOutputLocked(backend, writeOutput)
		promoteCommittedCacheLocked(inode, writeOutput.eTag, writeOutput.size, &writePartsReaderAt{
			parts:    inode.writeState.parts,
			partSize: inode.writeState.stream.partSize,
			size:     writeOutput.size,
		}, nil)
		inode.clearDeferredAccountingLocked()
		inode.writeState = writeState{}
		inode.writeStateActive = false
		clearWriteCommitControlLocked(inode.inodeNumber)
		return nil
	}

	// New S3 object that never crossed the multipart threshold: commit the
	// buffered bytes with a single PutObject. Routing this through
	// writeFileOverlay would open a multipart upload and defeat the deferral.
	if backend.backendType == "S3" && inode.sizeInBackend == 0 {
		var body []byte
		body, err = inode.materializeWriteStateLocked(backend)
		if err != nil {
			return err
		}
		writeOutput, err = writeFileWrapper(backend.context, &writeFileInputStruct{
			filePath:       inode.objectPath,
			ifMatch:        inode.eTag,
			body:           bytes.NewReader(body),
			readerAt:       bytes.NewReader(body),
			size:           inode.sizeInMemory,
			forceSinglePut: true,
		})
		if err != nil {
			return err
		}
		inode.applyWriteOutputLocked(backend, writeOutput)
		promoteCommittedCacheLocked(inode, writeOutput.eTag, writeOutput.size, bytes.NewReader(body), nil)
		inode.clearDeferredAccountingLocked()
		inode.writeState = writeState{}
		inode.writeStateActive = false
		clearWriteCommitControlLocked(inode.inodeNumber)
		return nil
	}

	if backend.backendType == "S3" {
		if s3Context, ok := backend.context.(*s3ContextStruct); ok {
			writeOutput, err = s3Context.writeFileOverlay(inode)
			if err != nil {
				return err
			}
			inode.applyWriteOutputLocked(backend, writeOutput)
			promoteCommittedCacheLocked(
				inode,
				writeOutput.eTag,
				writeOutput.size,
				&writeSegmentsReaderAt{segments: inode.writeState.segments, size: writeOutput.size},
				completeDirtyCacheLineNumbers(&inode.writeState, writeOutput.size, globals.config.cacheLineSize),
			)
			inode.clearDeferredAccountingLocked()
			inode.writeState = writeState{}
			inode.writeStateActive = false
			clearWriteCommitControlLocked(inode.inodeNumber)
			return nil
		}
	}

	body, err := inode.materializeWriteStateLocked(backend)
	if err != nil {
		return err
	}
	writeOutput, err = writeFileWrapper(backend.context, &writeFileInputStruct{
		filePath: inode.objectPath,
		ifMatch:  inode.eTag,
		body:     bytes.NewReader(body),
		readerAt: bytes.NewReader(body),
		size:     inode.sizeInMemory,
	})
	if err != nil {
		return err
	}
	inode.applyWriteOutputLocked(backend, writeOutput)
	promoteCommittedCacheLocked(inode, writeOutput.eTag, writeOutput.size, bytes.NewReader(body), nil)
	inode.clearDeferredAccountingLocked()
	inode.writeState = writeState{}
	inode.writeStateActive = false
	clearWriteCommitControlLocked(inode.inodeNumber)
	return nil
}

func (inode *inodeStruct) applyWriteOutputLocked(backend *backendStruct, writeOutput *writeFileOutputStruct) {
	inode.eTag = writeOutput.eTag
	inode.sizeInBackend = writeOutput.size
	inode.sizeInMemory = writeOutput.size
	if writeOutput.mTime.IsZero() {
		inode.mTime = time.Now()
	} else {
		inode.mTime = writeOutput.mTime
	}
	inode.writeDirty = false
	inode.invalidateCleanCacheLinesLocked()
	inode.touch(nil)
	inode.updateParentDirEntryLocked()
	if backend != nil {
		appendManifestDeltaForInodeLocked(backend, inode, manifestDeltaUpsert)
	}
}

func (inode *inodeStruct) invalidateCleanCacheLinesLocked() {
	if inode.cacheMap == nil {
		return
	}
	for cacheLineNumber, dataCacheLineNumber := range inode.cacheMap {
		if dataCacheLineNumber >= uint64(len(globals.dataCacheLinesTracker)) {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inode.cacheMap[cacheLineNumber] returned out-of-range dataCacheLineNumber")
		}
		dataCacheLineTracker := &globals.dataCacheLinesTracker[dataCacheLineNumber]
		switch dataCacheLineTracker.state {
		case CacheLineClean:
			delete(inode.cacheMap, cacheLineNumber)
			globals.dataCacheLineCleanLRU.popThis(dataCacheLineTracker)
			dataCacheLineTracker.free()
		case CacheLineInbound:
			// Detach ownership but leave the fetch running. Its completion
			// validates the reciprocal map entry and discards stale data.
			delete(inode.cacheMap, cacheLineNumber)
		}
	}
}

func (inode *inodeStruct) materializeWriteStateLocked(backend *backendStruct) ([]byte, error) {
	if inode.sizeInMemory > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("object too large to materialize in memory: %d bytes", inode.sizeInMemory)
	}

	body := make([]byte, inode.sizeInMemory)
	if inode.sizeInBackend > 0 && !inode.writeState.truncateAtOpen {
		for cacheLineNumber := uint64(0); cacheLineNumber*globals.config.cacheLineSize < inode.sizeInBackend && cacheLineNumber*globals.config.cacheLineSize < inode.sizeInMemory; cacheLineNumber++ {
			readOutput, err := readFileWrapper(backend.context, &readFileInputStruct{
				filePath:        inode.objectPath,
				offsetCacheLine: cacheLineNumber,
				ifMatch:         inode.eTag,
			})
			if err != nil {
				return nil, err
			}
			copy(body[cacheLineNumber*globals.config.cacheLineSize:], readOutput.buf)
		}
	}
	for _, segment := range inode.writeState.segments {
		overlayBytes(body, 0, segment)
	}
	return body, nil
}

func (inode *inodeStruct) closeWriteFileLocked() {
	if inode.writeStateActive && inode.writeState.streamActive {
		if err := inode.writeState.stream.abort(); err != nil {
			globals.logger.Printf("[WARN] abort S3 multipart write stream for %q failed during close: %v", inode.objectPath, err)
		}
	}
	inode.clearDeferredAccountingLocked()
	inode.writeState = writeState{}
	inode.writeStateActive = false
	inode.writeDirty = false
	clearWriteCommitControlLocked(inode.inodeNumber)
}

func flushWriteFileAsync(inodeNumber, backendNonce uint64) {
	go func() {
		globalsLock("write.go:454:3:funcLit@453")
		inode, ok := globals.inodeMap.get(inodeNumber)
		if !ok {
			globalsUnlock()
			return
		}
		backend, ok := globals.backendMap[backendNonce]
		if !ok {
			globalsUnlock()
			return
		}
		if inode.pendingDelete || !inode.writeDirty {
			globalsUnlock()
			return
		}
		if err := inode.flushWriteFileLocked(backend); err != nil {
			// close(2) already reported success, so latch the failure for the next
			// durability barrier rather than leaving the log as its only record.
			writeCommitControlLocked(inodeNumber).err = err
			globals.logger.Printf("[WARN] async release flush failed for %q: %s", inode.objectPath, redactSecrets(backend, err.Error()))
		}
		globalsUnlock()
	}()
}

func (state *writeState) hasDirtyOverlap(offset, length uint64) bool {
	limit := offset + length
	for _, segment := range state.segments {
		segmentLimit := segment.offset + uint64(len(segment.data))
		if segmentLimit > offset && segment.offset < limit {
			return true
		}
	}
	return false
}

// `mergedDirtyRanges` returns the dirty byte ranges in ascending order, with
// overlapping and adjacent ranges combined. `addWriteSegmentLocked` appends one
// segment per FUSE write and never coalesces, so a full rewrite accumulates a
// segment per write; merging once per flush keeps a per-part coverage test from
// rescanning that whole list.
func (state *writeState) mergedDirtyRanges() []writeRange {
	sorted := make([]writeRange, 0, len(state.segments))
	for _, segment := range state.segments {
		if len(segment.data) == 0 {
			continue
		}
		sorted = append(sorted, writeRange{
			start: segment.offset,
			end:   segment.offset + uint64(len(segment.data)),
		})
	}
	if len(sorted) == 0 {
		return nil
	}
	slices.SortFunc(sorted, func(a, b writeRange) int {
		return cmp.Compare(a.start, b.start)
	})

	merged := make([]writeRange, 0, len(sorted))
	merged = append(merged, sorted[0])
	for _, candidate := range sorted[1:] {
		last := &merged[len(merged)-1]
		if candidate.start > last.end {
			merged = append(merged, candidate)
			continue
		}
		if candidate.end > last.end {
			last.end = candidate.end
		}
	}
	return merged
}

// `rangesCover` reports whether ranges, which must be sorted and merged, span
// every byte of [offset, offset+length). A false negative only costs a read, so
// this stays conservative: an empty range list covers nothing.
func rangesCover(ranges []writeRange, offset, length uint64) bool {
	if length == 0 {
		return true
	}
	limit := offset + length
	for _, candidate := range ranges {
		if candidate.start > offset {
			return false
		}
		if candidate.end > offset {
			offset = candidate.end
			if offset >= limit {
				return true
			}
		}
	}
	return false
}

func (state *writeState) overlapsUploadedPart(offset, length uint64) bool {
	if length == 0 || !state.streaming || state.stream.partSize == 0 {
		return false
	}
	firstPart := offset / state.stream.partSize
	lastPart := (offset + length - 1) / state.stream.partSize
	for partIndex := firstPart; partIndex <= lastPart; partIndex++ {
		if state.uploadedParts[int32(partIndex+1)] {
			return true
		}
	}
	return false
}

func (inode *inodeStruct) queueCompleteStreamPartsLocked() {
	state := &inode.writeState
	if !state.streaming || state.stream.partSize == 0 || inode.sizeInMemory == 0 {
		return
	}
	if state.uploadedParts == nil {
		state.uploadedParts = make(map[int32]bool)
	}
	for partNumber, part := range state.parts {
		if state.uploadedParts[partNumber] || part.length < state.stream.partSize || part.filled < part.length {
			continue
		}
		state.stream.queuePartNumber(partNumber, part.data[:part.length])
		part.uploaded = true
		state.parts[partNumber] = part
		state.uploadedParts[partNumber] = true
	}
}

func (inode *inodeStruct) queueAllStreamPartsLocked() {
	state := &inode.writeState
	if !state.streaming || state.stream.partSize == 0 || inode.sizeInMemory == 0 {
		return
	}
	partSize := state.stream.partSize
	partCount := (inode.sizeInMemory + partSize - 1) / partSize
	for partIndex := range partCount {
		partNumber := int32(partIndex + 1)
		if state.uploadedParts[partNumber] {
			continue
		}
		length := partSize
		if (partIndex+1)*partSize > inode.sizeInMemory {
			length = inode.sizeInMemory - partIndex*partSize
		}
		part := state.parts[partNumber]
		if part.data == nil {
			part.data = make([]byte, partSize)
			part.length = length
		}
		if part.length < length {
			part.length = length
		}
		if part.filled < length {
			// Missing bytes are zero-filled. That matches sparse extension
			// semantics for new objects and avoids serial segment assembly.
		}
		state.stream.queuePartNumber(partNumber, part.data[:length])
		part.uploaded = true
		state.parts[partNumber] = part
		state.uploadedParts[partNumber] = true
	}
}

func (state *writeState) dropSegmentsForRange(offset, length uint64) {
	limit := offset + length
	kept := state.segments[:0]
	for _, segment := range state.segments {
		segmentEnd := segment.offset + uint64(len(segment.data))
		if segment.offset >= offset && segmentEnd <= limit {
			continue
		}
		kept = append(kept, segment)
	}
	state.segments = kept
}

func (part *writePart) addRange(start, end uint64) {
	if end <= start {
		return
	}
	part.ranges = append(part.ranges, writeRange{start: start, end: end})
	ranges := part.ranges
	for i := 1; i < len(ranges); i++ {
		current := ranges[i]
		j := i - 1
		for ; j >= 0 && ranges[j].start > current.start; j-- {
			ranges[j+1] = ranges[j]
		}
		ranges[j+1] = current
	}

	merged := ranges[:0]
	for _, r := range ranges {
		if len(merged) == 0 || r.start > merged[len(merged)-1].end {
			merged = append(merged, r)
			continue
		}
		if r.end > merged[len(merged)-1].end {
			merged[len(merged)-1].end = r.end
		}
	}
	part.ranges = merged

	var filled uint64
	for _, r := range merged {
		filled += r.end - r.start
	}
	part.filled = filled
}
