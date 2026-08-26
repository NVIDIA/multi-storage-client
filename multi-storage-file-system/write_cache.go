package main

import (
	"io"
	"sort"
	"sync"
)

type writeCachePromotionLine struct {
	buf           []byte
	contentLength uint64
	lineNumber    uint64
	readErr       error
	readLength    int
	trackerNumber uint64
}

type writeCachePromotionJob struct {
	inodeNumber uint64
	lines       []writeCachePromotionLine
	source      io.ReaderAt
}

type writeCachePromotionPool struct {
	jobs     chan *writeCachePromotionJob
	workerWG sync.WaitGroup
	closeMu  sync.RWMutex
	closed   bool
}

func newWriteCachePromotionPool(workerCount, queueDepth uint64) *writeCachePromotionPool {
	pool := &writeCachePromotionPool{
		jobs: make(chan *writeCachePromotionJob, int(queueDepth)),
	}
	for range workerCount {
		pool.workerWG.Add(1)
		go func() {
			defer pool.workerWG.Done()
			for job := range pool.jobs {
				job.run()
			}
		}()
	}
	return pool
}

// trySubmit never blocks, but it still races drainFS closing the channel, and it
// registers with dataCacheActivityWG -- which drainFS waits on immediately after
// draining, so an Add arriving after that Wait begins is a second panic. The read
// lock covers both.
func (pool *writeCachePromotionPool) trySubmit(job *writeCachePromotionJob) bool {
	if pool == nil {
		return false
	}
	pool.closeMu.RLock()
	defer pool.closeMu.RUnlock()
	if pool.closed {
		return false
	}
	globals.dataCacheActivityWG.Add(1)
	select {
	case pool.jobs <- job:
		return true
	default:
		globals.dataCacheActivityWG.Done()
		return false
	}
}

func (pool *writeCachePromotionPool) drain() {
	if pool == nil {
		return
	}
	pool.closeMu.Lock()
	if !pool.closed {
		pool.closed = true
		close(pool.jobs)
	}
	pool.closeMu.Unlock()
	pool.workerWG.Wait()
}

type writePartsReaderAt struct {
	parts    map[int32]writePart
	partSize uint64
	size     uint64
}

func (reader *writePartsReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) >= reader.size {
		return 0, io.EOF
	}
	written := 0
	for written < len(dst) && uint64(offset)+uint64(written) < reader.size {
		absoluteOffset := uint64(offset) + uint64(written)
		partIndex := absoluteOffset / reader.partSize
		part := reader.parts[int32(partIndex+1)]
		partOffset := absoluteOffset - partIndex*reader.partSize
		if part.data == nil || partOffset >= uint64(len(part.data)) {
			return written, io.ErrUnexpectedEOF
		}
		copyLength := min(
			uint64(len(dst)-written),
			uint64(len(part.data))-partOffset,
			reader.size-absoluteOffset,
		)
		copy(dst[written:written+int(copyLength)], part.data[partOffset:partOffset+copyLength])
		written += int(copyLength)
	}
	if written < len(dst) {
		return written, io.EOF
	}
	return written, nil
}

type writeSegmentsReaderAt struct {
	segments []writeSegment
	size     uint64
}

func (reader *writeSegmentsReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) >= reader.size {
		return 0, io.EOF
	}
	length := min(uint64(len(dst)), reader.size-uint64(offset))
	clear(dst[:length])
	for _, segment := range reader.segments {
		overlayBytes(dst[:length], uint64(offset), segment)
	}
	if length < uint64(len(dst)) {
		return int(length), io.EOF
	}
	return int(length), nil
}

func (job *writeCachePromotionJob) run() {
	defer globals.dataCacheActivityWG.Done()

	for index := range job.lines {
		line := &job.lines[index]
		if globals.config.cacheStorage == cacheStoragePerInodeFile {
			line.buf = make([]byte, line.contentLength)
		} else {
			tracker := &globals.dataCacheLinesTracker[line.trackerNumber]
			line.buf = globals.dataCacheLinesContent[tracker.contentStart : tracker.contentStart+line.contentLength]
		}
		line.readLength, line.readErr = job.source.ReadAt(line.buf, int64(line.lineNumber*globals.config.cacheLineSize))
	}

	globalsLock("write_cache.go:150:2:(*writeCachePromotionJob).run")
	inode, inodeOK := globals.inodeMap.get(job.inodeNumber)
	for index := range job.lines {
		line := &job.lines[index]
		tracker := &globals.dataCacheLinesTracker[line.trackerNumber]
		if tracker.state != CacheLineInbound {
			continue
		}
		globals.dataCacheLineInboundLRU.popThis(tracker)
		if inodeOK && inode.inboundCacheLineCount > 0 {
			inode.inboundCacheLineCount--
		}

		mappedTracker, mapped := uint64(0), false
		if inodeOK {
			mappedTracker, mapped = inode.cacheMap[line.lineNumber]
		}
		ownsLine := mapped && mappedTracker == tracker.pos
		readOK := uint64(line.readLength) == line.contentLength && (line.readErr == nil || line.readErr == io.EOF)
		if ownsLine && readOK {
			if globals.config.cacheStorage == cacheStoragePerInodeFile {
				tracker.contentLength = tracker.storeContentDisk(line.buf)
			} else {
				tracker.contentLength = uint64(line.readLength)
			}
		}

		tracker.contentGeneration.Add(1)
		if !ownsLine || tracker.contentLength != line.contentLength {
			if ownsLine {
				delete(inode.cacheMap, line.lineNumber)
			}
			tracker.notifyWaiters()
			tracker.free()
			continue
		}
		globals.dataCacheLineCleanLRU.pushTail(tracker)
		tracker.notifyWaiters()
	}
	if inodeOK {
		inode.touch(nil)
	}
	globalsUnlock()
}

func tryAllocateDataCacheLineLocked() *dataCacheLineTrackerStruct {
	tracker := globals.dataCacheLineFreeLRU.popHead()
	if tracker == nil {
		tracker = globals.dataCacheLineCleanLRU.popHead()
		if tracker == nil {
			return nil
		}
		owner, ok := globals.inodeMap.get(tracker.inodeNumber)
		if ok && owner.cacheMap[tracker.lineNumber] == tracker.pos {
			delete(owner.cacheMap, tracker.lineNumber)
			owner.touch(nil)
		}
	}

	if globals.config.cacheStorage == cacheStoragePerInodeFile {
		tracker.punchHoleDisk()
	}
	tracker.contentLength = 0
	tracker.contentGeneration.Add(1)
	tracker.inodeNumber = 0
	tracker.lineNumber = 0
	tracker.eTag = ""
	tracker.fetchFailed = false
	tracker.waiters = make([]*sync.WaitGroup, 0, 1)
	return tracker
}

func promoteCommittedCacheLocked(inode *inodeStruct, eTag string, size uint64, source io.ReaderAt, lineNumbers []uint64) {
	if !globals.config.writeCachePromotion || source == nil || size == 0 || globals.config.cacheLines == 0 {
		return
	}

	admissionLimit := globals.dataCacheLineFreeLRU.lruCount + globals.dataCacheLineCleanLRU.lruCount
	if admissionLimit == 0 {
		return
	}

	cacheLineSize := globals.config.cacheLineSize
	if lineNumbers == nil {
		lineCount := (size + cacheLineSize - 1) / cacheLineSize
		lineCount = min(lineCount, admissionLimit)
		lineNumbers = make([]uint64, lineCount)
		for lineNumber := range lineCount {
			lineNumbers[lineNumber] = lineNumber
		}
	} else {
		sort.Slice(lineNumbers, func(i, j int) bool { return lineNumbers[i] < lineNumbers[j] })
		if uint64(len(lineNumbers)) > admissionLimit {
			lineNumbers = lineNumbers[:admissionLimit]
		}
	}

	if inode.cacheMap == nil {
		inode.cacheMap = make(map[uint64]uint64)
	}
	job := &writeCachePromotionJob{
		inodeNumber: inode.inodeNumber,
		lines:       make([]writeCachePromotionLine, 0, len(lineNumbers)),
		source:      source,
	}
	for _, lineNumber := range lineNumbers {
		lineOffset := lineNumber * cacheLineSize
		if lineOffset >= size {
			continue
		}
		contentLength := min(cacheLineSize, size-lineOffset)
		tracker := tryAllocateDataCacheLineLocked()
		if tracker == nil {
			break
		}

		tracker.inodeNumber = inode.inodeNumber
		tracker.lineNumber = lineNumber
		tracker.eTag = eTag
		inode.cacheMap[lineNumber] = tracker.pos
		inode.inboundCacheLineCount++
		globals.dataCacheLineInboundLRU.pushTail(tracker)
		job.lines = append(job.lines, writeCachePromotionLine{
			contentLength: contentLength,
			lineNumber:    lineNumber,
			trackerNumber: tracker.pos,
		})
	}
	if len(job.lines) == 0 {
		return
	}
	if !globals.writeCachePromotionPool.trySubmit(job) {
		for _, line := range job.lines {
			tracker := &globals.dataCacheLinesTracker[line.trackerNumber]
			if tracker.state == CacheLineInbound {
				globals.dataCacheLineInboundLRU.popThis(tracker)
				if inode.inboundCacheLineCount > 0 {
					inode.inboundCacheLineCount--
				}
			}
			if inode.cacheMap[line.lineNumber] == tracker.pos {
				delete(inode.cacheMap, line.lineNumber)
			}
			tracker.notifyWaiters()
			tracker.free()
		}
	}
	inode.touch(nil)
}

func completeDirtyCacheLineNumbers(state *writeState, size, cacheLineSize uint64) []uint64 {
	if state == nil || size == 0 {
		return nil
	}
	candidates := make(map[uint64]struct{})
	for _, segment := range state.segments {
		if len(segment.data) == 0 {
			continue
		}
		first := segment.offset / cacheLineSize
		last := (segment.offset + uint64(len(segment.data)) - 1) / cacheLineSize
		for lineNumber := first; lineNumber <= last; lineNumber++ {
			candidates[lineNumber] = struct{}{}
		}
	}

	complete := make([]uint64, 0, len(candidates))
	for lineNumber := range candidates {
		lineStart := lineNumber * cacheLineSize
		lineEnd := min(lineStart+cacheLineSize, size)
		var coverage writePart
		for _, segment := range state.segments {
			segmentEnd := segment.offset + uint64(len(segment.data))
			start := max(lineStart, segment.offset)
			end := min(lineEnd, segmentEnd)
			if end > start {
				coverage.addRange(start-lineStart, end-lineStart)
			}
		}
		if coverage.filled == lineEnd-lineStart {
			complete = append(complete, lineNumber)
		}
	}
	sort.Slice(complete, func(i, j int) bool { return complete[i] < complete[j] })
	return complete
}
