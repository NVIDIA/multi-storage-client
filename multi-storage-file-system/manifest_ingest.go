package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultIngestBatchSize     = 5000
	defaultIngestFlushTrigger  = 1000
	defaultIngestFlushInterval = 10
	defaultIngestNumReaders    = 16
	defaultIngestNumWorkers    = 8
)

type ingestStats struct {
	filesIngested    int64
	dirsCreated      int64
	errors           int64
	batchesProcessed int64
	startTime        time.Time
	lastLogNano      int64 // atomic — UnixNano of last progress log
	lastLogCount     int64 // atomic — filesIngested at last progress log
}

type dirBatch struct {
	parentPath string
	entries    []manifestEntry
}

// `ingestTuneForBulk` overrides B+Tree flush/GC thresholds for bulk ingest performance
// and returns a cleanup function that restores the original values.
func ingestTuneForBulk() (restore func()) {
	savedPhysDirtyFlush := globals.physChildDirEntryMap.getPageDirtyFlushTrigger()
	savedInodeMapDirtyFlush := globals.inodeMap.getPageDirtyFlushTrigger()
	savedPhysGCMax := globals.physChildDirEntryMap.getFlushesSinceLastGCMax()
	savedInodeMapGCMax := globals.inodeMap.getFlushesSinceLastGCMax()

	globals.physChildDirEntryMap.setPageDirtyFlushTrigger(defaultIngestFlushTrigger)
	globals.inodeMap.setPageDirtyFlushTrigger(defaultIngestFlushTrigger)
	globals.physChildDirEntryMap.setFlushesSinceLastGCMax(0)
	globals.inodeMap.setFlushesSinceLastGCMax(0)

	restore = func() {
		globals.physChildDirEntryMap.setPageDirtyFlushTrigger(savedPhysDirtyFlush)
		globals.inodeMap.setPageDirtyFlushTrigger(savedInodeMapDirtyFlush)
		globals.physChildDirEntryMap.setFlushesSinceLastGCMax(savedPhysGCMax)
		globals.inodeMap.setFlushesSinceLastGCMax(savedInodeMapGCMax)
	}
	return
}

// `logIngestCompletion` logs the final ingest summary.
func logIngestCompletion(label string, stats *ingestStats, startTime time.Time) {
	elapsed := time.Since(startTime)
	filesIngested := atomic.LoadInt64(&stats.filesIngested)
	dirsCreated := atomic.LoadInt64(&stats.dirsCreated)
	errCount := atomic.LoadInt64(&stats.errors)

	objPerSec := float64(0)
	if elapsed.Seconds() > 0 {
		objPerSec = float64(filesIngested) / elapsed.Seconds()
	}

	globals.logger.Printf("[INFO] manifest-ingest: %s — %d files, %d dirs created, %d errors, %.1f obj/sec, %v elapsed",
		label, filesIngested, dirsCreated, errCount, objPerSec, elapsed.Round(time.Millisecond))
}

// `ingestManifest` is called to read a manifest file or directory and ingest entries into the filesystem.
// If manifestPath is a directory, uses parallel readers from per-directory TSVs.
func ingestManifest(manifestPath string, backend *backendStruct) (err error) {
	var manifestInfo os.FileInfo
	manifestInfo, err = os.Stat(manifestPath)
	if err != nil {
		return fmt.Errorf("stat manifest %s: %w", manifestPath, err)
	}
	if manifestInfo.IsDir() {
		globals.logger.Printf("[INFO] manifest-ingest: using parallel readers from per-directory TSVs in %q", manifestPath)
		return ingestFromDirTSVs(manifestPath, backend)
	}

	var (
		startTime time.Time
		stats     ingestStats
		f         *os.File
		batchCh   chan *dirBatch
		dirCache  map[string]*inodeStruct
	)

	startTime = time.Now()
	stats = ingestStats{
		startTime:   startTime,
		lastLogNano: startTime.UnixNano(),
	}

	f, err = os.Open(manifestPath)
	if err != nil {
		return fmt.Errorf("open manifest %s: %w", manifestPath, err)
	}
	defer f.Close()

	globals.logger.Printf("[INFO] manifest-ingest: starting (path=%q, backend=%q)", manifestPath, backend.dirName)

	restore := ingestTuneForBulk()
	defer restore()

	batchCh = make(chan *dirBatch, 64)

	go ingestReader(f, batchCh, &stats)

	dirCache = make(map[string]*inodeStruct)

	for batch := range batchCh {
		ingestWriteBatch(batch, backend, dirCache, &stats)
	}

	globals.physChildDirEntryMap.forceFlush()
	globals.inodeMap.forceFlush()

	logIngestCompletion("complete", &stats, startTime)

	return nil
}

// `ingestReader` is called to scan manifest lines and batch them by parent directory for ingestion.
func ingestReader(f *os.File, batchCh chan<- *dirBatch, stats *ingestStats) {
	defer close(batchCh)

	var (
		scanner      *bufio.Scanner
		currentBatch *dirBatch
		line         string
		fields       []string
		size         uint64
		err          error
		entry        manifestEntry
		parentDir    string
	)

	scanner = bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	currentBatch = nil

	for scanner.Scan() {
		line = scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}

		fields = strings.SplitN(line, "\t", 4)
		if len(fields) != 4 {
			atomic.AddInt64(&stats.errors, 1)
			continue
		}

		size, err = strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			atomic.AddInt64(&stats.errors, 1)
			continue
		}

		entry = manifestEntry{
			Path:  fields[0],
			Size:  size,
			ETag:  fields[2],
			MTime: fields[3],
		}

		parentDir = path.Dir(entry.Path)
		if parentDir == "." {
			parentDir = ""
		} else {
			parentDir += "/"
		}

		if currentBatch == nil || currentBatch.parentPath != parentDir {
			if currentBatch != nil && len(currentBatch.entries) > 0 {
				batchCh <- currentBatch
			}
			currentBatch = &dirBatch{
				parentPath: parentDir,
				entries:    make([]manifestEntry, 0, defaultIngestBatchSize),
			}
		}

		currentBatch.entries = append(currentBatch.entries, entry)

		if len(currentBatch.entries) >= defaultIngestBatchSize {
			batchCh <- currentBatch
			currentBatch = &dirBatch{
				parentPath: parentDir,
				entries:    make([]manifestEntry, 0, defaultIngestBatchSize),
			}
		}
	}

	if currentBatch != nil && len(currentBatch.entries) > 0 {
		batchCh <- currentBatch
	}

	if err := scanner.Err(); err != nil {
		atomic.AddInt64(&stats.errors, 1)
		globals.logger.Printf("[WARN] manifest-ingest: scanner error: %v", err)
	}
}

// `ingestWriteBatch` creates file inodes and inserts them into the sharded B+Tree.
// Step 1a (globals.Lock): resolve dir chain only (fast, usually cached).
// Step 1b (no globals.Lock): create file inodes + insert into sharded inodeMap.
// Step 2 (phys shard lock): insert DirEntryInfo into physChildDirEntryMap shard + flush to PebbleDB.
func ingestWriteBatch(batch *dirBatch, backend *backendStruct, dirCache map[string]*inodeStruct, stats *ingestStats) {
	var (
		parentInode      *inodeStruct
		basename         string
		mTime            time.Time
		err              error
		inodeNumber      uint64
		fileInode        *inodeStruct
		now              time.Time
		total            int64
		elapsed          time.Duration
		intervalCount    int64
		intervalDuration time.Duration
		currentRate      float64
		overallRate      float64
	)

	type pendingEntry struct {
		basename string
		info     DirEntryInfo
	}

	tWaitLock := time.Now()

	// Step 1a: Resolve dir chain under globals.Lock (fast, dirs are cached in localDirCache)
	globalsLock("manifest_ingest.go:246:2:ingestWriteBatch")

	tGotLock := time.Now()

	parentInode = findOrCreateDirChain(batch.parentPath, backend, dirCache, stats)
	if parentInode == nil {
		globals.logger.Printf("[WARN] manifest-ingest: could not resolve dir chain for %q", batch.parentPath)
		atomic.AddInt64(&stats.errors, int64(len(batch.entries)))
		globalsUnlock()
		return
	}

	globalsUnlock()

	tDirChain := time.Now()

	// Step 1b: Create file inodes + insert into sharded inodeMap (no globals.Lock needed)
	pending := make([]pendingEntry, 0, len(batch.entries))

	for _, entry := range batch.entries {
		basename = path.Base(entry.Path)

		mTime, err = time.Parse(time.RFC3339Nano, entry.MTime)
		if err != nil {
			mTime = time.Now()
		}

		inodeNumber = fetchNonce()

		fileInode = &inodeStruct{
			inodeNumber:       inodeNumber,
			inodeType:         FileObject,
			backendNonce:      backend.nonce,
			parentInodeNumber: parentInode.inodeNumber,
			isVirt:            false,
			objectPath:        parentInode.objectPath + basename,
			basename:          basename,
			sizeInBackend:     entry.Size,
			sizeInMemory:      entry.Size,
			eTag:              entry.ETag,
			mode:              uint32(syscall.S_IFREG | backend.filePerm),
			mTime:             mTime,
			xTime:             time.Time{},
			cacheMap:          make(map[uint64]uint64),
			fhSet:             make(map[uint64]struct{}),
		}

		globals.inodeMap.put(fileInode)

		pending = append(pending, pendingEntry{
			basename: basename,
			info: DirEntryInfo{
				InodeNumber:   fileInode.inodeNumber,
				InodeType:     FileObject,
				Size:          entry.Size,
				Mode:          fileInode.mode,
				MTimeUnixNano: mTime.UnixNano(),
			},
		})
	}

	tInodes := time.Now()

	// Step 2: Insert into B+Tree shard + conditionally flush to PebbleDB (under shard lock only)
	shard := globals.physChildDirEntryMap.shardFor(parentInode.inodeNumber)
	shard.mu.Lock()

	tGotShard := time.Now()

	for _, pe := range pending {
		shard.tree.putByKeyNoFlush(parentInode.inodeNumber, pe.basename, pe.info)
		atomic.AddInt64(&stats.filesIngested, 1)
	}

	tPuts := time.Now()

	didFlush := false
	shard.batchesSinceFlush++
	if shard.batchesSinceFlush >= defaultIngestFlushInterval {
		shard.tree.forceFlush()
		shard.batchesSinceFlush = 0
		didFlush = true
	}

	tFlush := time.Now()
	shard.mu.Unlock()

	shardIdx := parentInode.inodeNumber % dirEntryMapShardCount
	batchTotal := atomic.AddInt64(&stats.batchesProcessed, 1)
	if batchTotal%200 == 0 || tFlush.Sub(tWaitLock) > 500*time.Millisecond {
		globals.logger.Printf("[TRACE] ingest-timing: batch=%d shard=%d entries=%d lockWait=%v dirChain=%v inodes=%v shardWait=%v puts=%v flush=%v flushed=%v total=%v",
			batchTotal, shardIdx, len(batch.entries),
			tGotLock.Sub(tWaitLock).Round(time.Microsecond),
			tDirChain.Sub(tGotLock).Round(time.Microsecond),
			tInodes.Sub(tDirChain).Round(time.Microsecond),
			tGotShard.Sub(tInodes).Round(time.Microsecond),
			tPuts.Sub(tGotShard).Round(time.Microsecond),
			tFlush.Sub(tPuts).Round(time.Microsecond),
			didFlush,
			tFlush.Sub(tWaitLock).Round(time.Microsecond))
	}

	now = time.Now()
	total = atomic.LoadInt64(&stats.filesIngested)
	lastNano := atomic.LoadInt64(&stats.lastLogNano)
	if now.UnixNano()-lastNano >= int64(10*time.Second) {
		if atomic.CompareAndSwapInt64(&stats.lastLogNano, lastNano, now.UnixNano()) {
			elapsed = now.Sub(stats.startTime)
			lastCount := atomic.SwapInt64(&stats.lastLogCount, total)
			intervalCount = total - lastCount
			intervalDuration = time.Duration(now.UnixNano() - lastNano)
			currentRate = float64(intervalCount) / intervalDuration.Seconds()
			overallRate = float64(total) / elapsed.Seconds()
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			globals.logger.Printf("[INFO] manifest-ingest: progress — %d files, %d dirs, %.0f obj/sec (current), %.0f obj/sec (overall), %v elapsed, mem=%dMiB, gcPauses=%d",
				total, atomic.LoadInt64(&stats.dirsCreated), currentRate, overallRate, elapsed.Round(time.Second),
				memStats.Alloc/1024/1024, memStats.NumGC)
		}
	}
}

// `findOrCreateDirChain` is called to resolve or create a chain of pseudo-directory inodes for a path.
func findOrCreateDirChain(dirPath string, backend *backendStruct, dirCache map[string]*inodeStruct, stats *ingestStats) (result *inodeStruct) {
	if dirPath == "" {
		result = backend.inode
		return
	}

	var (
		cached       *inodeStruct
		ok           bool
		components   []string
		current      *inodeStruct
		builtPath    string
		childDirInfo DirEntryInfo
		childInode   *inodeStruct
		found        bool
		pseudoDir    *inodeStruct
	)

	cached, ok = dirCache[dirPath]
	if ok {
		result = cached
		return
	}

	dirPath = strings.TrimSuffix(dirPath, "/")
	components = strings.Split(dirPath, "/")

	current = backend.inode
	builtPath = ""

	for _, comp := range components {
		if comp == "" {
			continue
		}

		if builtPath == "" {
			builtPath = comp + "/"
		} else {
			builtPath += comp + "/"
		}

		cached, ok = dirCache[builtPath]
		if ok {
			current = cached
			continue
		}

		childDirInfo, ok = globals.physChildDirEntryMap.getByBasename(current.inodeNumber, comp)
		if ok {
			childInode, found = globals.inodeMap.get(childDirInfo.InodeNumber)
			if found && childInode.inodeType == PseudoDir {
				dirCache[builtPath] = childInode
				current = childInode
				continue
			}
		}

		pseudoDir = current.createPseudoDirInode(false, comp, true)
		dirCache[builtPath] = pseudoDir
		current = pseudoDir
		atomic.AddInt64(&stats.dirsCreated, 1)
	}

	dirCache[strings.TrimSuffix(dirPath, "/")+"/"] = current
	result = current
	return
}

// `ingestFromDirTSVs` reads per-directory TSV files using parallel readers and feeds
// entries through the ingestWriteBatch pipeline.
func ingestFromDirTSVs(manifestDir string, backend *backendStruct) (err error) {
	var (
		startTime  time.Time
		stats      ingestStats
		numReaders = defaultIngestNumReaders
	)

	startTime = time.Now()
	stats = ingestStats{
		startTime:   startTime,
		lastLogNano: startTime.UnixNano(),
	}

	globals.logger.Printf("[INFO] manifest-ingest: starting per-directory ingest (dir=%q, backend=%q, readers=%d)",
		manifestDir, backend.dirName, numReaders)

	restore := ingestTuneForBulk()
	defer restore()

	pathsCh := make(chan string, 1000)
	batchCh := make(chan *dirBatch, 64)

	go func() {
		defer close(pathsCh)
		_ = filepath.Walk(manifestDir, func(walkPath string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				globals.logger.Printf("[WARN] manifest-ingest: walk error at %q: %v", walkPath, walkErr)
				return nil
			}
			if info == nil || info.IsDir() || !strings.HasSuffix(walkPath, ".tsv") {
				return nil
			}
			pathsCh <- walkPath
			return nil
		})
	}()

	var readerWG sync.WaitGroup
	for range numReaders {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for walkPath := range pathsCh {
				relPath, relErr := filepath.Rel(manifestDir, walkPath)
				if relErr != nil {
					atomic.AddInt64(&stats.errors, 1)
					globals.logger.Printf("[WARN] manifest-ingest: filepath.Rel(%q, %q) failed: %v", manifestDir, walkPath, relErr)
					continue
				}

				var parentDirPath string
				if relPath == "_root.tsv" {
					parentDirPath = ""
				} else {
					parentDirPath = strings.TrimSuffix(relPath, ".tsv") + "/"
				}

				entries, readErr := readManifestPart(walkPath)
				if readErr != nil {
					atomic.AddInt64(&stats.errors, 1)
					globals.logger.Printf("[WARN] manifest-ingest: readManifestPart(%q) failed (relPath=%q): %v", walkPath, relPath, readErr)
					continue
				}

				currentBatch := &dirBatch{
					parentPath: parentDirPath,
					entries:    make([]manifestEntry, 0, len(entries)),
				}

				for _, e := range entries {
					if e.Kind == "d" {
						continue
					}
					currentBatch.entries = append(currentBatch.entries, manifestEntry{
						Path:  parentDirPath + e.Basename,
						Size:  e.Size,
						ETag:  e.ETag,
						MTime: e.MTime.UTC().Format(time.RFC3339Nano),
					})

					if len(currentBatch.entries) >= defaultIngestBatchSize {
						batchCh <- currentBatch
						currentBatch = &dirBatch{
							parentPath: parentDirPath,
							entries:    make([]manifestEntry, 0, defaultIngestBatchSize),
						}
					}
				}

				if len(currentBatch.entries) > 0 {
					batchCh <- currentBatch
				}
			}
		}()
	}

	go func() {
		readerWG.Wait()
		close(batchCh)
	}()

	numIngestWorkers := defaultIngestNumWorkers
	var ingestWG sync.WaitGroup
	for range numIngestWorkers {
		ingestWG.Add(1)
		go func() {
			defer ingestWG.Done()
			localDirCache := make(map[string]*inodeStruct)
			for batch := range batchCh {
				ingestWriteBatch(batch, backend, localDirCache, &stats)
			}
		}()
	}
	ingestWG.Wait()

	globals.physChildDirEntryMap.forceFlush()
	globals.inodeMap.forceFlush()

	logIngestCompletion("per-directory ingest complete", &stats, startTime)

	return nil
}
