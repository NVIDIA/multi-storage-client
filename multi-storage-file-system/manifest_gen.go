package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultManifestGenWorkers   = 200
	defaultManifestGenQueueSize = 100000
	manifestWriterBufSize       = 256 * 1024
	manifestVersion             = 2
	maxRecursionDepth           = 100
)

type manifestGenConfig struct {
	workers     int
	outputPath  string
	tempDir     string
	backendName string
	backend     *backendStruct
}

// manifestEntry is used by the ingest path (flat TSV format with full paths).
type manifestEntry struct {
	Path  string
	Size  uint64
	ETag  string
	MTime string
}

type manifestIndex struct {
	Version          int    `json:"version"`
	Format           string `json:"format"`
	Bucket           string `json:"bucket"`
	Prefix           string `json:"prefix"`
	TotalObjects     int64  `json:"total_objects"`
	TotalDirectories int64  `json:"total_directories"`
	CreatedAt        string `json:"created_at"`
}

type manifestDirEntry struct {
	Kind     string
	Basename string
	Size     uint64
	ETag     string
	MTime    time.Time
}

type manifestGenerator struct {
	cfg         *manifestGenConfig
	backend     *backendStruct
	workQueue   chan string
	wg          sync.WaitGroup
	totalFiles  atomic.Int64
	totalBytes  atomic.Int64
	totalDirs   atomic.Int64
	errors      atomic.Int64
	startTime   time.Time
	manifestDir string
}

// `manifestPartPath` returns the on-disk TSV path for a given directory's manifest.
// objectPath="" maps to _root.tsv; "dir-r0/r0/" maps to dir-r0/r0.tsv.
// objectPath is defensively normalized (rooted-clean) so cloud-supplied keys with
// "../" or leading "/" components cannot escape manifestDir.
func manifestPartPath(manifestDir, objectPath string) string {
	if objectPath == "" {
		return filepath.Join(manifestDir, "_root.tsv")
	}
	safe := strings.TrimPrefix(filepath.Clean("/"+strings.TrimSuffix(objectPath, "/")), "/")
	if safe == "" || safe == "." {
		return filepath.Join(manifestDir, "_root.tsv")
	}
	return filepath.Join(manifestDir, safe+".tsv")
}

// `generateManifest` is called to walk a backend namespace and write per-directory TSV manifest files.
func generateManifest(cfg *manifestGenConfig) (err error) {
	var (
		backend     *backendStruct
		ok          bool
		gen         *manifestGenerator
		workerWG    sync.WaitGroup
		elapsed     time.Duration
		totalFiles  int64
		totalDirs   int64
		totalBytes  int64
		totalErrors int64
		objPerSec   float64
	)

	backend = cfg.backend
	if backend == nil {
		backend, ok = globals.backendsToMount[cfg.backendName]
		if !ok {
			return fmt.Errorf("backend %q not found in config (available: %v)", cfg.backendName, backendNames())
		}
		err = backend.setupContext()
		if err != nil {
			return fmt.Errorf("setup backend context for %q: %w", cfg.backendName, err)
		}
		cfg.backend = backend
	}

	globals.logger.Printf("[INFO] manifest-gen: backend %q ready (bucket=%q, prefix=%q, path=%s)",
		cfg.backendName, backend.bucketContainerName, backend.prefix, backend.backendPath)

	// Refuse to operate on obviously dangerous outputPath values before we RemoveAll.
	if cfg.outputPath == "" || cfg.outputPath == "." || cfg.outputPath == "/" {
		return fmt.Errorf("refusing manifest output path %q", cfg.outputPath)
	}
	absOut, absErr := filepath.Abs(cfg.outputPath)
	if absErr != nil {
		return fmt.Errorf("resolve manifest output path %q: %w", cfg.outputPath, absErr)
	}
	if absOut == "/" || absOut == string(os.PathSeparator) {
		return fmt.Errorf("refusing root-level manifest output path %q", absOut)
	}

	err = os.RemoveAll(cfg.outputPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("clean manifest dir %s: %w", cfg.outputPath, err)
	}

	err = os.MkdirAll(cfg.outputPath, 0o755)
	if err != nil {
		return fmt.Errorf("create manifest dir %s: %w", cfg.outputPath, err)
	}

	gen = &manifestGenerator{
		cfg:         cfg,
		backend:     backend,
		workQueue:   make(chan string, defaultManifestGenQueueSize),
		manifestDir: cfg.outputPath,
		startTime:   time.Now(),
	}

	gen.wg.Add(1)
	gen.workQueue <- ""

	globals.logger.Printf("[INFO] manifest-gen: starting BFS with %d workers (per-directory format, dir=%q)",
		cfg.workers, gen.manifestDir)

	for i := range cfg.workers {
		workerWG.Add(1)
		go gen.worker(i, &workerWG)
	}

	// gen.wg tracks outstanding directory work items (Add before enqueue, Done after list).
	// When all directories have been listed, wg unblocks and we close the channel to stop workers.
	go func() {
		gen.wg.Wait()
		close(gen.workQueue)
	}()

	workerWG.Wait()

	err = writeManifestIndex(gen.manifestDir, backend.bucketContainerName, backend.prefix,
		gen.totalFiles.Load(), gen.totalDirs.Load())
	if err != nil {
		return fmt.Errorf("write manifest index: %w", err)
	}

	elapsed = time.Since(gen.startTime)
	totalFiles = gen.totalFiles.Load()
	totalDirs = gen.totalDirs.Load()
	totalBytes = gen.totalBytes.Load()
	totalErrors = gen.errors.Load()

	objPerSec = float64(0)
	if elapsed.Seconds() > 0 {
		objPerSec = float64(totalFiles) / elapsed.Seconds()
	}

	globals.logger.Printf("[INFO] manifest-gen: complete — %d objects, %d directories, %s total size, %d errors, %.1f obj/sec, %v elapsed",
		totalFiles, totalDirs, formatBytes(uint64(totalBytes)), totalErrors, objPerSec, elapsed.Round(time.Millisecond))
	globals.logger.Printf("[INFO] manifest-gen: output written to %s", gen.manifestDir)

	return nil
}

// `worker` is called to dequeue directory paths from the work queue and write per-directory TSV files.
func (gen *manifestGenerator) worker(id int, workerWG *sync.WaitGroup) {
	defer workerWG.Done()

	for dirPath := range gen.workQueue {
		gen.listDirectory(dirPath, 0)
		gen.wg.Done()
	}
}

const (
	flatDirMinPrefixSplits          = 4                                                                   // Minimum non-empty prefix probes before using prefix-parallel strategy (Strategy 2)
	flatDirMaxRangeSplitWorkers     = 200                                                                 // Upper bound on parallel range-split workers (Strategy 3)
	flatDirMinPagesPerWorker        = 5                                                                   // Minimum S3 pages per worker to avoid diminishing returns from too many workers
	flatDirProbeChars               = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-." // Characters probed during automatic prefix discovery (Strategy 2)
	defaultFlatDirConfirmationPages = 5                                                                   // Number of truncated pages required to confirm a directory is truly flat and large
)

// `listDirectory` lists objects under one directory via S3 and writes a per-directory TSV file.
// Includes the hybrid controller: checks for flat_dir_hints, detects flat directories, and
// dispatches to the appropriate parallel listing strategy.
func (gen *manifestGenerator) listDirectory(dirPath string, depth int) {
	if hint := gen.flatDirHintFor(dirPath); hint != nil {
		globals.logger.Printf("[INFO] manifest-gen: flat dir hint matched for %q (chars=%q, depth=%d)",
			dirPath, hint.KeyPrefixChars, hint.SplitDepth)
		gen.totalDirs.Add(1)
		gen.listFlatWithPrefixHints(dirPath, hint)
		return
	}

	gen.totalDirs.Add(1)

	input := &listDirectoryInputStruct{
		maxItems: 1000,
		dirPath:  dirPath,
	}
	firstPage, err := gen.backend.context.listDirectory(input)
	if err != nil {
		globals.logger.Printf("[WARN] manifest-gen: listDirectory failed for %q: %v", dirPath, err)
		gen.errors.Add(1)
		return
	}

	if len(firstPage.subdirectory) == 0 && firstPage.isTruncated {
		confirmed, collectedPages := gen.confirmFlatDirectory(dirPath, firstPage)
		if confirmed {
			if len(collectedPages) == 0 || len(collectedPages[0].file) == 0 {
				globals.logger.Printf("[WARN] manifest-gen: flat-confirmed at %q but no anchor file in first page; falling back to normal listing",
					dirPath)
				gen.listDirectoryNormalWithPages(dirPath, depth, collectedPages)
				return
			}
			firstBasename := collectedPages[0].file[0].basename
			flatPrefix := dirPath + string(firstBasename[0])
			globals.logger.Printf("[INFO] manifest-gen: flat directory confirmed at %q (%d+ pages, flatPrefix=%q), using parallel listing",
				dirPath, len(collectedPages), flatPrefix)
			gen.listFlatDirectory(dirPath, flatPrefix, collectedPages)
			gen.resumeBFSAfterFlat(dirPath, flatPrefix, depth)
			return
		}
		gen.listDirectoryNormalWithPages(dirPath, depth, collectedPages)
		return
	}

	gen.listDirectoryNormal(dirPath, depth, firstPage)
}

// `confirmFlatDirectory` fetches additional pages to confirm a directory is truly large + flat.
// Returns true only if all confirmation pages are still flat and truncated.
func (gen *manifestGenerator) confirmFlatDirectory(dirPath string, firstPage *listDirectoryOutputStruct) (confirmed bool, pages []*listDirectoryOutputStruct) {
	pages = append(pages, firstPage)
	token := firstPage.nextContinuationToken

	confirmPages := gen.backend.flatDirConfirmationPages
	if confirmPages <= 0 {
		confirmPages = defaultFlatDirConfirmationPages
	}

	for range confirmPages - 1 {
		page, err := gen.backend.context.listDirectory(&listDirectoryInputStruct{
			continuationToken: token,
			maxItems:          1000,
			dirPath:           dirPath,
		})
		if err != nil {
			return false, pages
		}
		pages = append(pages, page)

		if len(page.subdirectory) > 0 {
			return false, pages
		}
		if !page.isTruncated {
			return false, pages
		}
		token = page.nextContinuationToken
	}

	confirmed = true
	return
}

// `listDirectoryNormalWithPages` writes pre-fetched pages to TSV, then continues pagination.
func (gen *manifestGenerator) listDirectoryNormalWithPages(dirPath string, depth int, pages []*listDirectoryOutputStruct) {
	partPath := manifestPartPath(gen.manifestDir, dirPath)

	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		globals.logger.Printf("[WARN] manifest-gen: mkdir for %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}

	f, err := os.Create(partPath)
	if err != nil {
		globals.logger.Printf("[WARN] manifest-gen: create %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}

	w := bufio.NewWriterSize(f, manifestWriterBufSize)
	defer func() {
		if flushErr := w.Flush(); flushErr != nil {
			gen.errors.Add(1)
		}
		if closeErr := f.Close(); closeErr != nil {
			gen.errors.Add(1)
		}
	}()

	genTime := time.Now().UTC().Format(time.RFC3339Nano)

	for _, page := range pages {
		gen.writePageToTSV(w, dirPath, page, genTime, depth)
	}

	lastPage := pages[len(pages)-1]
	if !lastPage.isTruncated {
		return
	}

	continuationToken := lastPage.nextContinuationToken
	for {
		page, pageErr := gen.backend.context.listDirectory(&listDirectoryInputStruct{
			continuationToken: continuationToken,
			maxItems:          1000,
			dirPath:           dirPath,
		})
		if pageErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: listDirectory failed for %q: %v", dirPath, pageErr)
			gen.errors.Add(1)
			return
		}

		gen.writePageToTSV(w, dirPath, page, genTime, depth)

		if !page.isTruncated {
			break
		}
		continuationToken = page.nextContinuationToken
	}
}

// `listDirectoryNormal` handles the standard hierarchical BFS case (has subdirs or is small).
func (gen *manifestGenerator) listDirectoryNormal(dirPath string, depth int, firstPage *listDirectoryOutputStruct) {
	partPath := manifestPartPath(gen.manifestDir, dirPath)

	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		globals.logger.Printf("[WARN] manifest-gen: mkdir for %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}

	f, err := os.Create(partPath)
	if err != nil {
		globals.logger.Printf("[WARN] manifest-gen: create %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}

	w := bufio.NewWriterSize(f, manifestWriterBufSize)

	defer func() {
		if flushErr := w.Flush(); flushErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: flush %q: %v", partPath, flushErr)
			gen.errors.Add(1)
		}
		if closeErr := f.Close(); closeErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: close %q: %v", partPath, closeErr)
			gen.errors.Add(1)
		}
	}()

	genTime := time.Now().UTC().Format(time.RFC3339Nano)

	gen.writePageToTSV(w, dirPath, firstPage, genTime, depth)

	continuationToken := firstPage.nextContinuationToken
	for firstPage.isTruncated {
		input := &listDirectoryInputStruct{
			continuationToken: continuationToken,
			maxItems:          1000,
			dirPath:           dirPath,
		}

		page, pageErr := gen.backend.context.listDirectory(input)
		if pageErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: listDirectory failed for %q: %v", dirPath, pageErr)
			gen.errors.Add(1)
			return
		}

		gen.writePageToTSV(w, dirPath, page, genTime, depth)

		if !page.isTruncated {
			break
		}
		continuationToken = page.nextContinuationToken
	}
}

func (gen *manifestGenerator) writePageToTSV(w *bufio.Writer, dirPath string, page *listDirectoryOutputStruct, genTime string, depth int) {
	for _, subdir := range page.subdirectory {
		fmt.Fprintf(w, "d\t%s\t0\t-\t%s\n", subdir, genTime)

		childPath := dirPath + subdir + "/"
		gen.wg.Add(1)
		select {
		case gen.workQueue <- childPath:
		default:
			if depth >= maxRecursionDepth {
				globals.logger.Printf("[WARN] manifest-gen: max recursion depth %d reached for %q, blocking on queue", maxRecursionDepth, childPath)
				gen.workQueue <- childPath
			} else {
				gen.listDirectory(childPath, depth+1)
				gen.wg.Done()
			}
		}
	}

	for _, file := range page.file {
		fmt.Fprintf(w, "f\t%s\t%d\t%s\t%s\n",
			file.basename, file.size, file.eTag,
			file.mTime.UTC().Format(time.RFC3339Nano))
		gen.totalFiles.Add(1)
		gen.totalBytes.Add(int64(file.size))
	}
}

// `resumeBFSAfterFlat` uses startAfter to skip past all keys in the flat prefix range,
// then discovers any subdirectories or files that come after the flat portion.
// This handles mixed directories where flat files (e.g., file-*.bin) come lexicographically
// before subdirectories (e.g., r0/, r1/).
func (gen *manifestGenerator) resumeBFSAfterFlat(dirPath, flatPrefix string, depth int) {
	startAfterKey := flatPrefix + strings.Repeat("~", 20)

	partPath := manifestPartPath(gen.manifestDir, dirPath)
	genTime := time.Now().UTC().Format(time.RFC3339Nano)
	foundExtra := false
	continuationToken := ""

	for {
		input := &listDirectoryInputStruct{
			maxItems: 1000,
			dirPath:  dirPath,
		}
		if continuationToken != "" {
			input.continuationToken = continuationToken
		} else {
			input.startAfter = startAfterKey
		}

		page, err := gen.backend.context.listDirectory(input)
		if err != nil {
			globals.logger.Printf("[WARN] manifest-gen: resumeBFSAfterFlat failed for %q: %v", dirPath, err)
			gen.errors.Add(1)
			return
		}

		if len(page.subdirectory) > 0 || len(page.file) > 0 {
			if !foundExtra {
				globals.logger.Printf("[INFO] manifest-gen: resumeBFSAfterFlat for %q — found %d subdirs + %d files in tail",
					dirPath, len(page.subdirectory), len(page.file))
				foundExtra = true
			}

			f, openErr := os.OpenFile(partPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if openErr != nil {
				globals.logger.Printf("[WARN] manifest-gen: resumeBFSAfterFlat open %q: %v", partPath, openErr)
				gen.errors.Add(1)
			} else {
				w := bufio.NewWriterSize(f, manifestWriterBufSize)
				for _, subdir := range page.subdirectory {
					fmt.Fprintf(w, "d\t%s\t0\t-\t%s\n", subdir, genTime)
					childPath := dirPath + subdir + "/"
					gen.wg.Add(1)
					gen.workQueue <- childPath
				}
				for _, file := range page.file {
					fmt.Fprintf(w, "f\t%s\t%d\t%s\t%s\n",
						file.basename, file.size, file.eTag,
						file.mTime.UTC().Format(time.RFC3339Nano))
					gen.totalFiles.Add(1)
					gen.totalBytes.Add(int64(file.size))
				}
				if flushErr := w.Flush(); flushErr != nil {
					globals.logger.Printf("[WARN] manifest-gen: resumeBFSAfterFlat flush %q: %v", partPath, flushErr)
					gen.errors.Add(1)
				}
				if closeErr := f.Close(); closeErr != nil {
					globals.logger.Printf("[WARN] manifest-gen: resumeBFSAfterFlat close %q: %v", partPath, closeErr)
					gen.errors.Add(1)
				}
			}
		}

		if !page.isTruncated {
			break
		}
		continuationToken = page.nextContinuationToken
	}
}

// --- Hybrid controller for flat directories ---

func (gen *manifestGenerator) flatDirHintFor(dirPath string) *flatDirHintStruct {
	for i := range gen.backend.flatDirHints {
		if gen.backend.flatDirHints[i].Path == dirPath {
			return &gen.backend.flatDirHints[i]
		}
	}
	return nil
}

// `listFlatDirectory` auto-selects Strategy 2 (prefix discovery) or Strategy 3 (range splitting).
// collectedPages are used only for size estimation — they are NOT written to TSV.
// Range workers re-list the entire flat range to avoid overlap with confirmation pages.
func (gen *manifestGenerator) listFlatDirectory(dirPath, flatPrefix string, collectedPages []*listDirectoryOutputStruct) {
	prefixes := gen.discoverPrefixes(dirPath)
	if len(prefixes) >= flatDirMinPrefixSplits {
		globals.logger.Printf("[INFO] manifest-gen: flat dir %q — using prefix discovery (%d prefixes)", dirPath, len(prefixes))
		gen.listFlatWithPrefixes(dirPath, prefixes)
		return
	}

	globals.logger.Printf("[INFO] manifest-gen: flat dir %q — prefix discovery found only %d prefixes, falling back to range splitting", dirPath, len(prefixes))
	gen.listFlatWithRangeSplitting(dirPath, flatPrefix, collectedPages)
}

// --- Strategy 1: User-provided prefix hints ---

func (gen *manifestGenerator) listFlatWithPrefixHints(dirPath string, hint *flatDirHintStruct) {
	subPrefixes := generateSubPrefixes(hint.KeyPrefixChars, hint.SplitDepth)
	globals.logger.Printf("[INFO] manifest-gen: flat dir %q — Strategy 1 (prefix hints): %d sub-prefixes", dirPath, len(subPrefixes))
	gen.writeCollectedPagesAsTSV(dirPath, nil)
	gen.listFlatParallel(dirPath, subPrefixes)
}

func generateSubPrefixes(chars string, depth int) []string {
	if depth <= 0 {
		depth = 1
	}
	current := []string{""}
	for range depth {
		var next []string
		for _, prefix := range current {
			for _, ch := range chars {
				next = append(next, prefix+string(ch))
			}
		}
		current = next
	}
	return current
}

// --- Strategy 2: Automatic prefix discovery ---

func (gen *manifestGenerator) discoverPrefixes(dirPath string) []string {
	type probeResult struct {
		prefix   string
		nonEmpty bool
	}

	results := make(chan probeResult, len(flatDirProbeChars))
	var probeWG sync.WaitGroup

	for _, ch := range flatDirProbeChars {
		probeWG.Add(1)
		go func(c rune) {
			defer probeWG.Done()
			subPrefix := dirPath + string(c)
			output, err := listPrefixWrapper(gen.backend.context, &listPrefixInputStruct{
				prefix:   subPrefix,
				maxItems: 1,
			})
			if err != nil {
				results <- probeResult{prefix: string(c), nonEmpty: false}
				return
			}
			results <- probeResult{prefix: string(c), nonEmpty: len(output.file) > 0}
		}(ch)
	}

	go func() {
		probeWG.Wait()
		close(results)
	}()

	var nonEmpty []string
	for r := range results {
		if r.nonEmpty {
			nonEmpty = append(nonEmpty, r.prefix)
		}
	}

	globals.logger.Printf("[INFO] manifest-gen: prefix discovery for %q — probed %d chars, %d non-empty",
		dirPath, len(flatDirProbeChars), len(nonEmpty))

	return nonEmpty
}

// --- Strategy 3: StartAfter range splitting ---

func (gen *manifestGenerator) listFlatWithRangeSplitting(dirPath, flatPrefix string, collectedPages []*listDirectoryOutputStruct) {
	if len(collectedPages) == 0 || len(collectedPages[0].file) == 0 {
		globals.logger.Printf("[WARN] manifest-gen: range splitting — no anchor file in collectedPages for %q, skipping",
			dirPath)
		gen.errors.Add(1)
		return
	}
	firstKey := dirPath + collectedPages[0].file[0].basename

	lastKey, err := gen.estimateLastKey(flatPrefix, firstKey)
	if err != nil || lastKey == "" {
		globals.logger.Printf("[WARN] manifest-gen: range splitting — could not estimate last key for %q, listing sequentially", dirPath)
		partPath := manifestPartPath(gen.manifestDir, dirPath)
		if mkdirErr := os.MkdirAll(filepath.Dir(partPath), 0o755); mkdirErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: mkdir for %q: %v", partPath, mkdirErr)
			gen.errors.Add(1)
			return
		}
		tp := gen.listPrefixRange(flatPrefix, "", "")
		if tp != "" {
			if mergeErr := mergeTempFiles(partPath, []string{tp}); mergeErr != nil {
				globals.logger.Printf("[WARN] manifest-gen: merge temp for %q: %v", dirPath, mergeErr)
				gen.errors.Add(1)
			}
		}
		return
	}

	filesCollected := 0
	for _, p := range collectedPages {
		filesCollected += len(p.file)
	}
	estimatedTotalFiles := filesCollected * 20
	estimatedPages := estimatedTotalFiles / 1000
	if estimatedPages < 1 {
		estimatedPages = 1
	}

	n := estimatedPages / flatDirMinPagesPerWorker
	if n < 2 {
		n = 2
	}
	if n > flatDirMaxRangeSplitWorkers {
		n = flatDirMaxRangeSplitWorkers
	}

	splits := lexInterpolate(firstKey, lastKey, n)

	// Prepend "" so the first worker covers from the beginning of the prefix.
	// This re-lists the confirmation pages' files (~5 S3 calls) but eliminates
	// all overlap between confirmation pages and range workers.
	allSplits := make([]string, 0, len(splits)+1)
	allSplits = append(allSplits, "")
	allSplits = append(allSplits, splits...)

	globals.logger.Printf("[INFO] manifest-gen: flat dir %q — Strategy 3 (range splitting): %d workers (est %d files, %d pages), keyspace [%q..%q]",
		dirPath, len(allSplits), estimatedTotalFiles, estimatedPages, firstKey, lastKey)

	tempPaths := make([]string, len(allSplits))
	var subWG sync.WaitGroup
	for i := range allSplits {
		sa := allSplits[i]
		st := ""
		if i+1 < len(allSplits) {
			st = allSplits[i+1]
		}

		subWG.Add(1)
		go func(idx int, startAfter, stopAt string) {
			defer subWG.Done()
			tempPaths[idx] = gen.listPrefixRange(flatPrefix, startAfter, stopAt)
		}(i, sa, st)
	}
	subWG.Wait()

	partPath := manifestPartPath(gen.manifestDir, dirPath)
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		globals.logger.Printf("[WARN] manifest-gen: mkdir for %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}
	if err := mergeTempFiles(partPath, tempPaths); err != nil {
		globals.logger.Printf("[WARN] manifest-gen: merge temp files for %q: %v", dirPath, err)
		gen.errors.Add(1)
	}
}

// `estimateLastKey` finds the last key in a flat directory using binary search + forward sweep.
// firstKnownKey is the first key from the collected pages (our lower bound).
func (gen *manifestGenerator) estimateLastKey(flatPrefix, firstKnownKey string) (string, error) {
	hasKeysAfter := func(probe string) bool {
		output, err := listPrefixWrapper(gen.backend.context, &listPrefixInputStruct{
			prefix:     flatPrefix,
			startAfter: probe,
			maxItems:   1,
		})
		if err != nil {
			return false
		}
		return len(output.file) > 0
	}

	low := firstKnownKey
	high := flatPrefix + strings.Repeat("~", 20)

	if !hasKeysAfter(low) {
		return low, nil
	}

	for range 40 {
		mid := lexMidpoint(low, high)
		if mid == low || mid == high {
			break
		}
		if hasKeysAfter(mid) {
			low = mid
		} else {
			high = mid
		}
	}

	// Forward sweep: paginate from binary search result to find the actual last key.
	// The binary search gets us close; the sweep handles any residual gap.
	sweepKey := low
	for range 50 {
		output, err := listPrefixWrapper(gen.backend.context, &listPrefixInputStruct{
			prefix:     flatPrefix,
			startAfter: sweepKey,
			maxItems:   1000,
		})
		if err != nil || len(output.file) == 0 {
			break
		}
		sweepKey = flatPrefix + output.file[len(output.file)-1].basename
		if !output.isTruncated {
			break
		}
	}
	return sweepKey, nil
}

func lexMidpoint(a, b string) string {
	splits := lexInterpolate(a, b, 2)
	if len(splits) > 0 {
		return splits[0]
	}
	return a
}

// --- Common: parallel prefix listing with merge ---

func (gen *manifestGenerator) listFlatWithPrefixes(dirPath string, prefixes []string) {
	gen.listFlatParallel(dirPath, prefixes)
}

func (gen *manifestGenerator) listFlatParallel(dirPath string, prefixes []string) {
	tempPaths := make([]string, len(prefixes))
	var subWG sync.WaitGroup
	for i, p := range prefixes {
		subWG.Add(1)
		go func(idx int, subPrefix string) {
			defer subWG.Done()
			tempPaths[idx] = gen.listPrefixRange(dirPath+subPrefix, "", "")
		}(i, p)
	}
	subWG.Wait()

	partPath := manifestPartPath(gen.manifestDir, dirPath)
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		globals.logger.Printf("[WARN] manifest-gen: mkdir for %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}
	if err := mergeTempFiles(partPath, tempPaths); err != nil {
		globals.logger.Printf("[WARN] manifest-gen: merge temp files for %q: %v", dirPath, err)
		gen.errors.Add(1)
	}
}

// `listPrefixRange` lists all objects matching queryPrefix, writes to a temp file,
// and returns the temp file path. Optionally stops at stopAt.
func (gen *manifestGenerator) listPrefixRange(queryPrefix, startAfter, stopAt string) (tempPath string) {
	tmpFile, err := os.CreateTemp(gen.manifestDir, "flat_*.tsv")
	if err != nil {
		globals.logger.Printf("[WARN] manifest-gen: create temp file: %v", err)
		gen.errors.Add(1)
		return ""
	}
	tempPath = tmpFile.Name()

	w := bufio.NewWriterSize(tmpFile, manifestWriterBufSize)
	defer func() {
		if flushErr := w.Flush(); flushErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: flush %q: %v", tempPath, flushErr)
			gen.errors.Add(1)
		}
		if closeErr := tmpFile.Close(); closeErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: close %q: %v", tempPath, closeErr)
			gen.errors.Add(1)
		}
	}()

	continuationToken := ""
	for {
		input := &listPrefixInputStruct{
			prefix:            queryPrefix,
			startAfter:        startAfter,
			stopAt:            stopAt,
			continuationToken: continuationToken,
			maxItems:          1000,
		}

		output, listErr := listPrefixWrapper(gen.backend.context, input)
		if listErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: listPrefix failed for %q: %v", queryPrefix, listErr)
			gen.errors.Add(1)
			return
		}

		for _, file := range output.file {
			fmt.Fprintf(w, "f\t%s\t%d\t%s\t%s\n",
				file.basename, file.size, file.eTag,
				file.mTime.UTC().Format(time.RFC3339Nano))
			gen.totalFiles.Add(1)
			gen.totalBytes.Add(int64(file.size))
		}

		if output.stopped || !output.isTruncated {
			break
		}
		continuationToken = output.nextContinuationToken
		startAfter = ""
	}

	return tempPath
}

// `mergeTempFiles` concatenates temp files into the final TSV and removes the temps.
func mergeTempFiles(destPath string, tempPaths []string) error {
	dest, err := os.OpenFile(destPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open dest %q: %w", destPath, err)
	}
	defer dest.Close()

	for _, tp := range tempPaths {
		if tp == "" {
			continue
		}
		src, openErr := os.Open(tp)
		if openErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: open temp %q: %v", tp, openErr)
			continue
		}
		_, copyErr := io.Copy(dest, src)
		src.Close()
		os.Remove(tp)
		if copyErr != nil {
			return fmt.Errorf("copy temp %q: %w", tp, copyErr)
		}
	}
	return nil
}

func (gen *manifestGenerator) writeCollectedPagesAsTSV(dirPath string, pages []*listDirectoryOutputStruct) {
	if len(pages) == 0 {
		return
	}

	partPath := manifestPartPath(gen.manifestDir, dirPath)

	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		globals.logger.Printf("[WARN] manifest-gen: mkdir for %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}

	f, err := os.Create(partPath)
	if err != nil {
		globals.logger.Printf("[WARN] manifest-gen: create %q: %v", partPath, err)
		gen.errors.Add(1)
		return
	}

	w := bufio.NewWriterSize(f, manifestWriterBufSize)
	defer func() {
		if flushErr := w.Flush(); flushErr != nil {
			gen.errors.Add(1)
		}
		if closeErr := f.Close(); closeErr != nil {
			gen.errors.Add(1)
		}
	}()

	for _, page := range pages {
		for _, file := range page.file {
			fmt.Fprintf(w, "f\t%s\t%d\t%s\t%s\n",
				file.basename, file.size, file.eTag,
				file.mTime.UTC().Format(time.RFC3339Nano))
			gen.totalFiles.Add(1)
			gen.totalBytes.Add(int64(file.size))
		}
	}
}

// --- Lexicographic interpolation ---

func commonPrefix(a, b string) string {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := range n {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a[:n]
}

const (
	lexBase    = 95   // Printable ASCII: 0x20 (' ') through 0x7E ('~')
	lexMinByte = 0x20 // Space — lowest printable ASCII
)

// `lexInterpolate` computes n-1 evenly spaced split points between strings a and b.
// Uses base-95 (printable ASCII) arithmetic so all output is valid for S3 keys.
func lexInterpolate(a, b string, n int) []string {
	if n <= 1 {
		return []string{a}
	}

	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	maxLen += 2

	aBig := stringToBase95Int(a, maxLen)
	bBig := stringToBase95Int(b, maxLen)

	diff := new(big.Int).Sub(bBig, aBig)
	if diff.Sign() <= 0 {
		return []string{a}
	}

	nBig := big.NewInt(int64(n))
	splits := make([]string, 0, n-1)

	for i := 1; i < n; i++ {
		offset := new(big.Int).Mul(diff, big.NewInt(int64(i)))
		offset.Div(offset, nBig)
		point := new(big.Int).Add(aBig, offset)
		splits = append(splits, base95IntToString(point, maxLen))
	}

	return splits
}

func stringToBase95Int(s string, padLen int) *big.Int {
	result := big.NewInt(0)
	base := big.NewInt(lexBase)
	for i := range padLen {
		result.Mul(result, base)
		if i < len(s) {
			ch := int64(s[i]) - lexMinByte
			if ch < 0 {
				ch = 0
			} else if ch >= lexBase {
				ch = lexBase - 1
			}
			result.Add(result, big.NewInt(ch))
		}
	}
	return result
}

func base95IntToString(n *big.Int, length int) string {
	digits := make([]byte, length)
	val := new(big.Int).Set(n)
	base := big.NewInt(lexBase)
	mod := new(big.Int)
	for i := length - 1; i >= 0; i-- {
		val.DivMod(val, base, mod)
		digits[i] = byte(mod.Int64()) + lexMinByte
	}
	end := length
	for end > 0 && digits[end-1] == lexMinByte {
		end--
	}
	if end == 0 {
		return string([]byte{lexMinByte})
	}
	return string(digits[:end])
}

// `readManifestPart` reads all entries from a per-directory TSV manifest file.
func readManifestPart(partPath string) (entries []manifestDirEntry, err error) {
	var (
		f        *os.File
		scanner  *bufio.Scanner
		line     string
		fields   []string
		size     uint64
		mTime    time.Time
		parseErr error
	)

	f, err = os.Open(partPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)

	entries = make([]manifestDirEntry, 0, 1024)
	for scanner.Scan() {
		line = scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields = strings.SplitN(line, "\t", 5)
		if len(fields) != 5 {
			continue
		}

		size, parseErr = strconv.ParseUint(fields[2], 10, 64)
		if parseErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: malformed size in %q: fields=%v err=%v", partPath, fields, parseErr)
		}
		mTime, parseErr = time.Parse(time.RFC3339Nano, fields[4])
		if parseErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: malformed mtime in %q: fields=%v err=%v", partPath, fields, parseErr)
		}

		entries = append(entries, manifestDirEntry{
			Kind:     fields[0],
			Basename: fields[1],
			Size:     size,
			ETag:     fields[3],
			MTime:    mTime,
		})
	}

	return entries, scanner.Err()
}

// `lookupInManifestPart` scans a per-directory TSV for one basename.
func lookupInManifestPart(partPath, basename string) (entry manifestDirEntry, found bool) {
	var (
		f        *os.File
		scanner  *bufio.Scanner
		line     string
		fields   []string
		size     uint64
		mTime    time.Time
		err      error
		parseErr error
	)

	f, err = os.Open(partPath)
	if err != nil {
		return
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)

	for scanner.Scan() {
		line = scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		fields = strings.SplitN(line, "\t", 5)
		if len(fields) != 5 {
			continue
		}

		if fields[1] != basename {
			continue
		}

		size, parseErr = strconv.ParseUint(fields[2], 10, 64)
		if parseErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: malformed size in %q: fields=%v err=%v", partPath, fields, parseErr)
		}
		mTime, parseErr = time.Parse(time.RFC3339Nano, fields[4])
		if parseErr != nil {
			globals.logger.Printf("[WARN] manifest-gen: malformed mtime in %q: fields=%v err=%v", partPath, fields, parseErr)
		}

		entry = manifestDirEntry{
			Kind:     fields[0],
			Basename: fields[1],
			Size:     size,
			ETag:     fields[3],
			MTime:    mTime,
		}
		found = true
		return
	}

	return
}

// `writeManifestIndex` writes the manifest_index.json metadata file.
func writeManifestIndex(manifestDir, bucket, prefix string, totalObjects, totalDirs int64) error {
	var (
		index manifestIndex
		data  []byte
		err   error
	)

	index = manifestIndex{
		Version:          manifestVersion,
		Format:           "tsv",
		Bucket:           bucket,
		Prefix:           prefix,
		TotalObjects:     totalObjects,
		TotalDirectories: totalDirs,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
	}

	data, err = json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(manifestDir, "manifest_index.json"), data, 0o600)
}

// `formatBytes` is called to format a byte count as a human-readable IEC string.
func formatBytes(b uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TiB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// `backendNames` is called to return the configured backend mount names for diagnostics.
func backendNames() (names []string) {
	names = make([]string, 0, len(globals.backendsToMount))
	for name := range globals.backendsToMount {
		names = append(names, name)
	}
	return names
}
