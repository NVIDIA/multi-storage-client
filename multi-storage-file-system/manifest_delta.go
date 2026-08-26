package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	manifestDeltaDirName  = "_msfs_delta"
	manifestDeltaFileName = "delta.tsv"
	manifestDeltaUpsert   = "u"
	manifestDeltaDelete   = "d"

	// Consumers test for the directory kind and treat every other value as a
	// file, so an unrecognized kind would silently become a file entry.
	manifestEntryKindFile = "f"
	manifestEntryKindDir  = "d"
)

type manifestDeltaRecord struct {
	Op         string
	Entry      manifestDirEntry
	ParentPath string
}

func manifestDeltaPath(manifestDir string) string {
	return filepath.Join(manifestDir, manifestDeltaDirName, manifestDeltaFileName)
}

func appendManifestDeltaForInodeLocked(backend *backendStruct, inode *inodeStruct, op string) {
	if backend == nil || backend.manifestPath == "" || inode == nil {
		return
	}
	if inode.inodeType != FileObject && inode.inodeType != PseudoDir {
		return
	}

	// An empty parentPath means the backend root, so it cannot double as "parent
	// not found" -- a record misfiled at the root reconstructs the entry in the
	// wrong directory, and a tombstone there fails to hide the real base entry.
	// objectPath is built as parentPath + basename, so trimming recovers the
	// parent without the parent inode being resident.
	var parentPath string
	if parentInode, ok := globals.inodeMap.get(inode.parentInodeNumber); ok {
		parentPath = parentInode.objectPath
	} else {
		suffix := inode.basename
		if inode.inodeType == PseudoDir {
			suffix += "/"
		}
		parentPath = strings.TrimSuffix(inode.objectPath, suffix)
	}

	entry := manifestDirEntry{
		Kind:     manifestEntryKindFile,
		Basename: inode.basename,
		Size:     inode.sizeInMemory,
		ETag:     inode.eTag,
		MTime:    inode.mTime,
	}
	if inode.inodeType == PseudoDir {
		entry.Kind = manifestEntryKindDir
		entry.Size = 0
		entry.ETag = ""
	}
	if err := appendManifestDeltaRecord(backend.manifestPath, parentPath, op, &entry); err != nil {
		globals.logger.Printf("[WARN] append manifest delta failed for %q: %v", inode.objectPath, err)
	}
}

func appendManifestDeltaRecord(manifestDir, parentPath, op string, entry *manifestDirEntry) error {
	if manifestDir == "" {
		return nil
	}
	if op != manifestDeltaUpsert && op != manifestDeltaDelete {
		return fmt.Errorf("invalid manifest delta op %q", op)
	}
	deltaDir := filepath.Join(manifestDir, manifestDeltaDirName)
	if err := os.MkdirAll(deltaDir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(manifestDeltaPath(manifestDir), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	mTime := entry.MTime
	if mTime.IsZero() {
		mTime = time.Now()
	}
	_, err = fmt.Fprintf(f, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
		op,
		entry.Kind,
		escapeManifestDeltaField(parentPath),
		escapeManifestDeltaField(entry.Basename),
		entry.Size,
		escapeManifestDeltaField(entry.ETag),
		mTime.UTC().Format(time.RFC3339Nano))
	return err
}

// A path component may contain any byte but "/" and NUL, so a tab would shift
// every following field and a newline would split the record in two. Either way
// the record is unparseable and the mutation it recorded is lost.
func escapeManifestDeltaField(field string) string {
	if !strings.ContainsAny(field, "\\\t\n\r") {
		return field
	}
	var builder strings.Builder
	builder.Grow(len(field) + 8)
	for _, b := range []byte(field) {
		switch b {
		case '\\':
			builder.WriteString(`\\`)
		case '\t':
			builder.WriteString(`\t`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		default:
			builder.WriteByte(b)
		}
	}
	return builder.String()
}

func unescapeManifestDeltaField(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var builder strings.Builder
	builder.Grow(len(field))
	for index := 0; index < len(field); index++ {
		if field[index] != '\\' || index+1 >= len(field) {
			builder.WriteByte(field[index])
			continue
		}
		index++
		switch field[index] {
		case '\\':
			builder.WriteByte('\\')
		case 't':
			builder.WriteByte('\t')
		case 'n':
			builder.WriteByte('\n')
		case 'r':
			builder.WriteByte('\r')
		default:
			builder.WriteByte('\\')
			builder.WriteByte(field[index])
		}
	}
	return builder.String()
}

// A torn or malformed line — a crash between the object commit and the delta
// flush can leave one — is reported as not ok and dropped by every caller.
func parseManifestDeltaLine(line string) (manifestDeltaRecord, bool) {
	if line == "" || strings.HasPrefix(line, "#") {
		return manifestDeltaRecord{}, false
	}
	fields := strings.SplitN(line, "\t", 7)
	if len(fields) != 7 {
		return manifestDeltaRecord{}, false
	}
	// The writer rejects an unknown op, so one appearing here means a corrupted
	// record, and admitting it would apply an operation nothing wrote.
	if fields[0] != manifestDeltaUpsert && fields[0] != manifestDeltaDelete {
		globals.logger.Printf("[WARN] manifest-delta: unsupported op fields=%v", fields)
		return manifestDeltaRecord{}, false
	}
	// Kind decides what an upsert creates, and consumers read anything but the
	// directory kind as a file. A tombstone drops an entry without consulting its
	// kind and may legitimately carry none, so screening one on that field would
	// discard the delete and bring the object back.
	if fields[0] == manifestDeltaUpsert && fields[1] != manifestEntryKindFile && fields[1] != manifestEntryKindDir {
		globals.logger.Printf("[WARN] manifest-delta: unsupported kind fields=%v", fields)
		return manifestDeltaRecord{}, false
	}
	size, parseErr := strconv.ParseUint(fields[4], 10, 64)
	if parseErr != nil {
		globals.logger.Printf("[WARN] manifest-delta: malformed size fields=%v err=%v", fields, parseErr)
		return manifestDeltaRecord{}, false
	}
	mTime, parseErr := time.Parse(time.RFC3339Nano, fields[6])
	if parseErr != nil {
		globals.logger.Printf("[WARN] manifest-delta: malformed mtime fields=%v err=%v", fields, parseErr)
		return manifestDeltaRecord{}, false
	}
	return manifestDeltaRecord{
		Op:         fields[0],
		ParentPath: unescapeManifestDeltaField(fields[2]),
		Entry: manifestDirEntry{
			Kind:     fields[1],
			Basename: unescapeManifestDeltaField(fields[3]),
			Size:     size,
			ETag:     unescapeManifestDeltaField(fields[5]),
			MTime:    mTime,
		},
	}, true
}

func newManifestDeltaScanner(f *os.File) *bufio.Scanner {
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	return scanner
}

func readManifestDeltasForParent(manifestDir, parentPath string) (map[string]manifestDeltaRecord, error) {
	f, err := os.Open(manifestDeltaPath(manifestDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	records := make(map[string]manifestDeltaRecord)
	scanner := newManifestDeltaScanner(f)
	for scanner.Scan() {
		record, ok := parseManifestDeltaLine(scanner.Text())
		if !ok || record.ParentPath != parentPath {
			continue
		}
		records[record.Entry.Basename] = record
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

// `readAllManifestDeltas` parses the log once and groups records by parent path,
// keeping the newest per basename. Bulk callers that visit every directory use
// this instead of one full-log scan per directory, and the returned parent set
// also identifies directories that exist only in the log.
//
// The result is read-only once returned, so concurrent readers may share it.
func readAllManifestDeltas(manifestDir string) (map[string]map[string]manifestDeltaRecord, error) {
	f, err := os.Open(manifestDeltaPath(manifestDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	byParent := make(map[string]map[string]manifestDeltaRecord)
	scanner := newManifestDeltaScanner(f)
	for scanner.Scan() {
		record, ok := parseManifestDeltaLine(scanner.Text())
		if !ok {
			continue
		}
		records, found := byParent[record.ParentPath]
		if !found {
			records = make(map[string]manifestDeltaRecord)
			byParent[record.ParentPath] = records
		}
		records[record.Entry.Basename] = record
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return byParent, nil
}

// A directory with no base part is not an error: it can exist only in the delta
// log, and its records still have to be overlaid onto an empty base.
func readManifestBasePart(manifestDir, parentPath string) ([]manifestDirEntry, error) {
	entries, err := readManifestPart(manifestPartPath(manifestDir, parentPath))
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		return make([]manifestDirEntry, 0), nil
	}
	return entries, nil
}

func readManifestPartWithDelta(manifestDir, parentPath string) ([]manifestDirEntry, error) {
	entries, err := readManifestBasePart(manifestDir, parentPath)
	if err != nil {
		return nil, err
	}
	return applyManifestDeltas(manifestDir, parentPath, entries)
}

// `readManifestPartWithDeltaRecords` overlays records already parsed by
// `readAllManifestDeltas` rather than re-reading the log for this directory.
func readManifestPartWithDeltaRecords(manifestDir, parentPath string, deltas map[string]manifestDeltaRecord) ([]manifestDirEntry, error) {
	entries, err := readManifestBasePart(manifestDir, parentPath)
	if err != nil {
		return nil, err
	}
	return applyManifestDeltaRecords(entries, deltas), nil
}

func lookupInManifestPartWithDelta(manifestDir, parentPath, basename string) (manifestDirEntry, bool) {
	deltas, err := readManifestDeltasForParent(manifestDir, parentPath)
	if err == nil {
		if record, ok := deltas[basename]; ok {
			if record.Op == manifestDeltaDelete {
				return manifestDirEntry{}, false
			}
			return record.Entry, true
		}
	} else {
		globals.logger.Printf("[WARN] manifest-delta: read failed for %q: %v", parentPath, err)
	}
	return lookupInManifestPart(manifestPartPath(manifestDir, parentPath), basename)
}

func applyManifestDeltas(manifestDir, parentPath string, entries []manifestDirEntry) ([]manifestDirEntry, error) {
	deltas, err := readManifestDeltasForParent(manifestDir, parentPath)
	if err != nil {
		return entries, err
	}
	return applyManifestDeltaRecords(entries, deltas), nil
}

func applyManifestDeltaRecords(entries []manifestDirEntry, deltas map[string]manifestDeltaRecord) []manifestDirEntry {
	if len(deltas) == 0 {
		return entries
	}

	seen := make(map[string]struct{}, len(entries))
	out := make([]manifestDirEntry, 0, len(entries)+len(deltas))
	for _, entry := range entries {
		seen[entry.Basename] = struct{}{}
		record, ok := deltas[entry.Basename]
		if !ok {
			out = append(out, entry)
			continue
		}
		if record.Op == manifestDeltaDelete {
			continue
		}
		out = append(out, record.Entry)
	}

	var additions []manifestDirEntry
	for basename, record := range deltas {
		if record.Op == manifestDeltaDelete {
			continue
		}
		if _, ok := seen[basename]; ok {
			continue
		}
		additions = append(additions, record.Entry)
	}
	sort.Slice(additions, func(i, j int) bool {
		return additions[i].Basename < additions[j].Basename
	})
	out = append(out, additions...)
	return out
}
