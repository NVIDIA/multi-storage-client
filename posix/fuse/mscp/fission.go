package main

import (
	"fmt"
	"math"
	"syscall"

	"github.com/NVIDIA/fission/v3"
)

const (
	fuseSubtype = "msc-posix"

	initOutFlags = uint32(0) |
		fission.InitFlagsAsyncRead |
		fission.InitFlagsFileOps |
		fission.InitFlagsAtomicOTrunc |
		fission.InitFlagsExportSupport |
		fission.InitFlagsBigWrites |
		fission.InitFlagsAutoInvalData |
		fission.InitFlagsDoReadDirPlus |
		fission.InitFlagsParallelDirops

	initOutMaxBackgound         = uint16(100)
	initOutCongestionThreshhold = uint16(0)

	maxPages = 256                     // * 4KiB page size == 1MiB... the max read or write size in Linux FUSE at this time
	maxRead  = uint32(maxPages * 4096) //                     1MiB... the max read          size in Linux FUSE at this time
	maxWrite = uint32(maxPages * 4096) //                     1MiB... the max         write size in Linux FUSE at this time

	attrBlkSize   = uint32(512)
	statFSBlkSize = uint64(1024)

	maxNameLen = uint32(4096)
)

func performFissionMount() (err error) {
	globals.fissionVolume = fission.NewVolume(globals.config.mountName, globals.config.mountPoint, fuseSubtype, maxRead, maxWrite, true, globals.config.allowOther, &globals, globals.logger, globals.errChan)

	err = globals.fissionVolume.DoMount()

	return
}

func performFissionUnmount() (err error) {
	err = globals.fissionVolume.DoUnmount()

	return
}

func fixAttrSizes(attr *fission.Attr) {
	if syscall.S_IFREG == (attr.Mode & syscall.S_IFMT) {
		attr.Blocks = attr.Size + (uint64(attrBlkSize) - 1)
		attr.Blocks /= uint64(attrBlkSize)
		attr.BlkSize = attrBlkSize
		attr.NLink = 1
	} else {
		attr.Size = 0
		attr.Blocks = 0
		attr.BlkSize = 0
		attr.NLink = 2
	}
}

func fixStatXSizes(statX *fission.StatX) {
	if syscall.S_IFREG == (statX.Mode & syscall.S_IFMT) {
		statX.Blocks = statX.Size + (uint64(attrBlkSize) - 1)
		statX.Blocks /= uint64(attrBlkSize)
		statX.BlkSize = attrBlkSize
		statX.NLink = 1
	} else {
		statX.Size = 0
		statX.Blocks = 0
		statX.BlkSize = 0
		statX.NLink = 2
	}
}

func (inode *inodeStruct) dirEntType() (dirEntType uint32) {
	if inode.inodeType == FileObject {
		dirEntType = syscall.DT_REG
	} else {
		dirEntType = syscall.DT_DIR
	}

	return
}

func (*globalsStruct) DoLookup(inHeader *fission.InHeader, lookupIn *fission.LookupIn) (lookupOut *fission.LookupOut, errno syscall.Errno) {
	var (
		childInode         *inodeStruct
		childInodeNumber   uint64
		entryAttrValidNSec uint32
		entryAttrValidSec  uint64
		mTimeNSec          uint32
		mTimeSec           uint64
		ok                 bool
		parentInode        *inodeStruct
	)

	globals.Lock()

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		// We no longer know how to map inHeader.NodeID (an inodeNumber) to the parentInode
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if parentInode.inodeType == FileObject {
		// The parentInode must be a directory of some sort... not a FileObject
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	if parentInode.inodeType == FUSERootDir {
		// If lookupIn.Name exists, it is in parentInode.virtChildDirMap

		childInodeNumber, ok = parentInode.virtChildDirMap.GetByKey(string(lookupIn.Name))
		if !ok {
			globals.Unlock()
			errno = syscall.ENOENT
			return
		}

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok")
		}
	} else {
		// We only know parentInode is a BackendRootDir or a PseudoDir

		childInode, ok = parentInode.findChildInode(string(lookupIn.Name))
		if !ok {
			globals.Unlock()
			errno = syscall.ENOENT
			return
		}
	}

	entryAttrValidSec, entryAttrValidNSec = timeDurationToAttrDuration(globals.config.entryAttrTTL)
	mTimeSec, mTimeNSec = timeTimeToAttrTime(childInode.mTime)

	lookupOut = &fission.LookupOut{
		EntryOut: fission.EntryOut{
			NodeID:         childInode.inodeNumber,
			Generation:     0,
			EntryValidSec:  entryAttrValidSec,
			AttrValidSec:   entryAttrValidSec,
			EntryValidNSec: entryAttrValidNSec,
			AttrValidNSec:  entryAttrValidNSec,
			Attr: fission.Attr{
				Ino:       childInode.inodeNumber,
				Size:      childInode.sizeInMemory,
				ATimeSec:  mTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  mTimeSec,
				ATimeNSec: mTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: mTimeNSec,
				Mode:      childInode.mode,
				UID:       uint32(childInode.backend.uid),
				GID:       uint32(childInode.backend.gid),
				RDev:      0,
				Padding:   0,
			},
		},
	}
	fixAttrSizes(&lookupOut.Attr)

	globals.Unlock()

	errno = 0
	return
}

func (*globalsStruct) DoForget(inHeader *fission.InHeader, forgetIn *fission.ForgetIn) {}

func (*globalsStruct) DoGetAttr(inHeader *fission.InHeader, getAttrIn *fission.GetAttrIn) (getAttrOut *fission.GetAttrOut, errno syscall.Errno) {
	var (
		attrValidNSec uint32
		attrValidSec  uint64
		gid           uint32
		mTimeNSec     uint32
		mTimeSec      uint64
		ok            bool
		thisInode     *inodeStruct
		uid           uint32
	)

	globals.Lock()

	thisInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	switch thisInode.inodeType {
	case FileObject:
		uid = uint32(thisInode.backend.uid)
		gid = uint32(thisInode.backend.gid)
	case FUSERootDir:
		uid = uint32(globals.config.uid)
		gid = uint32(globals.config.gid)
	case BackendRootDir:
		uid = uint32(thisInode.backend.uid)
		gid = uint32(thisInode.backend.gid)
	case PseudoDir:
		uid = uint32(thisInode.backend.uid)
		gid = uint32(thisInode.backend.gid)
	default:
		globals.logger.Fatalf("unrecognized inodeType (%v)", thisInode.inodeType)
	}

	attrValidSec, attrValidNSec = timeDurationToAttrDuration(globals.config.entryAttrTTL)
	mTimeSec, mTimeNSec = timeTimeToAttrTime(thisInode.mTime)

	getAttrOut = &fission.GetAttrOut{
		AttrValidSec:  attrValidSec,
		AttrValidNSec: attrValidNSec,
		Dummy:         0,
		Attr: fission.Attr{
			Ino:       thisInode.inodeNumber,
			Size:      thisInode.sizeInMemory,
			ATimeSec:  mTimeSec,
			MTimeSec:  mTimeSec,
			CTimeSec:  mTimeSec,
			ATimeNSec: mTimeNSec,
			MTimeNSec: mTimeNSec,
			CTimeNSec: mTimeNSec,
			Mode:      thisInode.mode,
			UID:       uid,
			GID:       gid,
			RDev:      0,
			Padding:   0,
		},
	}
	fixAttrSizes(&getAttrOut.Attr)

	globals.Unlock()

	errno = 0
	return
}

func (*globalsStruct) DoSetAttr(inHeader *fission.InHeader, setAttrIn *fission.SetAttrIn) (setAttrOut *fission.SetAttrOut, errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoSetAttr()")
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoReadLink(inHeader *fission.InHeader) (readLinkOut *fission.ReadLinkOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoSymLink(inHeader *fission.InHeader, symLinkIn *fission.SymLinkIn) (symLinkOut *fission.SymLinkOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoMkNod(inHeader *fission.InHeader, mkNodIn *fission.MkNodIn) (mkNodOut *fission.MkNodOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoMkDir(inHeader *fission.InHeader, mkDirIn *fission.MkDirIn) (mkDirOut *fission.MkDirOut, errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoMkDir()")
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoUnlink(inHeader *fission.InHeader, unlinkIn *fission.UnlinkIn) (errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoUnlink()")
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoRmDir(inHeader *fission.InHeader, rmDirIn *fission.RmDirIn) (errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoRmDir()")
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoRename(inHeader *fission.InHeader, renameIn *fission.RenameIn) (errno syscall.Errno) {
	errno = syscall.EXDEV
	return
}

func (*globalsStruct) DoLink(inHeader *fission.InHeader, linkIn *fission.LinkIn) (linkOut *fission.LinkOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoOpen(inHeader *fission.InHeader, openIn *fission.OpenIn) (openOut *fission.OpenOut, errno syscall.Errno) {
	var (
		allowReads   bool
		allowWrites  bool
		appendWrites bool
		fh           *fhStruct
		inode        *inodeStruct
		isExclusive  bool
		ok           bool
	)

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if inode.inodeType != FileObject {
		globals.Unlock()
		errno = syscall.EISDIR
		return
	}

	if len(inode.fhMap) == 1 {
		for _, fh = range inode.fhMap {
			// Note that, due to the above if, this "loop" will execute exactly once

			if fh.isExclusive {
				globals.Unlock()
				errno = syscall.EACCES
				return
			}
		}
	}

	isExclusive = (openIn.Flags & fission.FOpenRequestEXCL) == fission.FOpenRequestEXCL
	allowReads = (openIn.Flags & (fission.FOpenRequestRDONLY | fission.FOpenRequestWRONLY | fission.FOpenRequestRDWR)) != fission.FOpenRequestWRONLY
	allowWrites = (openIn.Flags & (fission.FOpenRequestRDONLY | fission.FOpenRequestWRONLY | fission.FOpenRequestRDWR)) != fission.FOpenRequestRDONLY
	appendWrites = allowWrites && ((openIn.Flags & fission.FOpenRequestAPPEND) == fission.FOpenRequestAPPEND)

	if allowWrites && inode.backend.readOnly {
		globals.Unlock()
		errno = syscall.EACCES
		return
	}

	fh = &fhStruct{
		nonce:        fetchNonce(),
		inode:        inode,
		isExclusive:  isExclusive,
		allowReads:   allowReads,
		allowWrites:  allowWrites,
		appendWrites: appendWrites,
	}

	inode.fhMap[fh.nonce] = fh

	openOut = &fission.OpenOut{
		FH:        fh.nonce,
		OpenFlags: 0,
		Padding:   0,
	}

	globals.Unlock()

	errno = 0
	return
}

func (*globalsStruct) DoRead(inHeader *fission.InHeader, readIn *fission.ReadIn) (readOut *fission.ReadOut, errno syscall.Errno) {
	var (
		cacheLine            *cacheLineStruct
		cacheLineNumber      uint64
		cacheLineOffsetLimit uint64 // One greater than offset to last byte to return
		cacheLineOffsetStart uint64
		curOffset            = readIn.Offset
		fh                   *fhStruct
		inode                *inodeStruct
		ok                   bool
	)

	readOut = &fission.ReadOut{
		Data: make([]byte, 0, readIn.Size),
	}

	for len(readOut.Data) < cap(readOut.Data) {
		globals.Lock()

		inode, ok = globals.inodeMap[inHeader.NodeID]
		if !ok {
			globals.Unlock()
			errno = syscall.ENOENT
			return
		}
		if inode.inodeType != FileObject {
			globals.Unlock()
			errno = syscall.EBADF
			return
		}

		fh, ok = inode.fhMap[readIn.FH]
		if !ok {
			globals.Unlock()
			errno = syscall.EBADF
			return
		}
		if !fh.allowReads {
			globals.Unlock()
			errno = syscall.EBADF
			return
		}

		cacheLineNumber = curOffset / globals.config.cacheLineSize

		cacheLine, ok = inode.cache[cacheLineNumber]
		if !ok {
			cacheLine = &cacheLineStruct{
				state:       CacheLineInbound,
				inodeNumber: inode.inodeNumber,
				lineNumber:  cacheLineNumber,
			}

			inode.cache[cacheLineNumber] = cacheLine

			globals.inboundCacheLineCount++

			cacheLine.Add(1)
			go cacheLine.fetch()

			globals.Unlock()

			cacheLine.Wait()

			continue
		}

		if cacheLine.state == CacheLineInbound {
			globals.Unlock()

			cacheLine.Wait()

			continue
		}

		cacheLine.touch()

		cacheLineOffsetStart = curOffset - (cacheLineNumber * globals.config.cacheLineSize)

		cacheLineOffsetLimit = cacheLineOffsetStart + uint64((cap(readOut.Data) - len(readOut.Data)))
		if cacheLineOffsetLimit > globals.config.cacheLineSize {
			cacheLineOffsetLimit = globals.config.cacheLineSize
		}
		if cacheLineOffsetLimit > uint64(len(cacheLine.content)) {
			cacheLineOffsetLimit = uint64(len(cacheLine.content))
		}

		if cacheLineOffsetLimit == cacheLineOffsetStart {
			// We have reached EOF

			globals.Unlock()

			break
		}

		readOut.Data = append(readOut.Data, cacheLine.content[cacheLineOffsetStart:cacheLineOffsetLimit]...)
		curOffset += cacheLineOffsetLimit - cacheLineOffsetStart

		globals.Unlock()
	}

	errno = 0
	return
}

func (*globalsStruct) DoWrite(inHeader *fission.InHeader, writeIn *fission.WriteIn) (writeOut *fission.WriteOut, errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoWrite()")
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoStatFS(inHeader *fission.InHeader) (statFSOut *fission.StatFSOut, errno syscall.Errno) {
	globals.Lock()

	statFSOut = &fission.StatFSOut{
		KStatFS: fission.KStatFS{
			Blocks:  uint64(math.MaxUint64) / statFSBlkSize,
			BFree:   uint64(math.MaxUint64) / statFSBlkSize,
			BAvail:  uint64(math.MaxUint64) / statFSBlkSize,
			Files:   uint64(len(globals.inodeMap)),
			FFree:   uint64(math.MaxUint64) - globals.lastNonce,
			BSize:   uint32(globals.config.cacheLineSize),
			NameLen: maxNameLen,
			FRSize:  uint32(globals.config.cacheLineSize),
			Padding: 0,
			Spare:   [6]uint32{0, 0, 0, 0, 0, 0},
		},
	}

	globals.Unlock()

	errno = 0
	return
}

func (*globalsStruct) DoRelease(inHeader *fission.InHeader, releaseIn *fission.ReleaseIn) (errno syscall.Errno) {
	var (
		fh    *fhStruct
		inode *inodeStruct
		ok    bool
	)

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if inode.inodeType != FileObject {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}

	fh, ok = inode.fhMap[releaseIn.FH]
	if !ok {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}

	delete(inode.fhMap, fh.nonce)

	globals.Unlock()

	errno = 0
	return
}

func (*globalsStruct) DoFSync(inHeader *fission.InHeader, fSyncIn *fission.FSyncIn) (errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoFSync()")
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoSetXAttr(inHeader *fission.InHeader, setXAttrIn *fission.SetXAttrIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoGetXAttr(inHeader *fission.InHeader, getXAttrIn *fission.GetXAttrIn) (getXAttrOut *fission.GetXAttrOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoListXAttr(inHeader *fission.InHeader, listXAttrIn *fission.ListXAttrIn) (listXAttrOut *fission.ListXAttrOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoRemoveXAttr(inHeader *fission.InHeader, removeXAttrIn *fission.RemoveXAttrIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoFlush(inHeader *fission.InHeader, flushIn *fission.FlushIn) (errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoFlush()")
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoInit(inHeader *fission.InHeader, initIn *fission.InitIn) (initOut *fission.InitOut, errno syscall.Errno) {
	initOut = &fission.InitOut{
		Major:                initIn.Major,
		Minor:                initIn.Minor,
		MaxReadAhead:         initIn.MaxReadAhead,
		Flags:                initOutFlags,
		MaxBackground:        initOutMaxBackgound,
		CongestionThreshhold: initOutCongestionThreshhold,
		MaxWrite:             maxWrite,
		TimeGran:             0, // accept default
		MaxPages:             maxPages,
		MapAlignment:         0, // accept default
		Flags2:               0,
		MaxStackDepth:        0,
		RequestTimeout:       0,
		Unused:               [11]uint16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}

	errno = 0
	return
}

func (*globalsStruct) DoOpenDir(inHeader *fission.InHeader, openDirIn *fission.OpenDirIn) (openDirOut *fission.OpenDirOut, errno syscall.Errno) {
	var (
		fh    *fhStruct
		inode *inodeStruct
		ok    bool
	)

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if inode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	if inode.inodeType == FUSERootDir {
		fh = &fhStruct{
			nonce: fetchNonce(),
			inode: inode,
		}
	} else {
		fh = &fhStruct{
			nonce:                                 fetchNonce(),
			inode:                                 inode,
			listDirectoryInProgress:               false,
			listDirectorySequenceDone:             false,
			prevListDirectoryOutput:               nil,
			prevListDirectoryOutputFileLen:        0,
			prevListDirectoryOutputStartingOffset: 0,
			nextListDirectoryOutput:               nil,
			nextListDirectoryOutputFileLen:        0,
			nextListDirectoryOutputStartingOffset: 0,
			listDirectorySubdirectorySet:          make(map[string]struct{}),
			listDirectorySubdirectoryList:         make([]string, 0),
		}
	}

	inode.fhMap[fh.nonce] = fh

	openDirOut = &fission.OpenDirOut{
		FH:        fh.nonce,
		OpenFlags: 0,
		Padding:   0,
	}

	globals.Unlock()

	errno = 0
	return
}

func (inode *inodeStruct) appendToReadDirOut(readDirInSize uint64, readDirOut *fission.ReadDirOut, dirEntOff uint64, basename string, curReadDirOutSize *uint64) (ok bool) {
	var (
		dirEntSize uint64
	)

	dirEntSize = fission.DirEntFixedPortionSize + uint64(len(basename)) + fission.DirEntAlignment - 1
	dirEntSize /= fission.DirEntAlignment
	dirEntSize *= fission.DirEntAlignment

	if (*curReadDirOutSize + dirEntSize) > readDirInSize {
		ok = false
		return
	}

	*curReadDirOutSize += dirEntSize
	ok = true

	readDirOut.DirEnt = append(readDirOut.DirEnt, fission.DirEnt{
		Ino:     inode.inodeNumber,
		Off:     dirEntOff,
		NameLen: uint32(len(basename)),
		Type:    inode.dirEntType(),
		Name:    []byte(basename),
	})

	return
}

func (*globalsStruct) DoReadDir(inHeader *fission.InHeader, readDirIn *fission.ReadDirIn) (readDirOut *fission.ReadDirOut, errno syscall.Errno) {
	var (
		childInode                                  *inodeStruct
		childInodeBasename                          string
		childInodeNumber                            uint64
		curOffset                                   uint64
		curOffsetInNextListDirectoryOutputCap       uint64
		curOffsetInListDirectorySubdirectoryListCap uint64
		curOffsetInPrevListDirectoryOutputCap       uint64
		curOffsetInVirtChildDirMapCap               uint64
		curOffsetInVirtChildFileMapCap              uint64
		curReadDirOutSize                           uint64
		dirEntCountMax                              uint64
		dirEntMinSize                               uint64
		err                                         error
		fh                                          *fhStruct
		listDirectoryOutputFile                     *listDirectoryOutputFileStruct
		listDirectoryInput                          *listDirectoryInputStruct
		listDirectoryOutput                         *listDirectoryOutputStruct
		ok                                          bool
		parentInode                                 *inodeStruct
		subdirectory                                string
		virtChildDirMapIndex                        int
		virtChildDirMapLen                          uint64
		virtChildFileMapIndex                       int
	)

	dirEntMinSize = fission.DirEntFixedPortionSize + 1 + fission.DirEntAlignment - 1
	dirEntMinSize /= fission.DirEntAlignment
	dirEntMinSize *= fission.DirEntAlignment
	dirEntCountMax = uint64(readDirIn.Size) / dirEntMinSize

	readDirOut = &fission.ReadDirOut{
		DirEnt: make([]fission.DirEnt, 0, dirEntCountMax),
	}

	curReadDirOutSize = 0
	curOffset = readDirIn.Offset

	globals.Lock()

Restart:

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if parentInode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	fh, ok = parentInode.fhMap[readDirIn.FH]
	if !ok {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}

	if parentInode.inodeType == FUSERootDir {
		virtChildDirMapLen = uint64(parentInode.virtChildDirMap.Len()) // Will be == 2 + len(globals.config.backends)

		for {
			if curOffset >= virtChildDirMapLen {
				globals.Unlock()
				errno = 0
				return
			}

			virtChildDirMapIndex = int(curOffset)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex)
			if !ok {
				globals.logger.Fatalf("parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex < virtChildDirMapLen) returned !ok")
			}

			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok")
			}

			curOffset++

			ok = childInode.appendToReadDirOut(uint64(readDirIn.Size), readDirOut, curOffset, childInodeBasename, &curReadDirOutSize)
			if !ok {
				globals.Unlock()
				errno = 0
				return
			}
		}
	}

	// If we reach here, we know parentInode.inodeType == BackendRootDir | PseudoDir

	if fh.listDirectoryInProgress {
		globals.Unlock()
		errno = syscall.EACCES
		return
	}

	if curOffset < fh.prevListDirectoryOutputStartingOffset {
		// Adjust curOffset to not try to reference before the start of fh.prevListDirectoryOutput

		curOffset = fh.prevListDirectoryOutputStartingOffset
	}

	for {
		if !fh.listDirectorySequenceDone && (curOffset >= (fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen)) {
			// Fetch the next listDirectoryOutput

			if fh.nextListDirectoryOutput != nil {
				fh.prevListDirectoryOutput = fh.nextListDirectoryOutput
				fh.prevListDirectoryOutputFileLen = fh.nextListDirectoryOutputFileLen
				fh.prevListDirectoryOutputStartingOffset = fh.nextListDirectoryOutputStartingOffset

				fh.nextListDirectoryOutput = nil
				fh.nextListDirectoryOutputFileLen = 0
			}

			if fh.prevListDirectoryOutput == nil {
				listDirectoryInput = &listDirectoryInputStruct{
					continuationToken: "",
					maxItems:          parentInode.backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			} else {
				listDirectoryInput = &listDirectoryInputStruct{
					continuationToken: fh.prevListDirectoryOutput.nextContinuationToken,
					maxItems:          parentInode.backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			}

			fh.listDirectoryInProgress = true

			globals.Unlock()

			listDirectoryOutput, err = parentInode.backend.context.listDirectory(listDirectoryInput)

			globals.Lock()

			fh.listDirectoryInProgress = false

			if err != nil {
				globals.Unlock()
				globals.logger.Printf("unable to access backend \"%s\"", parentInode.backend.dirName)
				errno = syscall.EACCES
				return
			}

			fh.listDirectorySequenceDone = !listDirectoryOutput.isTruncated

			if fh.prevListDirectoryOutput == nil {
				fh.prevListDirectoryOutput = listDirectoryOutput
				fh.prevListDirectoryOutputFileLen = uint64(len(listDirectoryOutput.file))
				fh.prevListDirectoryOutputStartingOffset = 0

				fh.nextListDirectoryOutput = nil
				fh.nextListDirectoryOutputFileLen = 0
				fh.nextListDirectoryOutputStartingOffset = fh.prevListDirectoryOutputFileLen
			} else {
				fh.nextListDirectoryOutput = listDirectoryOutput
				fh.nextListDirectoryOutputFileLen = uint64(len(listDirectoryOutput.file))
				fh.nextListDirectoryOutputStartingOffset = fh.prevListDirectoryOutputStartingOffset + fh.prevListDirectoryOutputFileLen
			}

			// Ensure we remember all discovered subdirectories and no longer refer to now physical subdirectories as virtual

			for _, subdirectory = range listDirectoryOutput.subdirectory {
				_ = parentInode.findChildDirInode(subdirectory)
				_, ok = fh.listDirectorySubdirectorySet[subdirectory]
				if !ok {
					fh.listDirectorySubdirectorySet[subdirectory] = struct{}{}
					fh.listDirectorySubdirectoryList = append(fh.listDirectorySubdirectoryList, subdirectory)
				}
			}

			// Since we had to release globals.Lock during listDirectory() call, we must restart from where we first grabbed it

			goto Restart
		}

		// At this point, we know either we are still reading fh.{prev|next}ListDirectoryOutput's
		// or we are done with all of them and may proceed to return fh.listDirectorySubdirectoryList
		// & parentInode.virtChild{Dir|File}Map entries

		curOffsetInPrevListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset
		curOffsetInNextListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen
		curOffsetInListDirectorySubdirectoryListCap = curOffsetInNextListDirectoryOutputCap + uint64(len(fh.listDirectorySubdirectoryList))
		curOffsetInVirtChildDirMapCap = curOffsetInListDirectorySubdirectoryListCap + uint64(parentInode.virtChildDirMap.Len())
		curOffsetInVirtChildFileMapCap = curOffsetInVirtChildDirMapCap + uint64(parentInode.virtChildFileMap.Len())

		switch {
		case curOffset < curOffsetInPrevListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.prevListDirectoryOutput.file[curOffset-fh.prevListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInNextListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.nextListDirectoryOutput.file[curOffset-fh.nextListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInListDirectorySubdirectoryListCap:
			childInode = parentInode.findChildDirInode(fh.listDirectorySubdirectoryList[curOffset-curOffsetInNextListDirectoryOutputCap])
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInVirtChildDirMapCap:
			virtChildDirMapIndex = int(curOffset - curOffsetInListDirectorySubdirectoryListCap)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex)
			if !ok {
				globals.logger.Fatalf("parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok")
			}
		case curOffset < curOffsetInVirtChildFileMapCap:
			virtChildFileMapIndex = int(curOffset - curOffsetInVirtChildDirMapCap)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildFileMap.GetByIndex(virtChildFileMapIndex)
			if !ok {
				globals.logger.Fatalf("parentInode.virtChildFileMap.GetByIndex(virtChildFileMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok")
			}
		default:
			globals.Unlock()
			errno = 0
			return
		}

		curOffset++

		ok = childInode.appendToReadDirOut(uint64(readDirIn.Size), readDirOut, curOffset, childInodeBasename, &curReadDirOutSize)
		if !ok {
			globals.Unlock()
			errno = 0
			return
		}
	}
}

func (*globalsStruct) DoReleaseDir(inHeader *fission.InHeader, releaseDirIn *fission.ReleaseDirIn) (errno syscall.Errno) {
	var (
		fh    *fhStruct
		inode *inodeStruct
		ok    bool
	)

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	fh, ok = inode.fhMap[releaseDirIn.FH]

	if !ok {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}

	if inode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}

	if (inode.inodeType != FUSERootDir) && fh.listDirectoryInProgress {
		globals.Unlock()
		errno = syscall.EACCES
		return
	}

	delete(inode.fhMap, fh.nonce)

	globals.Unlock()

	errno = 0
	return
}

func (*globalsStruct) DoFSyncDir(inHeader *fission.InHeader, fSyncDirIn *fission.FSyncDirIn) (errno syscall.Errno) {
	errno = 0
	return
}
func (*globalsStruct) DoGetLK(inHeader *fission.InHeader, getLKIn *fission.GetLKIn) (getLKOut *fission.GetLKOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoSetLK(inHeader *fission.InHeader, setLKIn *fission.SetLKIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoSetLKW(inHeader *fission.InHeader, setLKWIn *fission.SetLKWIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoAccess(inHeader *fission.InHeader, accessIn *fission.AccessIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoCreate(inHeader *fission.InHeader, createIn *fission.CreateIn) (createOut *fission.CreateOut, errno syscall.Errno) {
	fmt.Printf("[TODO] fission.go::DoCreate() inHeader: %+v createIn: %+v\n", inHeader, createIn)
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoInterrupt(inHeader *fission.InHeader, interruptIn *fission.InterruptIn) {}
func (*globalsStruct) DoBMap(inHeader *fission.InHeader, bMapIn *fission.BMapIn) (bMapOut *fission.BMapOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoDestroy(inHeader *fission.InHeader) (errno syscall.Errno) { return }
func (*globalsStruct) DoPoll(inHeader *fission.InHeader, pollIn *fission.PollIn) (pollOut *fission.PollOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}
func (*globalsStruct) DoBatchForget(inHeader *fission.InHeader, batchForgetIn *fission.BatchForgetIn) {
}
func (*globalsStruct) DoFAllocate(inHeader *fission.InHeader, fAllocateIn *fission.FAllocateIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (inode *inodeStruct) appendToReadDirPlusOut(readDirPlusInSize uint64, readDirPlusOut *fission.ReadDirPlusOut, entryAttrValidSec uint64, entryAttrValidNSec uint32, dirEntPlusOff uint64, basename string, curReadDirOutSize *uint64) (ok bool) {
	var (
		dirEntPlus     fission.DirEntPlus
		dirEntPlusSize uint64
		gid            uint64
		mTimeNSec      uint32
		mTimeSec       uint64
		uid            uint64
	)

	dirEntPlusSize = fission.DirEntPlusFixedPortionSize + uint64(len(basename)) + fission.DirEntAlignment - 1
	dirEntPlusSize /= fission.DirEntAlignment
	dirEntPlusSize *= fission.DirEntAlignment

	if (*curReadDirOutSize + dirEntPlusSize) > readDirPlusInSize {
		ok = false
		return
	}

	*curReadDirOutSize += dirEntPlusSize
	ok = true

	mTimeSec, mTimeNSec = timeTimeToAttrTime(inode.mTime)

	if inode.inodeType == FUSERootDir {
		uid = globals.config.uid
		gid = globals.config.gid
	} else {
		uid = inode.backend.uid
		gid = inode.backend.gid
	}

	dirEntPlus = fission.DirEntPlus{
		EntryOut: fission.EntryOut{
			NodeID:         inode.inodeNumber,
			Generation:     0,
			EntryValidSec:  entryAttrValidSec,
			EntryValidNSec: entryAttrValidNSec,
			AttrValidSec:   entryAttrValidSec,
			AttrValidNSec:  entryAttrValidNSec,
			Attr: fission.Attr{
				Ino:       inode.inodeNumber,
				Size:      inode.sizeInMemory,
				ATimeSec:  mTimeSec,
				MTimeSec:  mTimeSec,
				CTimeSec:  mTimeSec,
				ATimeNSec: mTimeNSec,
				MTimeNSec: mTimeNSec,
				CTimeNSec: mTimeNSec,
				Mode:      inode.mode,
				UID:       uint32(uid),
				GID:       uint32(gid),
				RDev:      0,
				Padding:   0,
			},
		},
		DirEnt: fission.DirEnt{
			Ino:     inode.inodeNumber,
			Off:     dirEntPlusOff,
			NameLen: uint32(len(basename)),
			Type:    inode.dirEntType(),
			Name:    []byte(basename),
		},
	}
	fixAttrSizes(&dirEntPlus.Attr)

	readDirPlusOut.DirEntPlus = append(readDirPlusOut.DirEntPlus, dirEntPlus)

	return
}

func (*globalsStruct) DoReadDirPlus(inHeader *fission.InHeader, readDirPlusIn *fission.ReadDirPlusIn) (readDirPlusOut *fission.ReadDirPlusOut, errno syscall.Errno) {
	var (
		childInode                                  *inodeStruct
		childInodeBasename                          string
		childInodeNumber                            uint64
		curOffset                                   uint64
		curOffsetInNextListDirectoryOutputCap       uint64
		curOffsetInListDirectorySubdirectoryListCap uint64
		curOffsetInPrevListDirectoryOutputCap       uint64
		curOffsetInVirtChildDirMapCap               uint64
		curOffsetInVirtChildFileMapCap              uint64
		curReadDirPlusOutSize                       uint64
		dirEntPlusCountMax                          uint64
		dirEntPlusMinSize                           uint64
		entryAttrValidNSec                          uint32
		entryAttrValidSec                           uint64
		err                                         error
		fh                                          *fhStruct
		listDirectoryOutputFile                     *listDirectoryOutputFileStruct
		listDirectoryInput                          *listDirectoryInputStruct
		listDirectoryOutput                         *listDirectoryOutputStruct
		ok                                          bool
		parentInode                                 *inodeStruct
		subdirectory                                string
		virtChildDirMapIndex                        int
		virtChildDirMapLen                          uint64
		virtChildFileMapIndex                       int
	)

	dirEntPlusMinSize = fission.DirEntFixedPortionSize + 1 + fission.DirEntAlignment - 1
	dirEntPlusMinSize /= fission.DirEntAlignment
	dirEntPlusMinSize *= fission.DirEntAlignment
	dirEntPlusCountMax = uint64(readDirPlusIn.Size) / dirEntPlusMinSize

	readDirPlusOut = &fission.ReadDirPlusOut{
		DirEntPlus: make([]fission.DirEntPlus, 0, dirEntPlusCountMax),
	}

	curReadDirPlusOutSize = 0
	curOffset = readDirPlusIn.Offset

	entryAttrValidSec, entryAttrValidNSec = timeDurationToAttrDuration(globals.config.entryAttrTTL)

	globals.Lock()

Restart:

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if parentInode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	fh, ok = parentInode.fhMap[readDirPlusIn.FH]
	if !ok {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}

	if parentInode.inodeType == FUSERootDir {
		virtChildDirMapLen = uint64(parentInode.virtChildDirMap.Len()) // Will be == 2 + len(globals.config.backends)

		for {
			if curOffset >= virtChildDirMapLen {
				globals.Unlock()
				errno = 0
				return
			}

			virtChildDirMapIndex = int(curOffset)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex)
			if !ok {
				globals.logger.Fatalf("parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex < virtChildDirMapLen) returned !ok")
			}

			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok")
			}

			curOffset++

			ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
			if !ok {
				globals.Unlock()
				errno = 0
				return
			}
		}
	}

	// If we reach here, we know parentInode.inodeType == BackendRootDir | PseudoDir

	if fh.listDirectoryInProgress {
		globals.Unlock()
		errno = syscall.EACCES
		return
	}

	if curOffset < fh.prevListDirectoryOutputStartingOffset {
		// Adjust curOffset to not try to reference before the start of fh.prevListDirectoryOutput

		curOffset = fh.prevListDirectoryOutputStartingOffset
	}

	for {
		if !fh.listDirectorySequenceDone && (curOffset >= (fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen)) {
			// Fetch the next listDirectoryOutput

			if fh.nextListDirectoryOutput != nil {
				fh.prevListDirectoryOutput = fh.nextListDirectoryOutput
				fh.prevListDirectoryOutputFileLen = fh.nextListDirectoryOutputFileLen
				fh.prevListDirectoryOutputStartingOffset = fh.nextListDirectoryOutputStartingOffset

				fh.nextListDirectoryOutput = nil
				fh.nextListDirectoryOutputFileLen = 0
			}

			if fh.prevListDirectoryOutput == nil {
				listDirectoryInput = &listDirectoryInputStruct{
					continuationToken: "",
					maxItems:          parentInode.backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			} else {
				listDirectoryInput = &listDirectoryInputStruct{
					continuationToken: fh.prevListDirectoryOutput.nextContinuationToken,
					maxItems:          parentInode.backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			}

			fh.listDirectoryInProgress = true

			globals.Unlock()

			listDirectoryOutput, err = parentInode.backend.context.listDirectory(listDirectoryInput)

			globals.Lock()

			fh.listDirectoryInProgress = false

			if err != nil {
				globals.Unlock()
				globals.logger.Printf("unable to access backend \"%s\"", parentInode.backend.dirName)
				errno = syscall.EACCES
				return
			}

			fh.listDirectorySequenceDone = !listDirectoryOutput.isTruncated

			if fh.prevListDirectoryOutput == nil {
				fh.prevListDirectoryOutput = listDirectoryOutput
				fh.prevListDirectoryOutputFileLen = uint64(len(listDirectoryOutput.file))
				fh.prevListDirectoryOutputStartingOffset = 0

				fh.nextListDirectoryOutput = nil
				fh.nextListDirectoryOutputFileLen = 0
				fh.nextListDirectoryOutputStartingOffset = fh.prevListDirectoryOutputFileLen
			} else {
				fh.nextListDirectoryOutput = listDirectoryOutput
				fh.nextListDirectoryOutputFileLen = uint64(len(listDirectoryOutput.file))
				fh.nextListDirectoryOutputStartingOffset = fh.prevListDirectoryOutputStartingOffset + fh.prevListDirectoryOutputFileLen
			}

			// Ensure we remember all discovered subdirectories and no longer refer to now physical subdirectories as virtual

			for _, subdirectory = range listDirectoryOutput.subdirectory {
				_ = parentInode.findChildDirInode(subdirectory)
				_, ok = fh.listDirectorySubdirectorySet[subdirectory]
				if !ok {
					fh.listDirectorySubdirectorySet[subdirectory] = struct{}{}
					fh.listDirectorySubdirectoryList = append(fh.listDirectorySubdirectoryList, subdirectory)
				}
			}

			// Since we had to release globals.Lock during listDirectory() call, we must restart from where we first grabbed it

			goto Restart
		}

		// At this point, we know either we are still reading fh.{prev|next}ListDirectoryOutput's
		// or we are done with all of them and may proceed to return fh.listDirectorySubdirectoryList
		// & parentInode.virtChild{Dir|File}Map entries

		curOffsetInPrevListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset
		curOffsetInNextListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen
		curOffsetInListDirectorySubdirectoryListCap = curOffsetInNextListDirectoryOutputCap + uint64(len(fh.listDirectorySubdirectoryList))
		curOffsetInVirtChildDirMapCap = curOffsetInListDirectorySubdirectoryListCap + uint64(parentInode.virtChildDirMap.Len())
		curOffsetInVirtChildFileMapCap = curOffsetInVirtChildDirMapCap + uint64(parentInode.virtChildFileMap.Len())

		switch {
		case curOffset < curOffsetInPrevListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.prevListDirectoryOutput.file[curOffset-fh.prevListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInNextListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.nextListDirectoryOutput.file[curOffset-fh.nextListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInListDirectorySubdirectoryListCap:
			childInode = parentInode.findChildDirInode(fh.listDirectorySubdirectoryList[curOffset-curOffsetInNextListDirectoryOutputCap])
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInVirtChildDirMapCap:
			virtChildDirMapIndex = int(curOffset - curOffsetInListDirectorySubdirectoryListCap)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex)
			if !ok {
				globals.logger.Fatalf("parentInode.virtChildDirMap.GetByIndex(virtChildDirMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok")
			}
		case curOffset < curOffsetInVirtChildFileMapCap:
			virtChildFileMapIndex = int(curOffset - curOffsetInVirtChildDirMapCap)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildFileMap.GetByIndex(virtChildFileMapIndex)
			if !ok {
				globals.logger.Fatalf("parentInode.virtChildFileMap.GetByIndex(virtChildFileMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok")
			}
		default:
			globals.Unlock()
			errno = 0
			return
		}

		curOffset++

		ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
		if !ok {
			globals.Unlock()
			errno = 0
			return
		}
	}
}

func (*globalsStruct) DoRename2(inHeader *fission.InHeader, rename2In *fission.Rename2In) (errno syscall.Errno) {
	errno = syscall.EXDEV
	return
}

func (*globalsStruct) DoLSeek(inHeader *fission.InHeader, lSeekIn *fission.LSeekIn) (lSeekOut *fission.LSeekOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

func (*globalsStruct) DoStatX(inHeader *fission.InHeader, statXIn *fission.StatXIn) (statXOut *fission.StatXOut, errno syscall.Errno) {
	var (
		attrValidNSec uint32
		attrValidSec  uint64
		gid           uint32
		mTimeNSec     uint32
		mTimeSec      uint64
		ok            bool
		thisInode     *inodeStruct
		uid           uint32
	)

	globals.Lock()

	thisInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	switch thisInode.inodeType {
	case FileObject:
		uid = uint32(thisInode.backend.uid)
		gid = uint32(thisInode.backend.gid)
	case FUSERootDir:
		uid = uint32(globals.config.uid)
		gid = uint32(globals.config.gid)
	case BackendRootDir:
		uid = uint32(thisInode.backend.uid)
		gid = uint32(thisInode.backend.gid)
	case PseudoDir:
		uid = uint32(thisInode.backend.uid)
		gid = uint32(thisInode.backend.gid)

	default:
		globals.logger.Fatalf("unrecognized inodeType (%v)", thisInode.inodeType)
	}

	attrValidSec, attrValidNSec = timeDurationToAttrDuration(globals.config.entryAttrTTL)
	mTimeSec, mTimeNSec = timeTimeToAttrTime(thisInode.mTime)

	statXOut = &fission.StatXOut{
		AttrValidSec:  attrValidSec,
		AttrValidNSec: attrValidNSec,
		Flags:         0,
		Spare:         [2]uint64{0, 0},
		StatX: fission.StatX{
			Mask:           (fission.StatXMaskBasicStats | fission.StatXMaskBTime),
			Attributes:     0,
			UID:            uid,
			GID:            gid,
			Mode:           uint16(thisInode.mode),
			Spare0:         [1]uint16{0},
			Ino:            thisInode.inodeNumber,
			Size:           thisInode.sizeInMemory,
			AttributesMask: 0,
			ATime: fission.SXTime{
				TVSec:    mTimeSec,
				TVNSec:   mTimeNSec,
				Reserved: 0,
			},
			BTime: fission.SXTime{
				TVSec:    mTimeSec,
				TVNSec:   mTimeNSec,
				Reserved: 0,
			},
			CTime: fission.SXTime{
				TVSec:    mTimeSec,
				TVNSec:   mTimeNSec,
				Reserved: 0,
			},
			MTime: fission.SXTime{
				TVSec:    mTimeSec,
				TVNSec:   mTimeNSec,
				Reserved: 0,
			},
			RDevMajor: 0,
			RDevMinor: 0,
			DevMajor:  0,
			DevMinor:  0,
			Spare2:    [14]uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	fixStatXSizes(&statXOut.StatX)

	globals.Unlock()

	errno = 0
	return
}
