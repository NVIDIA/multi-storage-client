package main

import (
	"fmt"
	"log"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/fission/v3"
)

const (
	fuseSubtype = "msfs"

	initOutFlags = uint32(0) |
		fission.InitFlagsAsyncRead |
		fission.InitFlagsFileOps |
		fission.InitFlagsAtomicOTrunc |
		fission.InitFlagsExportSupport |
		fission.InitFlagsBigWrites |
		fission.InitFlagsAutoInvalData |
		fission.InitFlagsDoReadDirPlus |
		fission.InitFlagsParallelDirops

	initOutFlags2 = uint32(0) |
		fission.InitFlags2DirectIoAllowMmap

	initOutMaxBackgound         = uint16(100)
	initOutCongestionThreshhold = uint16(0)

	maxPages = 256                     // * 4KiB page size == 1MiB... the max read or write size in Linux FUSE at this time
	maxRead  = uint32(maxPages * 4096) //                     1MiB... the max read          size in Linux FUSE at this time
	maxWrite = uint32(maxPages * 4096) //                     1MiB... the max         write size in Linux FUSE at this time

	attrBlkSize   = uint32(512)
	statFSBlkSize = uint64(1024)

	maxNameLen = uint32(4096)

	openOutFlags = uint32(0) |
		fission.FOpenResponseDirectIO
)

// `performFissionMount` is called to do the single FUSE mount at startup.
func performFissionMount() (err error) {
	var (
		fissionLogger = log.New(globals.logger.Writer(), "[FISSION] ", globals.logger.Flags()) // set prefix to differentiate package fission logging
	)

	globals.fissionVolume = fission.NewVolume(globals.config.mountName, globals.config.mountPoint, fuseSubtype, maxRead, maxWrite, true, globals.config.allowOther, &globals, fissionLogger, globals.errChan)

	err = globals.fissionVolume.DoMount()

	return
}

// `performFissionUnmount` is called to do the single FUSE unmount at shutdown.
func performFissionUnmount() (err error) {
	err = globals.fissionVolume.DoUnmount()

	return
}

// `fixAttrSizes` is called to leverage the .Size field of a fission.Attr
// struct to compute and fill in the related .Blocks field. The .BlkSize
// and .NLink fields are also set to their hard-coded values noting that
// the .NLink field value is wrong for directories that have subdirectories
// as is will not account for the ".." directory entries in each of those
// subdirectories.
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

// `fixAttrSizes` is called to leverage the .Size field of a fission.StatX
// struct to compute and fill in the related .Blocks field. The .BlkSize
// and .NLink fields are also set to their hard-coded values noting that
// the .NLink field value is wrong for directories that have subdirectories
// as is will not account for the ".." directory entries in each of those
// subdirectories.
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

// `dirEntType` computes the directory entry type returned by DoReadDir{|Plus}()
// for each directory entry.
func (inode *inodeStruct) dirEntType() (dirEntType uint32) {
	if inode.inodeType == FileObject {
		dirEntType = syscall.DT_REG
	} else {
		dirEntType = syscall.DT_DIR
	}

	return
}

// `DoLookup` implements the package fission callback to fetch metadata
// information about a directory entry (if present).
func (*globalsStruct) DoLookup(inHeader *fission.InHeader, lookupIn *fission.LookupIn) (lookupOut *fission.LookupOut, errno syscall.Errno) {
	var (
		backend            *backendStruct
		childInode         *inodeStruct
		childInodeNumber   uint64
		entryAttrValidNSec uint32
		entryAttrValidSec  uint64
		latency            float64
		mTimeNSec          uint32
		mTimeSec           uint64
		ok                 bool
		parentInode        *inodeStruct
		startTime          = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.LookupSuccesses.Inc()
			globals.fissionMetrics.LookupSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.LookupSuccesses.Inc()
				backend.fissionMetrics.LookupSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.LookupFailures.Inc()
			globals.fissionMetrics.LookupFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.LookupFailures.Inc()
				backend.fissionMetrics.LookupFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		// We no longer know how to map inHeader.NodeID (an inodeNumber) to the parentInode
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if parentInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[parentInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce]")
		}
	}

	if parentInode.inodeType == FileObject {
		// The parentInode must be a directory of some sort... not a FileObject
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	if parentInode.inodeType == FUSERootDir {
		// If lookupIn.Name exists, it is in parentInode.childDirMap

		childInodeNumber, ok = parentInode.physChildInodeMap.GetByKey(string(lookupIn.Name))
		if !ok {
			childInodeNumber, ok = parentInode.virtChildInodeMap.GetByKey(string(lookupIn.Name))
			if !ok {
				globals.Unlock()
				errno = syscall.ENOENT
				return
			}
		}

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [DoLookup()]")
		}
	} else {
		// We only know parentInode is a BackendRootDir or a PseudoDir

		childInode, ok = parentInode.findChildInode(string(lookupIn.Name))
		if !ok || childInode.pendingDelete {
			globals.Unlock()
			errno = syscall.ENOENT
			return
		}
	}

	backend, ok = globals.backendMap[childInode.backendNonce]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.backendMap[childInode.backendNonce]")
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
				UID:       uint32(backend.uid),
				GID:       uint32(backend.gid),
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

// `DoForget` implements the package fission callback to note that
// the kernel has removed an inode from its internal caches.
func (*globalsStruct) DoForget(inHeader *fission.InHeader, forgetIn *fission.ForgetIn) {}

// `DoGetAttr` implements the package fission callback to fetch metadata
// information about an inode.
func (*globalsStruct) DoGetAttr(inHeader *fission.InHeader, getAttrIn *fission.GetAttrIn) (getAttrOut *fission.GetAttrOut, errno syscall.Errno) {
	var (
		attrValidNSec uint32
		attrValidSec  uint64
		backend       *backendStruct
		gid           uint32
		latency       float64
		mTimeNSec     uint32
		mTimeSec      uint64
		ok            bool
		thisInode     *inodeStruct
		startTime     = time.Now()
		uid           uint32
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.GetAttrSuccesses.Inc()
			globals.fissionMetrics.GetAttrSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.GetAttrSuccesses.Inc()
				backend.fissionMetrics.GetAttrSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.GetAttrFailures.Inc()
			globals.fissionMetrics.GetAttrFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.GetAttrFailures.Inc()
				backend.fissionMetrics.GetAttrFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	thisInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if thisInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[thisInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[thisInode.backendNonce]")
		}
	}

	if thisInode.pendingDelete {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	thisInode.touch(nil)

	switch thisInode.inodeType {
	case FileObject:
		uid = uint32(backend.uid)
		gid = uint32(backend.gid)
	case FUSERootDir:
		uid = uint32(globals.config.uid)
		gid = uint32(globals.config.gid)
	case BackendRootDir:
		uid = uint32(backend.uid)
		gid = uint32(backend.gid)
	case PseudoDir:
		uid = uint32(backend.uid)
		gid = uint32(backend.gid)
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] unrecognized inodeType (%v)", thisInode.inodeType)
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

// `DoSetAttr` implements the package fission callback to set attributes of an inode.
func (*globalsStruct) DoSetAttr(inHeader *fission.InHeader, setAttrIn *fission.SetAttrIn) (setAttrOut *fission.SetAttrOut, errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoSetAttr()")
	errno = syscall.ENOSYS
	return
}

// `DoReadLink` implements the package fission callback to read the target
// of a symlink inode (not supported)
func (*globalsStruct) DoReadLink(inHeader *fission.InHeader) (readLinkOut *fission.ReadLinkOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoSymLink` implements the package fission callback to create a symlink inode (not supported)
func (*globalsStruct) DoSymLink(inHeader *fission.InHeader, symLinkIn *fission.SymLinkIn) (symLinkOut *fission.SymLinkOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoMkNod` implements the package fission callback to create a file inode.
func (*globalsStruct) DoMkNod(inHeader *fission.InHeader, mkNodIn *fission.MkNodIn) (mkNodOut *fission.MkNodOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoMkDir` implements the package fission callback to create a directory inode.
func (*globalsStruct) DoMkDir(inHeader *fission.InHeader, mkDirIn *fission.MkDirIn) (mkDirOut *fission.MkDirOut, errno syscall.Errno) {
	var (
		backend            *backendStruct
		basename           = string(mkDirIn.Name)
		childInode         *inodeStruct
		entryAttrValidNSec uint32
		entryAttrValidSec  uint64
		latency            float64
		mTimeNSec          uint32
		mTimeSec           uint64
		ok                 bool
		parentInode        *inodeStruct
		startTime          = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.MkDirSuccesses.Inc()
			globals.fissionMetrics.MkDirSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.MkDirSuccesses.Inc()
				backend.fissionMetrics.MkDirSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.MkDirFailures.Inc()
			globals.fissionMetrics.MkDirFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.MkDirFailures.Inc()
				backend.fissionMetrics.MkDirFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		// We no longer know how to map inHeader.NodeID (an inodeNumber) to the parentInode
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if parentInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[parentInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce]")
		}
	}

	if parentInode.inodeType == FileObject {
		// The parentInode must be a directory of some sort... not a FileObject
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}
	if parentInode.inodeType == FUSERootDir {
		// Never allowed in FUSERootDir
		globals.Unlock()
		errno = syscall.EPERM
		return
	}
	if backend.readOnly {
		// Never allowed in a readOnly backend
		globals.Unlock()
		errno = syscall.EPERM
		return
	}

	_, ok = parentInode.findChildInode(basename)
	if ok {
		// We just return EEXIST if we find a phys or virt child dir entry (whether or not it is a dir or a file)
		globals.Unlock()
		errno = syscall.EEXIST
		return
	}

	// From here, we know we will succeed

	childInode = parentInode.createPseudoDirInode(true, basename)

	entryAttrValidSec, entryAttrValidNSec = timeDurationToAttrDuration(globals.config.entryAttrTTL)
	mTimeSec, mTimeNSec = timeTimeToAttrTime(childInode.mTime)

	mkDirOut = &fission.MkDirOut{
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
				UID:       uint32(backend.uid),
				GID:       uint32(backend.gid),
				RDev:      0,
				Padding:   0,
			},
		},
	}
	fixAttrSizes(&mkDirOut.Attr)

	globals.Unlock()

	errno = 0
	return
}

// `DoUnlink` implements the package fission callback to remove a directory entry of a
// file inode that, since hardlinks are not supported, also removes the file inode itself.
func (*globalsStruct) DoUnlink(inHeader *fission.InHeader, unlinkIn *fission.UnlinkIn) (errno syscall.Errno) {
	var (
		backend     *backendStruct
		basename    = string(unlinkIn.Name)
		childInode  *inodeStruct
		latency     float64
		ok          bool
		parentInode *inodeStruct
		startTime   = time.Now()
	)

	// Record metrics on function exit
	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.UnlinkSuccesses.Inc()
			globals.fissionMetrics.UnlinkSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.UnlinkSuccesses.Inc()
				backend.fissionMetrics.UnlinkSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.UnlinkFailures.Inc()
			globals.fissionMetrics.UnlinkFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.UnlinkFailures.Inc()
				backend.fissionMetrics.UnlinkFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	if parentInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[parentInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce]")
		}
	}

	parentInode.touch(nil)

	if parentInode.inodeType == FUSERootDir {
		globals.Unlock()
		errno = syscall.EPERM
		return
	}
	if parentInode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}
	if backend.readOnly {
		globals.Unlock()
		errno = syscall.EPERM
		return
	}

	childInode, ok = parentInode.findChildInode(basename)
	if !ok {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if childInode.pendingDelete {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if childInode.inodeType != FileObject {
		childInode.touch(nil)
		globals.Unlock()
		errno = syscall.EISDIR
		return
	}

	// One way or another, childInode will be deleted

	childInode.pendingDelete = true
	childInode.touch(nil)

	if len(childInode.fhSet) != 0 {
		// Since fhSet is not empty, cannot delete childInode

		globals.Unlock()

		errno = 0
		return
	}

	globals.Unlock()

	childInode.finishPendingDelete()

	errno = 0
	return
}

// `DoRmDir` implements the package fission callback to remove a directory inode.
func (*globalsStruct) DoRmDir(inHeader *fission.InHeader, rmDirIn *fission.RmDirIn) (errno syscall.Errno) {
	var (
		backend     *backendStruct
		basename    = string(rmDirIn.Name)
		childInode  *inodeStruct
		latency     float64
		ok          bool
		parentInode *inodeStruct
		startTime   = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.RmDirSuccesses.Inc()
			globals.fissionMetrics.RmDirSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.RmDirSuccesses.Inc()
				backend.fissionMetrics.RmDirSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.RmDirFailures.Inc()
			globals.fissionMetrics.RmDirFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.RmDirFailures.Inc()
				backend.fissionMetrics.RmDirFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		// We no longer know how to map inHeader.NodeID (an inodeNumber) to the parentInode
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if parentInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[parentInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce]")
		}
	}

	if parentInode.inodeType == FileObject {
		// The parentInode must be a directory of some sort... not a FileObject
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}
	if parentInode.inodeType == FUSERootDir {
		// Never allowed in FUSERootDir
		globals.Unlock()
		errno = syscall.EPERM
		return
	}
	if backend.readOnly {
		// Never allowed in a readOnly backend
		globals.Unlock()
		errno = syscall.EPERM
		return
	}

	childInode, ok = parentInode.findChildInode(basename)
	if !ok {
		// We didn't find the child directory, so just return ENOENT
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if childInode.inodeType != PseudoDir {
		// This child exists but is not a directory, so we return ENOTDIR
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}
	if len(childInode.fhSet) > 0 {
		// We return EBUSY if the directory is currently "open"
		globals.Unlock()
		errno = syscall.EBUSY
		return
	}
	if (childInode.physChildInodeMap.Len() > 0) || (childInode.virtChildInodeMap.Len() > 2) {
		// Return ENOTEMPTY if childInode has any children
		globals.Unlock()
		errno = syscall.ENOTEMPTY
		return
	}

	// From here, we know we will succeed

	if childInode.listElement != nil {
		globals.inodeEvictionLRU.Remove(childInode.xTime, childInode.listElement)
		childInode.xTime = time.Time{}
		childInode.listElement = nil
	}

	_ = childInode.virtChildInodeMap.DeleteByKey(DotDirEntryBasename)
	_ = childInode.virtChildInodeMap.DeleteByKey(DotDotDirEntryBasename)

	if childInode.isVirt {
		ok = parentInode.virtChildInodeMap.DeleteByKey(basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(basename) returned !ok")
		}
	} else {
		ok = parentInode.physChildInodeMap.DeleteByKey(basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.DeleteByKey(basename) returned !ok")
		}
	}

	delete(globals.inodeMap, childInode.inodeNumber)

	parentInode.touch(nil)

	globals.Unlock()

	errno = 0
	return
}

// `DoRename` implements the package fission callback to rename a directory entry (not supported).
func (*globalsStruct) DoRename(inHeader *fission.InHeader, renameIn *fission.RenameIn) (errno syscall.Errno) {
	errno = syscall.EXDEV
	return
}

// `DoLink` implements the package fission callback to create a hardlink to an existing file inode (not supported).
func (*globalsStruct) DoLink(inHeader *fission.InHeader, linkIn *fission.LinkIn) (linkOut *fission.LinkOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoOpen` implements the package fission callback to open an existing file inode.
func (*globalsStruct) DoOpen(inHeader *fission.InHeader, openIn *fission.OpenIn) (openOut *fission.OpenOut, errno syscall.Errno) {
	var (
		allowReads   bool
		allowWrites  bool
		appendWrites bool
		backend      *backendStruct
		fh           *fhStruct
		fhNonce      uint64
		inode        *inodeStruct
		isExclusive  bool
		latency      float64
		ok           bool
		startTime    = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.OpenSuccesses.Inc()
			globals.fissionMetrics.OpenSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.OpenSuccesses.Inc()
				backend.fissionMetrics.OpenSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.OpenFailures.Inc()
			globals.fissionMetrics.OpenFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.OpenFailures.Inc()
				backend.fissionMetrics.OpenFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if inode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[inode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[inode.backendNonce]")
		}
	}

	if inode.pendingDelete {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}
	if inode.inodeType != FileObject {
		globals.Unlock()
		errno = syscall.EISDIR
		return
	}

	if len(inode.fhSet) == 1 {
		for fhNonce = range inode.fhSet {
			// Note that, due to the above if, this "loop" will execute exactly once

			fh, ok = globals.fhMap[fhNonce]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.fhMap[fhNonce] returned !ok")
			}

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

	if allowWrites && backend.readOnly {
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

	inode.fhSet[fh.nonce] = struct{}{}
	globals.fhMap[fh.nonce] = fh

	inode.touch(nil)

	openOut = &fission.OpenOut{
		FH:        fh.nonce,
		OpenFlags: openOutFlags,
		Padding:   0,
	}

	globals.Unlock()

	errno = 0
	return
}

// `DoRead` implements the package fission callback to read a portion of a file inode's contents.
func (*globalsStruct) DoRead(inHeader *fission.InHeader, readIn *fission.ReadIn) (readOut *fission.ReadOut, errno syscall.Errno) {
	var (
		backend                         *backendStruct
		cacheLine                       *cacheLineStruct
		cacheLineHits                   uint64 // As this is the fall-thru condition, includes +cacheMisses+cacheWaits
		cacheLineNonce                  uint64
		cacheLineNumber                 uint64
		cacheLineNumberMaxInBackend     uint64
		cacheLineMisses                 uint64
		cacheLineOffsetLimit            uint64 // One greater than offset to last byte to return
		cacheLineOffsetStart            uint64
		cacheLineWaiter                 sync.WaitGroup
		cacheLineWaits                  uint64
		cacheLinesToPotentiallyPrefetch uint64
		curOffset                       = readIn.Offset
		fh                              *fhStruct
		inode                           *inodeStruct
		latency                         float64
		ok                              bool
		prefetchCacheLinesIssued        uint64
		prefetchCacheLineNumber         uint64
		prefetchCacheLineNumberMax      uint64
		prefetchCacheLineNumberMin      uint64
		startTime                       = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.ReadSuccesses.Inc()
			globals.fissionMetrics.ReadSuccessLatencies.Observe(latency)
			globals.fissionMetrics.ReadSuccessSizes.Observe(float64(len(readOut.Data)))
			if backend != nil {
				backend.fissionMetrics.ReadSuccesses.Inc()
				backend.fissionMetrics.ReadSuccessLatencies.Observe(latency)
				backend.fissionMetrics.ReadSuccessSizes.Observe(float64(len(readOut.Data)))
			}
		} else {
			globals.fissionMetrics.ReadFailures.Inc()
			globals.fissionMetrics.ReadFailureLatencies.Observe(latency)
			globals.fissionMetrics.ReadFailureSizes.Observe(float64(readIn.Size))
			if backend != nil {
				backend.fissionMetrics.ReadFailures.Inc()
				backend.fissionMetrics.ReadFailureLatencies.Observe(latency)
				backend.fissionMetrics.ReadFailureSizes.Observe(float64(readIn.Size))
			}
		}
		globals.fissionMetrics.ReadCacheHits.Add(float64(cacheLineHits - cacheLineMisses - cacheLineWaits))
		globals.fissionMetrics.ReadCacheMisses.Add(float64(cacheLineMisses))
		globals.fissionMetrics.ReadCacheWaits.Add(float64(cacheLineWaits))
		globals.fissionMetrics.ReadCachePrefetches.Add(float64(prefetchCacheLinesIssued))
		if backend != nil {
			backend.fissionMetrics.ReadCacheHits.Add(float64(cacheLineHits - cacheLineMisses - cacheLineWaits))
			backend.fissionMetrics.ReadCacheMisses.Add(float64(cacheLineMisses))
			backend.fissionMetrics.ReadCacheWaits.Add(float64(cacheLineWaits))
			backend.fissionMetrics.ReadCachePrefetches.Add(float64(prefetchCacheLinesIssued))
		}
		globals.Unlock()
	}()

	readOut = &fission.ReadOut{
		Data: make([]byte, 0, readIn.Size),
	}

	for len(readOut.Data) < cap(readOut.Data) {
		globals.Lock()

		inode, ok = globals.inodeMap[inHeader.NodeID]
		if !ok {
			backend = nil
			globals.Unlock()
			errno = syscall.ENOENT
			return
		}

		if inode.backendNonce == 0 {
			backend = nil
		} else {
			backend, ok = globals.backendMap[inode.backendNonce]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.backendMap[inode.backendNonce]")
			}
		}

		if inode.inodeType != FileObject {
			globals.Unlock()
			errno = syscall.EBADF
			return
		}

		_, ok = inode.fhSet[readIn.FH]
		if !ok {
			globals.Unlock()
			errno = syscall.EBADF
			return
		}
		fh, ok = globals.fhMap[readIn.FH]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.fhMap[readIn.FH] returned !ok")
		}
		if !fh.allowReads {
			globals.Unlock()
			errno = syscall.EBADF
			return
		}

		inode.touch(nil)

		if curOffset >= inode.sizeInBackend {
			// We have reached EOF

			globals.Unlock()

			break
		}

		cacheLineNumber = curOffset / globals.config.cacheLineSize

		cacheLineNonce, ok = inode.cacheMap[cacheLineNumber]
		if !ok {
			cacheLineMisses++

			cacheLine = &cacheLineStruct{
				nonce:       fetchNonce(),
				state:       CacheLineInbound,
				waiters:     make([]*sync.WaitGroup, 1),
				inodeNumber: inode.inodeNumber,
				lineNumber:  cacheLineNumber,
			}

			cacheLineWaiter.Add(1)
			cacheLine.waiters[0] = &cacheLineWaiter

			inode.cacheMap[cacheLineNumber] = cacheLine.nonce
			globals.cacheMap[cacheLine.nonce] = cacheLine

			inode.inboundCacheLineCount++
			cacheLine.listElement = globals.inboundCacheLineList.PushBack(cacheLine)

			go cacheLine.fetch()

			if globals.config.cacheLinesToPrefetch > 0 {
				cacheLineNumberMaxInBackend = ((inode.sizeInBackend + globals.config.cacheLineSize - 1) / globals.config.cacheLineSize) - 1

				if cacheLineNumberMaxInBackend >= (cacheLineNumber + globals.config.cacheLinesToPrefetch) {
					cacheLinesToPotentiallyPrefetch = globals.config.cacheLinesToPrefetch
				} else {
					cacheLinesToPotentiallyPrefetch = cacheLineNumberMaxInBackend - cacheLineNumber
				}

				if cacheLinesToPotentiallyPrefetch > 0 {
					prefetchCacheLineNumberMin = cacheLineNumber + 1
					prefetchCacheLineNumberMax = prefetchCacheLineNumberMin + cacheLinesToPotentiallyPrefetch - 1

					for prefetchCacheLineNumber = prefetchCacheLineNumberMin; prefetchCacheLineNumber <= prefetchCacheLineNumberMax; prefetchCacheLineNumber++ {
						_, ok = inode.cacheMap[prefetchCacheLineNumber]
						if !ok {
							cacheLine = &cacheLineStruct{
								nonce:       fetchNonce(),
								state:       CacheLineInbound,
								waiters:     make([]*sync.WaitGroup, 0, 1),
								inodeNumber: inode.inodeNumber,
								lineNumber:  prefetchCacheLineNumber,
							}

							inode.cacheMap[prefetchCacheLineNumber] = cacheLine.nonce
							globals.cacheMap[cacheLine.nonce] = cacheLine

							inode.inboundCacheLineCount++
							cacheLine.listElement = globals.inboundCacheLineList.PushBack(cacheLine)

							go cacheLine.fetch()

							prefetchCacheLinesIssued++
						}
					}
				}
			}

			globals.Unlock()

			cacheLineWaiter.Wait()

			continue
		}

		cacheLine, ok = globals.cacheMap[cacheLineNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.cacheMap[cacheLineNonce] returned !ok")
		}

		if cacheLine.state == CacheLineInbound {
			cacheLineWaits++

			cacheLineWaiter.Add(1)
			cacheLine.waiters = append(cacheLine.waiters, &cacheLineWaiter)

			globals.Unlock()

			cacheLineWaiter.Wait()

			continue
		}

		cacheLineHits++ // Note that this is the fall-thru condition that counts resolved (cacheLine)Misses & (cacheLine)Waits as (subsequent) Hits

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

// `DoWrite` implements the package fission callback to add or replace a portion of a file inode's contents.
func (*globalsStruct) DoWrite(inHeader *fission.InHeader, writeIn *fission.WriteIn) (writeOut *fission.WriteOut, errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoWrite()")
	errno = syscall.ENOSYS
	return
}

// `DoStatFS` implements the package fission callback to fetch statistics about this FUSE file system.
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

	globals.fissionMetrics.StatFSCalls.Inc()

	globals.Unlock()

	errno = 0
	return
}

// `DoRelease` implements the package fission callback to close a file inode's file handle.
func (*globalsStruct) DoRelease(inHeader *fission.InHeader, releaseIn *fission.ReleaseIn) (errno syscall.Errno) {
	var (
		backend   *backendStruct
		fh        *fhStruct
		inode     *inodeStruct
		latency   float64
		ok        bool
		startTime = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.ReleaseSuccesses.Inc()
			globals.fissionMetrics.ReleaseSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.ReleaseSuccesses.Inc()
				backend.fissionMetrics.ReleaseSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.ReleaseFailures.Inc()
			globals.fissionMetrics.ReleaseFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.ReleaseFailures.Inc()
				backend.fissionMetrics.ReleaseFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if inode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[inode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[inode.backendNonce]")
		}
	}

	if inode.inodeType != FileObject {
		inode.touch(nil)
		globals.Unlock()
		errno = syscall.EBADF
		return
	}

	_, ok = inode.fhSet[releaseIn.FH]
	if !ok {
		inode.touch(nil)
		globals.Unlock()
		errno = syscall.EBADF
		return
	}
	fh, ok = globals.fhMap[releaseIn.FH]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.fhMap[releaseIn.FH] returned !ok")
	}

	delete(inode.fhSet, fh.nonce)
	delete(globals.fhMap, fh.nonce)

	inode.touch(nil)

	if !inode.pendingDelete || (len(inode.fhSet) != 0) {
		globals.Unlock()

		errno = 0
		return
	}

	globals.Unlock()

	inode.finishPendingDelete()

	errno = 0
	return
}

// `DoFSync` implements the package fission callback to ensure modified metadata and/or
// content for a file inode is flushed to the underlying object.
func (*globalsStruct) DoFSync(inHeader *fission.InHeader, fSyncIn *fission.FSyncIn) (errno syscall.Errno) {
	fmt.Println("[TODO] fission.go::DoFSync()")
	errno = syscall.ENOSYS
	return
}

// `DoSetXAttr` implements the package fission callback to set or update an extended attribute
// for an inode (not supported).
func (*globalsStruct) DoSetXAttr(inHeader *fission.InHeader, setXAttrIn *fission.SetXAttrIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoGetXAttr` implements the package fission callback to fetch an extended attribute
// for an inode (not supported).
func (*globalsStruct) DoGetXAttr(inHeader *fission.InHeader, getXAttrIn *fission.GetXAttrIn) (getXAttrOut *fission.GetXAttrOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoListXAttr` implements the package fission callback to list the extended attributes
// for an inode (not supported).
func (*globalsStruct) DoListXAttr(inHeader *fission.InHeader, listXAttrIn *fission.ListXAttrIn) (listXAttrOut *fission.ListXAttrOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoRemoveXAttr` implements the package fission callback to remove an extended attribute
// for an inode (not supported).
func (*globalsStruct) DoRemoveXAttr(inHeader *fission.InHeader, removeXAttrIn *fission.RemoveXAttrIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoFlush` implements the package fission callback to ensure both modified metadata and
// content for a file inode is flushed to the underlying object.
func (*globalsStruct) DoFlush(inHeader *fission.InHeader, flushIn *fission.FlushIn) (errno syscall.Errno) {
	// fmt.Println("[TODO] fission.go::DoFlush()")
	errno = syscall.ENOSYS
	return
}

// `DoInit` implements the package fission callback to initialize this FUSE file system.
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
		Flags2:               initOutFlags2,
		MaxStackDepth:        0,
		RequestTimeout:       0,
		Unused:               [11]uint16{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}

	errno = 0
	return
}

// `DoOpenDir` implements the package fission callback to open a directory inode.
func (*globalsStruct) DoOpenDir(inHeader *fission.InHeader, openDirIn *fission.OpenDirIn) (openDirOut *fission.OpenDirOut, errno syscall.Errno) {
	var (
		backend   *backendStruct
		fh        *fhStruct
		inode     *inodeStruct
		latency   float64
		ok        bool
		startTime = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.OpenDirSuccesses.Inc()
			globals.fissionMetrics.OpenDirSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.OpenDirSuccesses.Inc()
				backend.fissionMetrics.OpenDirSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.OpenDirFailures.Inc()
			globals.fissionMetrics.OpenDirFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.OpenDirFailures.Inc()
				backend.fissionMetrics.OpenDirFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if inode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[inode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[inode.backendNonce]")
		}
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

	inode.fhSet[fh.nonce] = struct{}{}
	globals.fhMap[fh.nonce] = fh

	inode.touch(nil)

	openDirOut = &fission.OpenDirOut{
		FH:        fh.nonce,
		OpenFlags: 0,
		Padding:   0,
	}

	globals.Unlock()

	errno = 0
	return
}

// `appendToReadDirOut` appends the information about an inode in the form of a fission.DirEnt
// to the accumulating fission.ReadDirOut struct if there is room.
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

	readDirOut.DirEnt = append(readDirOut.DirEnt, fission.DirEnt{
		Ino:     inode.inodeNumber,
		Off:     dirEntOff,
		NameLen: uint32(len(basename)),
		Type:    inode.dirEntType(),
		Name:    []byte(basename),
	})

	ok = true
	return
}

// `DoReadDir` implements the package fission callback to enumerate a directory inode's entries (non-verbosely).
func (*globalsStruct) DoReadDir(inHeader *fission.InHeader, readDirIn *fission.ReadDirIn) (readDirOut *fission.ReadDirOut, errno syscall.Errno) {
	var (
		backend                                     *backendStruct
		childDirMapIndex                            int
		childDirMapLen                              uint64
		childInode                                  *inodeStruct
		childInodeBasename                          string
		childInodeNumber                            uint64
		curOffset                                   uint64
		curOffsetInListDirectorySubdirectoryListCap uint64
		curOffsetInNextListDirectoryOutputCap       uint64
		curOffsetInPrevListDirectoryOutputCap       uint64
		curOffsetInVirtChildInodeMapCap             uint64
		curReadDirOutSize                           uint64
		dirEntCountMax                              uint64
		dirEntMinSize                               uint64
		err                                         error
		fh                                          *fhStruct
		latency                                     float64
		listDirectoryOutputFile                     *listDirectoryOutputFileStruct
		listDirectoryInput                          *listDirectoryInputStruct
		listDirectoryOutput                         *listDirectoryOutputStruct
		ok                                          bool
		parentInode                                 *inodeStruct
		startTime                                   = time.Now()
		subdirectory                                string
		virtChildInodeMapIndex                      int
	)

	defer func() {
		var entriesReturned float64
		if (errno == 0) && (readDirOut != nil) {
			entriesReturned = float64(len(readDirOut.DirEnt))
		}

		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.ReadDirSuccesses.Inc()
			globals.fissionMetrics.ReadDirSuccessLatencies.Observe(latency)
			if entriesReturned != 0 {
				globals.fissionMetrics.ReadDirEntriesReturned.Add(entriesReturned)
			}

			if backend != nil {
				backend.fissionMetrics.ReadDirSuccesses.Inc()
				backend.fissionMetrics.ReadDirSuccessLatencies.Observe(latency)
				if entriesReturned != 0 {
					backend.fissionMetrics.ReadDirEntriesReturned.Add(entriesReturned)
				}
			}
		} else {
			globals.fissionMetrics.ReadDirFailures.Inc()
			globals.fissionMetrics.ReadDirFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.ReadDirFailures.Inc()
				backend.fissionMetrics.ReadDirFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

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
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if parentInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[parentInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce]")
		}
	}

	if parentInode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	_, ok = parentInode.fhSet[readDirIn.FH]
	if !ok {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}
	fh, ok = globals.fhMap[readDirIn.FH]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.fhMap[readDirIn.FH] returned !ok")
	}

	parentInode.touch(nil)

	if parentInode.inodeType == FUSERootDir {
		childDirMapLen = uint64(parentInode.virtChildInodeMap.Len()) // Will be == 2 + len(globals.config.backends)

		for {
			if curOffset >= childDirMapLen {
				globals.Unlock()
				errno = 0
				return
			}

			childDirMapIndex = int(curOffset)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildInodeMap.GetByIndex(childDirMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.GetByIndex(childDirMapIndex < childDirMapLen) returned !ok")
			}

			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [DoReadDir() case 1]")
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
					maxItems:          backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			} else {
				listDirectoryInput = &listDirectoryInputStruct{
					continuationToken: fh.prevListDirectoryOutput.nextContinuationToken,
					maxItems:          backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			}

			fh.listDirectoryInProgress = true

			globals.Unlock()

			listDirectoryOutput, err = listDirectoryWrapper(backend.context, listDirectoryInput)

			globals.Lock()

			fh.listDirectoryInProgress = false

			if err != nil {
				globals.Unlock()
				globals.logger.Printf("[WARN] unable to access backend \"%s\"", backend.dirName)
				errno = syscall.EACCES
				return
			}

			if (len(listDirectoryOutput.file) > 0) || (len(listDirectoryOutput.subdirectory) > 0) {
				parentInode.convertToPhysInodeIfNecessary()
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

			// Ensure we remember all discovered subdirectories

			for _, subdirectory = range listDirectoryOutput.subdirectory {
				_, ok = fh.listDirectorySubdirectorySet[subdirectory]
				if !ok {
					fh.listDirectorySubdirectorySet[subdirectory] = struct{}{}
					fh.listDirectorySubdirectoryList = append(fh.listDirectorySubdirectoryList, subdirectory)
				}
			}

			// Since we had to release globals.Lock during listDirectoryWrapper() call, we must restart from where we first grabbed it

			goto Restart
		}

		// At this point, we know either we are still reading fh.{prev|next}ListDirectoryOutput's
		// or we are done with all of them and may proceed to return fh.listDirectorySubdirectoryList
		// & parentInode.virtChildInodeMap entries

		curOffsetInPrevListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset
		curOffsetInNextListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen
		curOffsetInListDirectorySubdirectoryListCap = curOffsetInNextListDirectoryOutputCap + uint64(len(fh.listDirectorySubdirectoryList))
		curOffsetInVirtChildInodeMapCap = curOffsetInListDirectorySubdirectoryListCap + uint64(parentInode.virtChildInodeMap.Len())

		switch {
		case curOffset < curOffsetInPrevListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.prevListDirectoryOutput.file[curOffset-fh.prevListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInode.convertToPhysInodeIfNecessary()
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInNextListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.nextListDirectoryOutput.file[curOffset-fh.nextListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInode.convertToPhysInodeIfNecessary()
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInListDirectorySubdirectoryListCap:
			childInode = parentInode.findChildDirInode(fh.listDirectorySubdirectoryList[curOffset-curOffsetInNextListDirectoryOutputCap])
			childInode.convertToPhysInodeIfNecessary()
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInVirtChildInodeMapCap:
			virtChildInodeMapIndex = int(curOffset - curOffsetInListDirectorySubdirectoryListCap)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildInodeMap.GetByIndex(virtChildInodeMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.GetByIndex(virtChildInodeMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [DoReadDir() case 2]")
			}
		default:
			globals.Unlock()
			errno = 0
			return
		}

		curOffset++

		if !childInode.pendingDelete {
			ok = childInode.appendToReadDirOut(uint64(readDirIn.Size), readDirOut, curOffset, childInodeBasename, &curReadDirOutSize)
			if !ok {
				globals.Unlock()
				errno = 0
				return
			}
		}
	}
}

// `DoReleaseDir` implements the package fission callback to close a directory inode's file handle.
func (*globalsStruct) DoReleaseDir(inHeader *fission.InHeader, releaseDirIn *fission.ReleaseDirIn) (errno syscall.Errno) {
	var (
		backend   *backendStruct
		fh        *fhStruct
		inode     *inodeStruct
		latency   float64
		ok        bool
		startTime = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.ReleaseDirSuccesses.Inc()
			globals.fissionMetrics.ReleaseDirSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.ReleaseDirSuccesses.Inc()
				backend.fissionMetrics.ReleaseDirSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.ReleaseDirFailures.Inc()
			globals.fissionMetrics.ReleaseDirFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.ReleaseDirFailures.Inc()
				backend.fissionMetrics.ReleaseDirFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	inode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if inode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[inode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[inode.backendNonce]")
		}
	}

	_, ok = inode.fhSet[releaseDirIn.FH]
	if !ok {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}
	fh, ok = globals.fhMap[releaseDirIn.FH]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.fhMap[releaseDirIn.FH] returned !ok")
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

	delete(inode.fhSet, fh.nonce)
	delete(globals.fhMap, fh.nonce)

	inode.touch(nil)

	globals.Unlock()

	errno = 0
	return
}

// `DoFSyncDir` implements the package fission callback to ensure modified metadata and/or
// content for a directory inode is flushed (a no-op for this FUSE file system).
func (*globalsStruct) DoFSyncDir(inHeader *fission.InHeader, fSyncDirIn *fission.FSyncDirIn) (errno syscall.Errno) {
	errno = 0
	return
}

// `DoGetLK` implements the package fission callback to retrieve the state
// of a POSIX lock on the file inode (not supported).
func (*globalsStruct) DoGetLK(inHeader *fission.InHeader, getLKIn *fission.GetLKIn) (getLKOut *fission.GetLKOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoSetLK` implements the package fission callback to attempt to acquire
// a POSIX lock (i.e. "trylock", non-blocking) on a file inode (not supported).
func (*globalsStruct) DoSetLK(inHeader *fission.InHeader, setLKIn *fission.SetLKIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoSetLKW` implements the package fission callback to acquire a POSIX lock
// (i.e. non-blocking) on a file inode (not supported).
func (*globalsStruct) DoSetLKW(inHeader *fission.InHeader, setLKWIn *fission.SetLKWIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoAccess` implements the package fission callback to test for access
// permissions to an inode based solely on user's UID and GID and the
// metadata for the inode. As this is an incomplete access check, this
// FUSE file system defers such authorization checks to the kernel.
func (*globalsStruct) DoAccess(inHeader *fission.InHeader, accessIn *fission.AccessIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoCreate` implements the package fission callback to create and open a new file inode.
func (*globalsStruct) DoCreate(inHeader *fission.InHeader, createIn *fission.CreateIn) (createOut *fission.CreateOut, errno syscall.Errno) {
	var (
		backend     *backendStruct
		basename    = string(createIn.Name)
		latency     float64
		ok          bool
		parentInode *inodeStruct
		startTime   = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.CreateSuccesses.Inc()
			globals.fissionMetrics.CreateSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.CreateSuccesses.Inc()
				backend.fissionMetrics.CreateSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.CreateFailures.Inc()
			globals.fissionMetrics.CreateFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.CreateFailures.Inc()
				backend.fissionMetrics.CreateFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	parentInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if parentInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[parentInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce]")
		}
	}

	if parentInode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}
	if parentInode.inodeType == FUSERootDir {
		globals.Unlock()
		errno = syscall.EPERM
		return
	}
	if backend.readOnly {
		globals.Unlock()
		errno = syscall.EPERM
		return
	}
	_, ok = parentInode.findChildInode(basename)
	if ok {
		globals.Unlock()
		errno = syscall.EEXIST
		return
	}

	globals.Unlock()

	fmt.Printf("[TODO] fission.go::DoCreate() inHeader: %+v createIn: %+v\n", inHeader, createIn)
	errno = syscall.ENOSYS
	return
}

// `DoInterrupt` implements the package fission callback to interrupt another
// active callback (not supported).
func (*globalsStruct) DoInterrupt(inHeader *fission.InHeader, interruptIn *fission.InterruptIn) {}

// `DoBMap` implements the package fission callback to map blocks of a FUSE "blkdev" device (not supported).
func (*globalsStruct) DoBMap(inHeader *fission.InHeader, bMapIn *fission.BMapIn) (bMapOut *fission.BMapOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoDestroy` implements the package fission callback to clean up this FUSE file system.
func (*globalsStruct) DoDestroy(inHeader *fission.InHeader) (errno syscall.Errno) { return }

// `DoPoll` implements the package fission callback to poll for whether or not
// another operation (e.g. DoRead) on a file handle has data available (not supported).
func (*globalsStruct) DoPoll(inHeader *fission.InHeader, pollIn *fission.PollIn) (pollOut *fission.PollOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoBatchForget` implements the package fission callback to note that
// the kernel has removed a set of inodes from its internal caches.
func (*globalsStruct) DoBatchForget(inHeader *fission.InHeader, batchForgetIn *fission.BatchForgetIn) {
}

// `DoFAllocate` implements the package fission callback to reserve space that
// would subsequently be needed by a DoWrite callback to avoid failures due
// to space allocation unavailable when that DoWrite callback is made (not supported).
func (*globalsStruct) DoFAllocate(inHeader *fission.InHeader, fAllocateIn *fission.FAllocateIn) (errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `appendToReadDirPlusOut` appends the information about an inode in the form of a fission.DirEntPlus
// to the accumulating fission.ReadDirPlusOut struct if there is room.
func (inode *inodeStruct) appendToReadDirPlusOut(readDirPlusInSize uint64, readDirPlusOut *fission.ReadDirPlusOut, entryAttrValidSec uint64, entryAttrValidNSec uint32, dirEntPlusOff uint64, basename string, curReadDirPlusOutSize *uint64) (ok bool) {
	var (
		backend        *backendStruct
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

	if (*curReadDirPlusOutSize + dirEntPlusSize) > readDirPlusInSize {
		ok = false
		return
	}

	*curReadDirPlusOutSize += dirEntPlusSize

	mTimeSec, mTimeNSec = timeTimeToAttrTime(inode.mTime)

	if inode.inodeType == FUSERootDir {
		uid = globals.config.uid
		gid = globals.config.gid
	} else {
		backend, ok = globals.backendMap[inode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[inode.backendNonce] returned !ok")
		}

		uid = backend.uid
		gid = backend.gid
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

	ok = true
	return
}

// `DoReadDirPlus` implements the package fission callback to enumerate a directory inode's entries (verbosely).
func (*globalsStruct) DoReadDirPlus(inHeader *fission.InHeader, readDirPlusIn *fission.ReadDirPlusIn) (readDirPlusOut *fission.ReadDirPlusOut, errno syscall.Errno) {
	var (
		backend                                     *backendStruct
		childDirMapIndex                            int
		childDirMapLen                              uint64
		childInode                                  *inodeStruct
		childInodeBasename                          string
		childInodeNumber                            uint64
		curOffset                                   uint64
		curOffsetInListDirectorySubdirectoryListCap uint64
		curOffsetInNextListDirectoryOutputCap       uint64
		curOffsetInPrevListDirectoryOutputCap       uint64
		curOffsetInVirtChildInodeMapCap             uint64
		curReadDirPlusOutSize                       uint64
		dirEntPlusCountMax                          uint64
		dirEntPlusMinSize                           uint64
		entryAttrValidNSec                          uint32
		entryAttrValidSec                           uint64
		err                                         error
		fh                                          *fhStruct
		latency                                     float64
		listDirectoryOutputFile                     *listDirectoryOutputFileStruct
		listDirectoryInput                          *listDirectoryInputStruct
		listDirectoryOutput                         *listDirectoryOutputStruct
		ok                                          bool
		parentInode                                 *inodeStruct
		startTime                                   = time.Now()
		subdirectory                                string
		virtChildInodeMapIndex                      int
	)

	defer func() {
		var entriesReturned float64
		if (errno == 0) && (readDirPlusOut != nil) {
			entriesReturned = float64(len(readDirPlusOut.DirEntPlus))
		}

		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.ReadDirPlusSuccesses.Inc()
			globals.fissionMetrics.ReadDirPlusSuccessLatencies.Observe(latency)
			if entriesReturned != 0 {
				globals.fissionMetrics.ReadDirPlusEntriesReturned.Add(entriesReturned)
			}

			if backend != nil {
				backend.fissionMetrics.ReadDirPlusSuccesses.Inc()
				backend.fissionMetrics.ReadDirPlusSuccessLatencies.Observe(latency)
				if entriesReturned != 0 {
					backend.fissionMetrics.ReadDirPlusEntriesReturned.Add(entriesReturned)
				}
			}
		} else {
			globals.fissionMetrics.ReadDirPlusFailures.Inc()
			globals.fissionMetrics.ReadDirPlusFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.ReadDirPlusFailures.Inc()
				backend.fissionMetrics.ReadDirPlusFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

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
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if parentInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[parentInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce]")
		}
	}

	if parentInode.inodeType == FileObject {
		globals.Unlock()
		errno = syscall.ENOTDIR
		return
	}

	_, ok = parentInode.fhSet[readDirPlusIn.FH]
	if !ok {
		globals.Unlock()
		errno = syscall.EBADF
		return
	}
	fh, ok = globals.fhMap[readDirPlusIn.FH]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.fhMap[readDirPlusIn.FH] returned !ok")
	}

	parentInode.touch(nil)

	if parentInode.inodeType == FUSERootDir {
		childDirMapLen = uint64(parentInode.virtChildInodeMap.Len()) // Will be == 2 + len(globals.config.backends)

		for {
			if curOffset >= childDirMapLen {
				globals.Unlock()
				errno = 0
				return
			}

			childDirMapIndex = int(curOffset)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildInodeMap.GetByIndex(childDirMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.GetByIndex(childDirMapIndex < childDirMapLen) returned !ok")
			}

			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [DoReadDirPlus() case 1]")
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
					maxItems:          backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			} else {
				listDirectoryInput = &listDirectoryInputStruct{
					continuationToken: fh.prevListDirectoryOutput.nextContinuationToken,
					maxItems:          backend.directoryPageSize,
					dirPath:           parentInode.objectPath,
				}
			}

			fh.listDirectoryInProgress = true

			globals.Unlock()

			listDirectoryOutput, err = listDirectoryWrapper(backend.context, listDirectoryInput)

			globals.Lock()

			fh.listDirectoryInProgress = false

			if err != nil {
				globals.Unlock()
				globals.logger.Printf("[WARN] unable to access backend \"%s\"", backend.dirName)
				errno = syscall.EACCES
				return
			}

			if (len(listDirectoryOutput.file) > 0) || (len(listDirectoryOutput.subdirectory) > 0) {
				parentInode.convertToPhysInodeIfNecessary()
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

			// Ensure we remember all discovered subdirectories

			for _, subdirectory = range listDirectoryOutput.subdirectory {
				_, ok = fh.listDirectorySubdirectorySet[subdirectory]
				if !ok {
					fh.listDirectorySubdirectorySet[subdirectory] = struct{}{}
					fh.listDirectorySubdirectoryList = append(fh.listDirectorySubdirectoryList, subdirectory)
				}
			}

			// Since we had to release globals.Lock during listDirectoryWrapper() call, we must restart from where we first grabbed it

			goto Restart
		}

		// At this point, we know either we are still reading fh.{prev|next}ListDirectoryOutput's
		// or we are done with all of them and may proceed to return fh.listDirectorySubdirectoryList
		// & parentInode.virtChildInodeMap entries

		curOffsetInPrevListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset
		curOffsetInNextListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen
		curOffsetInListDirectorySubdirectoryListCap = curOffsetInNextListDirectoryOutputCap + uint64(len(fh.listDirectorySubdirectoryList))
		curOffsetInVirtChildInodeMapCap = curOffsetInListDirectorySubdirectoryListCap + uint64(parentInode.virtChildInodeMap.Len())

		switch {
		case curOffset < curOffsetInPrevListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.prevListDirectoryOutput.file[curOffset-fh.prevListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInode.convertToPhysInodeIfNecessary()
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInNextListDirectoryOutputCap:
			listDirectoryOutputFile = &fh.nextListDirectoryOutput.file[curOffset-fh.nextListDirectoryOutputStartingOffset]
			childInode = parentInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
			childInode.convertToPhysInodeIfNecessary()
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInListDirectorySubdirectoryListCap:
			childInode = parentInode.findChildDirInode(fh.listDirectorySubdirectoryList[curOffset-curOffsetInNextListDirectoryOutputCap])
			childInode.convertToPhysInodeIfNecessary()
			childInodeBasename = childInode.basename
		case curOffset < curOffsetInVirtChildInodeMapCap:
			virtChildInodeMapIndex = int(curOffset - curOffsetInListDirectorySubdirectoryListCap)
			childInodeBasename, childInodeNumber, ok = parentInode.virtChildInodeMap.GetByIndex(virtChildInodeMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.GetByIndex(virtChildInodeMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [DoReadDirPlus() case 2]")
			}
		default:
			globals.Unlock()
			errno = 0
			return
		}

		curOffset++

		if !childInode.pendingDelete {
			ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
			if !ok {
				globals.Unlock()
				errno = 0
				return
			}
		}
	}
}

// `DoRename2` implements the package fission callback to rename a directory entry (not supported).
func (*globalsStruct) DoRename2(inHeader *fission.InHeader, rename2In *fission.Rename2In) (errno syscall.Errno) {
	errno = syscall.EXDEV
	return
}

// `DoLSeek` implements the package fission callback to fetch the offset of the start
// of the next sequence of data or the next "hole" in data of the content of a file
// inode (not supported).
func (*globalsStruct) DoLSeek(inHeader *fission.InHeader, lSeekIn *fission.LSeekIn) (lSeekOut *fission.LSeekOut, errno syscall.Errno) {
	errno = syscall.ENOSYS
	return
}

// `DoStatX` implements the package fission callback to fetch netadata
// information about an inode. This is a slightly more complete result
// than that provided in DoLookup, DoGetAttr, and DoReadDirPlus.
func (*globalsStruct) DoStatX(inHeader *fission.InHeader, statXIn *fission.StatXIn) (statXOut *fission.StatXOut, errno syscall.Errno) {
	var (
		attrValidNSec uint32
		attrValidSec  uint64
		backend       *backendStruct
		gid           uint32
		latency       float64
		mTimeNSec     uint32
		mTimeSec      uint64
		ok            bool
		startTime     = time.Now()
		thisInode     *inodeStruct
		uid           uint32
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if errno == 0 {
			globals.fissionMetrics.StatXSuccesses.Inc()
			globals.fissionMetrics.StatXSuccessLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.StatXSuccesses.Inc()
				backend.fissionMetrics.StatXSuccessLatencies.Observe(latency)
			}
		} else {
			globals.fissionMetrics.StatXFailures.Inc()
			globals.fissionMetrics.StatXFailureLatencies.Observe(latency)
			if backend != nil {
				backend.fissionMetrics.StatXFailures.Inc()
				backend.fissionMetrics.StatXFailureLatencies.Observe(latency)
			}
		}
		globals.Unlock()
	}()

	globals.Lock()

	thisInode, ok = globals.inodeMap[inHeader.NodeID]
	if !ok {
		backend = nil
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	if thisInode.backendNonce == 0 {
		backend = nil
	} else {
		backend, ok = globals.backendMap[thisInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[thisInode.backendNonce]")
		}
	}

	if thisInode.pendingDelete {
		globals.Unlock()
		errno = syscall.ENOENT
		return
	}

	thisInode.touch(nil)

	switch thisInode.inodeType {
	case FileObject:
		uid = uint32(backend.uid)
		gid = uint32(backend.gid)
	case FUSERootDir:
		uid = uint32(globals.config.uid)
		gid = uint32(globals.config.gid)
	case BackendRootDir:
		uid = uint32(backend.uid)
		gid = uint32(backend.gid)
	case PseudoDir:
		uid = uint32(backend.uid)
		gid = uint32(backend.gid)
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] unrecognized inodeType (%v)", thisInode.inodeType)
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
