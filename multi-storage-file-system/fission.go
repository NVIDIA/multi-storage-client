package main

import (
	"fmt"
	"log"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/fission/v4"
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
		fissionLogger       = log.New(globals.logger.Writer(), "[FISSION] ", globals.logger.Flags()) // set prefix to differentiate package fission logging
		fissionVolumeConfig *fission.VolumeConfig
	)

	fissionVolumeConfig = &fission.VolumeConfig{
		VolumeName:         globals.config.mountName,
		MountpointDirPath:  globals.config.mountPoint,
		FuseSubtype:        fuseSubtype,
		MaxRead:            maxRead,
		MaxWrite:           maxWrite,
		DefaultPermissions: true,
		AllowOther:         globals.config.allowOther,
		NumWorkers:         int(globals.config.fuseWorkers),
		PerWorkerFD:        globals.config.fuseFdPerWorker,
		Callbacks:          &globals,
		Logger:             fissionLogger,
		ErrChan:            globals.errChan,
	}

	globals.fissionVolume = fission.NewVolume(fissionVolumeConfig)

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

func bpTreeDirEntType(inodeType uint32) uint32 {
	if inodeType == FileObject {
		return syscall.DT_REG
	}
	return syscall.DT_DIR
}

// `DoLookup` implements the package fission callback to fetch metadata
// information about a directory entry (if present).
func (*globalsStruct) DoLookup(inHeader *fission.InHeader, lookupIn *fission.LookupIn) (lookupOut *fission.LookupOut, errno syscall.Errno) {
	var (
		backend            *backendStruct
		childInode         *inodeStruct
		childDirInfo       DirEntryInfo
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
		globalsLock("fission.go:160:3:funcLit@158")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:179:2:(*globalsStruct).DoLookup")

	parentInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		// We no longer know how to map inHeader.NodeID (an inodeNumber) to the parentInode
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}

	if parentInode.inodeType == FUSERootDir {
		// If lookupIn.Name exists, it is in parentInode's portion of the global {phys|virt}ChildDirEntryMap

		childDirInfo, ok = globals.physChildDirEntryMap.getByBasename(parentInode.inodeNumber, string(lookupIn.Name))
		if !ok {
			childDirInfo, ok = globals.virtChildDirEntryMap.getByBasename(parentInode.inodeNumber, string(lookupIn.Name))
			if !ok {
				globalsUnlock()
				errno = syscall.ENOENT
				return
			}
		}

		childInode, ok = globals.inodeMap.get(childDirInfo.InodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childDirInfo.InodeNumber) returned !ok [DoLookup()]")
		}
	} else {
		// We only know parentInode is a BackendRootDir or a PseudoDir

		childInode, ok = parentInode.findChildInode(string(lookupIn.Name))
		if !ok || childInode.pendingDelete {
			globalsUnlock()
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

	globalsUnlock()

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
		globalsLock("fission.go:301:3:funcLit@299")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:320:2:(*globalsStruct).DoGetAttr")

	thisInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
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

	globalsUnlock()

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
		globalsLock("fission.go:441:3:funcLit@439")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:460:2:(*globalsStruct).DoMkDir")

	parentInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		// We no longer know how to map inHeader.NodeID (an inodeNumber) to the parentInode
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}
	if parentInode.inodeType == FUSERootDir {
		// Never allowed in FUSERootDir
		globalsUnlock()
		errno = syscall.EPERM
		return
	}
	if backend.readOnly {
		// Never allowed in a readOnly backend
		globalsUnlock()
		errno = syscall.EPERM
		return
	}

	_, ok = parentInode.findChildInode(basename)
	if ok {
		// We just return EEXIST if we find a phys or virt child dir entry (whether or not it is a dir or a file)
		globalsUnlock()
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

	globalsUnlock()

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
		globalsLock("fission.go:564:3:funcLit@562")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:583:2:(*globalsStruct).DoUnlink")

	parentInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.EPERM
		return
	}
	if parentInode.inodeType == FileObject {
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}
	if backend.readOnly {
		globalsUnlock()
		errno = syscall.EPERM
		return
	}

	childInode, ok = parentInode.findChildInode(basename)
	if !ok {
		globalsUnlock()
		errno = syscall.ENOENT
		return
	}

	if childInode.pendingDelete {
		globalsUnlock()
		errno = syscall.ENOENT
		return
	}
	if childInode.inodeType != FileObject {
		childInode.touch(nil)
		globalsUnlock()
		errno = syscall.EISDIR
		return
	}

	// One way or another, childInode will be deleted

	childInode.pendingDelete = true
	childInode.touch(nil)

	if len(childInode.fhSet) != 0 {
		// Since fhSet is not empty, cannot delete childInode

		globalsUnlock()

		errno = 0
		return
	}

	globalsUnlock()

	childInode.finishPendingDelete()

	errno = 0
	return
}

// `DoRmDir` implements the package fission callback to remove a directory inode.
func (*globalsStruct) DoRmDir(inHeader *fission.InHeader, rmDirIn *fission.RmDirIn) (errno syscall.Errno) {
	var (
		backend                             *backendStruct
		basename                            = string(rmDirIn.Name)
		childInode                          *inodeStruct
		childInodePhysChildDirEntryMapLimit uint64
		childInodePhysChildDirEntryMapStart uint64
		childInodeVirtChildDirEntryMapLimit uint64
		childInodeVirtChildDirEntryMapStart uint64
		latency                             float64
		ok                                  bool
		parentInode                         *inodeStruct
		startTime                           = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globalsLock("fission.go:680:3:funcLit@678")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:699:2:(*globalsStruct).DoRmDir")

	parentInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		// We no longer know how to map inHeader.NodeID (an inodeNumber) to the parentInode
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}
	if parentInode.inodeType == FUSERootDir {
		// Never allowed in FUSERootDir
		globalsUnlock()
		errno = syscall.EPERM
		return
	}
	if backend.readOnly {
		// Never allowed in a readOnly backend
		globalsUnlock()
		errno = syscall.EPERM
		return
	}

	childInode, ok = parentInode.findChildInode(basename)
	if !ok {
		// We didn't find the child directory, so just return ENOENT
		globalsUnlock()
		errno = syscall.ENOENT
		return
	}
	if childInode.inodeType != PseudoDir {
		// This child exists but is not a directory, so we return ENOTDIR
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}
	if len(childInode.fhSet) > 0 {
		// We return EBUSY if the directory is currently "open"
		globalsUnlock()
		errno = syscall.EBUSY
		return
	}
	childInodePhysChildDirEntryMapStart, childInodePhysChildDirEntryMapLimit = globals.physChildDirEntryMap.getIndexRange(childInode.inodeNumber)
	childInodeVirtChildDirEntryMapStart, childInodeVirtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(childInode.inodeNumber)
	if ((childInodePhysChildDirEntryMapLimit - childInodePhysChildDirEntryMapStart) > 0) || ((childInodeVirtChildDirEntryMapLimit - childInodeVirtChildDirEntryMapStart) > 2) {
		// Return ENOTEMPTY if childInode has any children
		globalsUnlock()
		errno = syscall.ENOTEMPTY
		return
	}

	// From here, we know we will succeed

	if !childInode.xTime.IsZero() {
		ok = globals.inodeEvictionQueue.remove(childInode)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.remove(childInode) returned !ok")
		}
	}

	ok = globals.virtChildDirEntryMap.delete(childInode.inodeNumber, DotDirEntryBasename)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(childInode.inodeNumber, DotDirEntryBasename) returned !ok")
	}
	ok = globals.virtChildDirEntryMap.delete(childInode.inodeNumber, DotDotDirEntryBasename)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(childInode.inodeNumber, DotDotDirEntryBasename) returned !ok")
	}

	if childInode.isVirt {
		ok = globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, basename) returned !ok")
		}
	} else {
		ok = globals.physChildDirEntryMap.delete(parentInode.inodeNumber, basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.delete(parentInode.inodeNumber, basename) returned !ok")
		}
	}

	ok = globals.inodeMap.delete(childInode.inodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap.delete(childInode.inodeNumber) returned !ok")
	}

	parentInode.touch(nil)

	globalsUnlock()

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
		globalsLock("fission.go:846:3:funcLit@844")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:865:2:(*globalsStruct).DoOpen")

	inode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOENT
		return
	}
	if inode.inodeType != FileObject {
		globalsUnlock()
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
				globalsUnlock()
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
		globalsUnlock()
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

	globalsUnlock()

	errno = 0
	return
}

// `DoRead` implements the package fission callback to read a portion of a file inode's contents.
func (*globalsStruct) DoRead(inHeader *fission.InHeader, readIn *fission.ReadIn) (readOut *fission.ReadOut, errno syscall.Errno) {
	var (
		backend                         *backendStruct
		cacheLineHits                   uint64 // As this is the fall-thru condition, includes +cacheMisses+cacheWaits
		cacheLineNumber                 uint64
		cacheLineNumberMaxInBackend     uint64
		cacheLineMisses                 uint64
		cacheLineOffsetLimit            uint64 // One greater than offset to last byte to return
		cacheLineOffsetStart            uint64
		cacheLineWaiter                 sync.WaitGroup
		cacheLineWaits                  uint64
		cacheLinesToPotentiallyPrefetch uint64
		copyDstStart                    int    // len(readOut.Data) snapshot for optimistic-copy rollback
		copyGeneration                  uint64 // dataCacheLineTracker.contentGeneration snapshot
		copyLength                      uint64
		copySrcStart                    uint64
		curOffset                       = readIn.Offset
		dataCacheLineNumber             uint64
		dataCacheLineNumbers            []uint64
		dataCacheLineTracker            *dataCacheLineTrackerStruct
		fh                              *fhStruct
		inode                           *inodeStruct
		latency                         float64
		ok                              bool
		prefetchCacheLinesIssued        uint64
		prefetchCacheLineNumber         uint64
		prefetchCacheLineNumberMax      uint64
		prefetchCacheLineNumberMin      uint64
		prefetchCacheLineNumbers        []uint64
		startTime                       = time.Now()
	)

	defer func() {
		latency = time.Since(startTime).Seconds()
		globalsLock("fission.go:982:3:funcLit@980")
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
		if cacheLineHits >= (cacheLineMisses + cacheLineWaits) {
			cacheLineHits -= cacheLineMisses + cacheLineWaits
		} else {
			cacheLineHits = 0
		}
		if backend == nil {
			if cacheLineHits != 0 {
				globals.fissionMetrics.ReadCacheHits.Add(float64(cacheLineHits))
			}
			if cacheLineMisses != 0 {
				globals.fissionMetrics.ReadCacheMisses.Add(float64(cacheLineMisses))
			}
			if cacheLineWaits != 0 {
				globals.fissionMetrics.ReadCacheWaits.Add(float64(cacheLineWaits))
			}
			if prefetchCacheLinesIssued != 0 {
				globals.fissionMetrics.ReadCachePrefetches.Add(float64(prefetchCacheLinesIssued))
			}
		} else {
			if cacheLineHits != 0 {
				globals.fissionMetrics.ReadCacheHits.Add(float64(cacheLineHits))
				backend.fissionMetrics.ReadCacheHits.Add(float64(cacheLineHits))
			}
			if cacheLineMisses != 0 {
				globals.fissionMetrics.ReadCacheMisses.Add(float64(cacheLineMisses))
				backend.fissionMetrics.ReadCacheMisses.Add(float64(cacheLineMisses))
			}
			if cacheLineWaits != 0 {
				globals.fissionMetrics.ReadCacheWaits.Add(float64(cacheLineWaits))
				backend.fissionMetrics.ReadCacheWaits.Add(float64(cacheLineWaits))
			}
			if prefetchCacheLinesIssued != 0 {
				globals.fissionMetrics.ReadCachePrefetches.Add(float64(prefetchCacheLinesIssued))
				backend.fissionMetrics.ReadCachePrefetches.Add(float64(prefetchCacheLinesIssued))
			}
		}
		globalsUnlock()
	}()

	readOut = &fission.ReadOut{
		Data: make([]byte, 0, readIn.Size),
	}

	for len(readOut.Data) < cap(readOut.Data) {
		globalsLock("fission.go:1046:3:(*globalsStruct).DoRead")

		inode, ok = globals.inodeMap.get(inHeader.NodeID)
		if !ok {
			backend = nil
			globalsUnlock()
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
			globalsUnlock()
			errno = syscall.EBADF
			return
		}

		_, ok = inode.fhSet[readIn.FH]
		if !ok {
			globalsUnlock()
			errno = syscall.EBADF
			return
		}
		fh, ok = globals.fhMap[readIn.FH]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.fhMap[readIn.FH] returned !ok")
		}
		if !fh.allowReads {
			globalsUnlock()
			errno = syscall.EBADF
			return
		}

		inode.touch(nil)

		if curOffset >= inode.sizeInBackend {
			// We have reached EOF

			globalsUnlock()

			break
		}

		cacheLineNumber = curOffset / globals.config.cacheLineSize

		dataCacheLineNumber, ok = inode.cacheMap[cacheLineNumber]
		if !ok {
			cacheLineMisses++

			globals.fissionVolume.HighLatencyCallback(inHeader)

			prefetchCacheLineNumbers = prefetchCacheLineNumbers[:0]

			if globals.config.cacheLinesToPrefetch > 0 {
				cacheLineNumberMaxInBackend = ((inode.sizeInBackend + globals.config.cacheLineSize - 1) / globals.config.cacheLineSize) - 1

				if cacheLineNumberMaxInBackend >= (cacheLineNumber + globals.config.cacheLinesToPrefetch) {
					cacheLinesToPotentiallyPrefetch = globals.config.cacheLinesToPrefetch
				} else {
					cacheLinesToPotentiallyPrefetch = cacheLineNumberMaxInBackend - cacheLineNumber
				}

				if cacheLinesToPotentiallyPrefetch >= globals.config.cacheLines {
					cacheLinesToPotentiallyPrefetch = globals.config.cacheLines - 1
				}

				if cacheLinesToPotentiallyPrefetch > 0 {
					prefetchCacheLineNumberMin = cacheLineNumber + 1
					prefetchCacheLineNumberMax = prefetchCacheLineNumberMin + cacheLinesToPotentiallyPrefetch - 1

					for prefetchCacheLineNumber = prefetchCacheLineNumberMin; prefetchCacheLineNumber <= prefetchCacheLineNumberMax; prefetchCacheLineNumber++ {
						_, ok = inode.cacheMap[prefetchCacheLineNumber]
						if !ok {
							prefetchCacheLineNumbers = append(prefetchCacheLineNumbers, prefetchCacheLineNumber)
						}
					}
				}
			}

			dataCacheLineNumbers, _ = allocateDataCacheLines(1 + uint64(len(prefetchCacheLineNumbers)))

			globalsLock("fission.go:1137:4:(*globalsStruct).DoRead")

			inode, ok = globals.inodeMap.get(inHeader.NodeID)
			if !ok {
				backend = nil
				releaseDataCacheLines(dataCacheLineNumbers)
				globalsUnlock()
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
				releaseDataCacheLines(dataCacheLineNumbers)
				globalsUnlock()
				errno = syscall.EBADF
				return
			}

			_, ok = inode.fhSet[readIn.FH]
			if !ok {
				releaseDataCacheLines(dataCacheLineNumbers)
				globalsUnlock()
				errno = syscall.EBADF
				return
			}
			fh, ok = globals.fhMap[readIn.FH]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.fhMap[readIn.FH] returned !ok")
			}
			if !fh.allowReads {
				releaseDataCacheLines(dataCacheLineNumbers)
				globalsUnlock()
				errno = syscall.EBADF
				return
			}

			inode.touch(nil)

			if curOffset >= inode.sizeInBackend {
				releaseDataCacheLines(dataCacheLineNumbers)
				globalsUnlock()
				break
			}

			cacheLineNumber = curOffset / globals.config.cacheLineSize
			_, ok = inode.cacheMap[cacheLineNumber]
			if ok {
				releaseDataCacheLines(dataCacheLineNumbers)
				globalsUnlock()
				continue
			}

			if len(dataCacheLineNumbers) == 0 {
				dumpStack()
				globals.logger.Fatalf("[FATAL] allocateDataCacheLines() returned no data cache lines")
			}

			dataCacheLineTracker = &globals.dataCacheLinesTracker[dataCacheLineNumbers[0]]
			dataCacheLineNumbers = dataCacheLineNumbers[1:]

			cacheLineWaiter.Add(1)
			dataCacheLineTracker.waiters = make([]*sync.WaitGroup, 1)
			dataCacheLineTracker.waiters[0] = &cacheLineWaiter
			dataCacheLineTracker.contentLength = 0
			dataCacheLineTracker.contentGeneration++
			dataCacheLineTracker.inodeNumber = inode.inodeNumber
			dataCacheLineTracker.lineNumber = cacheLineNumber
			dataCacheLineTracker.eTag = ""

			inode.cacheMap[cacheLineNumber] = dataCacheLineTracker.pos
			inode.inboundCacheLineCount++
			globals.dataCacheLineInboundLRU.pushTail(dataCacheLineTracker)

			globals.dataCacheActivityWG.Add(1)
			go dataCacheLineTracker.fetch()

			for _, prefetchCacheLineNumber = range prefetchCacheLineNumbers {
				if len(dataCacheLineNumbers) == 0 {
					break
				}

				_, ok = inode.cacheMap[prefetchCacheLineNumber]
				if ok {
					continue
				}

				dataCacheLineTracker = &globals.dataCacheLinesTracker[dataCacheLineNumbers[0]]
				dataCacheLineNumbers = dataCacheLineNumbers[1:]

				dataCacheLineTracker.waiters = make([]*sync.WaitGroup, 0, 1)
				dataCacheLineTracker.contentLength = 0
				dataCacheLineTracker.contentGeneration++
				dataCacheLineTracker.inodeNumber = inode.inodeNumber
				dataCacheLineTracker.lineNumber = prefetchCacheLineNumber
				dataCacheLineTracker.eTag = ""

				inode.cacheMap[prefetchCacheLineNumber] = dataCacheLineTracker.pos
				inode.inboundCacheLineCount++
				globals.dataCacheLineInboundLRU.pushTail(dataCacheLineTracker)

				globals.dataCacheActivityWG.Add(1)
				go dataCacheLineTracker.fetch()

				prefetchCacheLinesIssued++
			}

			releaseDataCacheLines(dataCacheLineNumbers)

			globalsUnlock()

			cacheLineWaiter.Wait()

			continue
		}

		if dataCacheLineNumber >= uint64(len(globals.dataCacheLinesTracker)) {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inode.cacheMap[cacheLineNumber] returned out-of-range dataCacheLineNumber")
		}

		dataCacheLineTracker = &globals.dataCacheLinesTracker[dataCacheLineNumber]

		if dataCacheLineTracker.state == CacheLineInbound {
			cacheLineWaits++

			globals.fissionVolume.HighLatencyCallback(inHeader)

			cacheLineWaiter.Add(1)
			dataCacheLineTracker.waiters = append(dataCacheLineTracker.waiters, &cacheLineWaiter)

			globalsUnlock()

			cacheLineWaiter.Wait()

			continue
		}

		if dataCacheLineTracker.state != CacheLineClean {
			dumpStack()
			globals.logger.Fatalf("[FATAL] dataCacheLineTracker.state(%v) != CacheLineClean(%v)", dataCacheLineTracker.state, CacheLineClean)
		}

		if dataCacheLineTracker.fetchFailed {
			// The backend read that was supposed to populate this cache line
			// failed. Surface EIO to the caller rather than serving empty/short
			// content (which previously produced an inverted slice and panicked),
			// and evict the line so a subsequent read re-fetches it.
			delete(inode.cacheMap, cacheLineNumber)
			globals.dataCacheLineCleanLRU.popThis(dataCacheLineTracker)
			dataCacheLineTracker.free()
			globalsUnlock()
			errno = syscall.EIO
			return
		}

		cacheLineHits++ // Note that this is the fall-thru condition that counts resolved (cacheLine)Misses & (cacheLine)Waits as (subsequent) Hits

		dataCacheLineTracker.touch()

		cacheLineOffsetStart = curOffset - (cacheLineNumber * globals.config.cacheLineSize)

		cacheLineOffsetLimit = cacheLineOffsetStart + uint64((cap(readOut.Data) - len(readOut.Data)))
		if cacheLineOffsetLimit > globals.config.cacheLineSize {
			cacheLineOffsetLimit = globals.config.cacheLineSize
		}
		if cacheLineOffsetLimit > dataCacheLineTracker.contentLength {
			cacheLineOffsetLimit = dataCacheLineTracker.contentLength
		}

		if cacheLineOffsetLimit <= cacheLineOffsetStart {
			// We have reached EOF (==) or, defensively, a short/empty cache line
			// where the limit fell below the start (<). Never slice with lo > hi.

			globalsUnlock()

			break
		}

		// Optimistic-lock copy: perform the cache-line -> reply memcpy WITHOUT
		// holding the global lock, so concurrent warm reads do not serialize on
		// it (the copy-under-lock was the warm-read throughput ceiling at high
		// thread counts). globals.dataCacheLinesContent (the cache-line content buffer) and
		// globals.dataCacheLinesTracker are fixed allocations for the life of the
		// mount, so reading from them after unlock is memory-safe. The only hazard
		// is that this line is evicted/refetched mid-copy (yielding wrong bytes);
		// .contentGeneration is bumped on every free()/fetch(), so we snapshot it
		// under the lock, copy unlocked, then re-validate under the lock and retry
		// the same offset on mismatch.
		copyGeneration = dataCacheLineTracker.contentGeneration
		copySrcStart = dataCacheLineTracker.contentStart + cacheLineOffsetStart
		copyLength = cacheLineOffsetLimit - cacheLineOffsetStart
		copyDstStart = len(readOut.Data)

		globalsUnlock()

		readOut.Data = append(readOut.Data, globals.dataCacheLinesContent[copySrcStart:copySrcStart+copyLength]...)

		globalsLock("fission.go:DoRead:optimistic-copy-revalidate")

		if dataCacheLineTracker.contentGeneration != copyGeneration {
			// The cache line was evicted/refetched while we copied; discard the
			// bytes we appended and retry this same offset.
			readOut.Data = readOut.Data[:copyDstStart]
			globalsUnlock()
			continue
		}

		globalsUnlock()

		curOffset += copyLength
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
	globalsLock("fission.go:1331:2:(*globalsStruct).DoStatFS")

	statFSOut = &fission.StatFSOut{
		KStatFS: fission.KStatFS{
			Blocks:  uint64(math.MaxUint64) / statFSBlkSize,
			BFree:   uint64(math.MaxUint64) / statFSBlkSize,
			BAvail:  uint64(math.MaxUint64) / statFSBlkSize,
			Files:   uint64(globals.inodeMap.len()),
			FFree:   uint64(math.MaxUint64) - globals.lastNonce,
			BSize:   uint32(globals.config.cacheLineSize),
			NameLen: maxNameLen,
			FRSize:  uint32(globals.config.cacheLineSize),
			Padding: 0,
			Spare:   [6]uint32{0, 0, 0, 0, 0, 0},
		},
	}

	globals.fissionMetrics.StatFSCalls.Inc()

	globalsUnlock()

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
		globalsLock("fission.go:1369:3:funcLit@1367")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:1388:2:(*globalsStruct).DoRelease")

	inode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.EBADF
		return
	}

	_, ok = inode.fhSet[releaseIn.FH]
	if !ok {
		inode.touch(nil)
		globalsUnlock()
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
		globalsUnlock()

		errno = 0
		return
	}

	globalsUnlock()

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
		globalsLock("fission.go:1528:3:funcLit@1526")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:1547:2:(*globalsStruct).DoOpenDir")

	inode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}

	if inode.inodeType == FUSERootDir {
		fh = &fhStruct{
			nonce: fetchNonce(),
			inode: inode,
		}
	} else {
		canServeFromBPTree := false
		if globals.physChildDirEntryMap != nil && backend != nil && backend.readOnly {
			if globals.physChildDirEntryMap.lenForParent(inode.inodeNumber) > 0 {
				canServeFromBPTree = true
			}
		}

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
			serveFromBPTree:                       canServeFromBPTree,
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

	globalsUnlock()

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
		childDirMapLen                              uint64
		childInode                                  *inodeStruct
		childInodeBasename                          string
		childDirInfo                                DirEntryInfo
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
		parentInodeVirtChildDirEntryMapLimit        uint64
		parentInodeVirtChildDirEntryMapStart        uint64
		startTime                                   = time.Now()
		subdirectory                                string
		virtChildDirEntryMapIndex                   uint64
	)

	defer func() {
		var entriesReturned float64
		if (errno == 0) && (readDirOut != nil) {
			entriesReturned = float64(len(readDirOut.DirEnt))
		}

		latency = time.Since(startTime).Seconds()
		globalsLock("fission.go:1688:3:funcLit@1681")
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
		globalsUnlock()
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

	globalsLock("fission.go:1726:2:(*globalsStruct).DoReadDir")

Restart:

	parentInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}

	_, ok = parentInode.fhSet[readDirIn.FH]
	if !ok {
		globalsUnlock()
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
		parentInodeVirtChildDirEntryMapStart, parentInodeVirtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
		childDirMapLen = parentInodeVirtChildDirEntryMapLimit - parentInodeVirtChildDirEntryMapStart // Will be == 2 + len(globals.config.backends)

		for {
			if curOffset >= childDirMapLen {
				globalsUnlock()
				errno = 0
				return
			}

			childInodeBasename, childDirInfo, ok = globals.virtChildDirEntryMap.getByIndex(parentInodeVirtChildDirEntryMapStart + curOffset)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.GetByIndex(childDirMapIndex < childDirMapLen) returned !ok")
			}

			childInode, ok = globals.inodeMap.get(childDirInfo.InodeNumber)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childDirInfo.InodeNumber) returned !ok [DoReadDir() case 1]")
			}

			curOffset++

			ok = childInode.appendToReadDirOut(uint64(readDirIn.Size), readDirOut, curOffset, childInodeBasename, &curReadDirOutSize)
			if !ok {
				globalsUnlock()
				errno = 0
				return
			}
		}
	}

	// If we reach here, we know parentInode.inodeType == BackendRootDir | PseudoDir

	if fh.listDirectoryInProgress {
		globalsUnlock()
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

			globalsUnlock()

			listDirectoryOutput, err = listDirectoryWrapper(backend.context, listDirectoryInput)

			globalsLock("fission.go:1849:4:(*globalsStruct).DoReadDir")

			fh.listDirectoryInProgress = false

			if err != nil {
				globalsUnlock()
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

			// Since we had to release the global lock (globalsUnlock) during listDirectoryWrapper() call, we must restart from where we first grabbed it

			goto Restart
		}

		// At this point, we know either we are still reading fh.{prev|next}ListDirectoryOutput's
		// or we are done with all of them and may proceed to return fh.listDirectorySubdirectoryList
		// & parentInode's virtChildDirEntryMap entries

		parentInodeVirtChildDirEntryMapStart, parentInodeVirtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(parentInode.inodeNumber)

		curOffsetInPrevListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset
		curOffsetInNextListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen
		curOffsetInListDirectorySubdirectoryListCap = curOffsetInNextListDirectoryOutputCap + uint64(len(fh.listDirectorySubdirectoryList))
		curOffsetInVirtChildInodeMapCap = curOffsetInListDirectorySubdirectoryListCap + (parentInodeVirtChildDirEntryMapLimit - parentInodeVirtChildDirEntryMapStart)

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
			virtChildDirEntryMapIndex = parentInodeVirtChildDirEntryMapStart + (curOffset - curOffsetInListDirectorySubdirectoryListCap)
			childInodeBasename, childDirInfo, ok = globals.virtChildDirEntryMap.getByIndex(virtChildDirEntryMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.getByIndex(virtChildDirEntryMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap.get(childDirInfo.InodeNumber)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childDirInfo.InodeNumber) returned !ok [DoReadDir() case 2]")
			}
		default:
			globalsUnlock()
			errno = 0
			return
		}

		curOffset++

		if !childInode.pendingDelete {
			ok = childInode.appendToReadDirOut(uint64(readDirIn.Size), readDirOut, curOffset, childInodeBasename, &curReadDirOutSize)
			if !ok {
				globalsUnlock()
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
		globalsLock("fission.go:1965:3:funcLit@1963")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:1984:2:(*globalsStruct).DoReleaseDir")

	inode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.EBADF
		return
	}
	fh, ok = globals.fhMap[releaseDirIn.FH]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.fhMap[releaseDirIn.FH] returned !ok")
	}

	if inode.inodeType == FileObject {
		globalsUnlock()
		errno = syscall.EBADF
		return
	}

	if (inode.inodeType != FUSERootDir) && fh.listDirectoryInProgress {
		globalsUnlock()
		errno = syscall.EACCES
		return
	}

	delete(inode.fhSet, fh.nonce)
	delete(globals.fhMap, fh.nonce)

	inode.touch(nil)

	globalsUnlock()

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
		globalsLock("fission.go:2089:3:funcLit@2087")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:2108:2:(*globalsStruct).DoCreate")

	parentInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}
	if parentInode.inodeType == FUSERootDir {
		globalsUnlock()
		errno = syscall.EPERM
		return
	}
	if backend.readOnly {
		globalsUnlock()
		errno = syscall.EPERM
		return
	}
	_, ok = parentInode.findChildInode(basename)
	if ok {
		globalsUnlock()
		errno = syscall.EEXIST
		return
	}

	globalsUnlock()

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
		childDirMapLen                              uint64
		childInode                                  *inodeStruct
		childInodeBasename                          string
		childDirInfo                                DirEntryInfo
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
		parentInodeVirtChildDirEntryMapLimit        uint64
		parentInodeVirtChildDirEntryMapStart        uint64
		startTime                                   = time.Now()
		subdirectory                                string
		virtChildDirEntryMapIndex                   uint64
	)

	defer func() {
		var entriesReturned float64
		if (errno == 0) && (readDirPlusOut != nil) {
			entriesReturned = float64(len(readDirPlusOut.DirEntPlus))
		}

		latency = time.Since(startTime).Seconds()
		globalsLock("fission.go:2310:3:funcLit@2303")
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
		globalsUnlock()
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

	globalsLock("fission.go:2350:2:(*globalsStruct).DoReadDirPlus")

Restart:

	parentInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
		errno = syscall.ENOTDIR
		return
	}

	_, ok = parentInode.fhSet[readDirPlusIn.FH]
	if !ok {
		globalsUnlock()
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
		parentInodeVirtChildDirEntryMapStart, parentInodeVirtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
		childDirMapLen = parentInodeVirtChildDirEntryMapLimit - parentInodeVirtChildDirEntryMapStart // Will be == 2 + len(globals.config.backends)

		for {
			if curOffset >= childDirMapLen {
				globalsUnlock()
				errno = 0
				return
			}

			childInodeBasename, childDirInfo, ok = globals.virtChildDirEntryMap.getByIndex(parentInodeVirtChildDirEntryMapStart + curOffset)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.GetByIndex(childDirMapIndex < childDirMapLen) returned !ok")
			}

			childInode, ok = globals.inodeMap.get(childDirInfo.InodeNumber)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childDirInfo.InodeNumber) returned !ok [DoReadDirPlus() case 1]")
			}

			curOffset++

			ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
			if !ok {
				globalsUnlock()
				errno = 0
				return
			}
		}
	}

	// If we reach here, we know parentInode.inodeType == BackendRootDir | PseudoDir

	if fh.serveFromBPTree {
		globals.logger.Printf("[TRACE] DoReadDirPlus: serving from B+Tree/DB for inode %d (objectPath=%q)", parentInode.inodeNumber, parentInode.objectPath)
		bpPhysStart, bpPhysLimit := globals.physChildDirEntryMap.getIndexRange(parentInode.inodeNumber)
		bpPhysCount := bpPhysLimit - bpPhysStart

		bpVirtStart, bpVirtLimit := globals.virtChildDirEntryMap.getIndexRange(parentInode.inodeNumber)
		virtDotCount := bpVirtLimit - bpVirtStart
		totalEntries := bpPhysCount + virtDotCount

		for curOffset < totalEntries {
			if curOffset < virtDotCount {
				childInodeBasename, childDirInfo, ok = globals.virtChildDirEntryMap.getByIndex(bpVirtStart + curOffset)
				if !ok {
					dumpStack()
					globals.logger.Printf("[ERROR] globals.virtChildDirEntryMap.getByIndex returned !ok [DoReadDirPlus B+Tree fast-path virt, parent=%d offset=%d] — returning partial readdir", parentInode.inodeNumber, bpVirtStart+curOffset)
					break
				}
				childInode, ok = globals.inodeMap.get(childDirInfo.InodeNumber)
				if !ok {
					dumpStack()
					globals.logger.Printf("[ERROR] globals.inodeMap.get(%d) returned !ok [DoReadDirPlus B+Tree fast-path virt, parent=%d] — returning partial readdir", childDirInfo.InodeNumber, parentInode.inodeNumber)
					break
				}
				curOffset++
				ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
				if !ok {
					globalsUnlock()
					errno = 0
					return
				}
			} else {
				bpBasename, bpInfo, bpOk := globals.physChildDirEntryMap.getByIndex(parentInode.inodeNumber, bpPhysStart+(curOffset-virtDotCount))
				if !bpOk {
					dumpStack()
					globals.logger.Printf("[ERROR] globals.physChildDirEntryMap.getByIndex returned !ok [DoReadDirPlus B+Tree fast-path phys, parent=%d offset=%d] — returning partial readdir", parentInode.inodeNumber, bpPhysStart+(curOffset-virtDotCount))
					break
				}
				curOffset++
				bpMTimeSec, bpMTimeNSec := uint64(0), uint32(0)
				if bpInfo.MTimeUnixNano > 0 {
					bpT := time.Unix(0, bpInfo.MTimeUnixNano)
					bpMTimeSec = uint64(bpT.Unix())
					bpMTimeNSec = uint32(bpT.Nanosecond())
				}
				dirEntPlus := fission.DirEntPlus{
					EntryOut: fission.EntryOut{
						NodeID:         bpInfo.InodeNumber,
						Generation:     0,
						EntryValidSec:  entryAttrValidSec,
						AttrValidSec:   entryAttrValidSec,
						EntryValidNSec: entryAttrValidNSec,
						AttrValidNSec:  entryAttrValidNSec,
						Attr: fission.Attr{
							Ino:       bpInfo.InodeNumber,
							Size:      bpInfo.Size,
							Blocks:    (bpInfo.Size + 511) / 512,
							ATimeSec:  bpMTimeSec,
							MTimeSec:  bpMTimeSec,
							CTimeSec:  bpMTimeSec,
							ATimeNSec: bpMTimeNSec,
							MTimeNSec: bpMTimeNSec,
							CTimeNSec: bpMTimeNSec,
							Mode:      bpInfo.Mode,
							NLink:     1,
						},
					},
					DirEnt: fission.DirEnt{
						Ino:     bpInfo.InodeNumber,
						Off:     curOffset,
						NameLen: uint32(len(bpBasename)),
						Type:    bpTreeDirEntType(bpInfo.InodeType),
						Name:    []byte(bpBasename),
					},
				}
				if backend != nil {
					dirEntPlus.EntryOut.Attr.UID = uint32(backend.uid)
					dirEntPlus.EntryOut.Attr.GID = uint32(backend.gid)
				}
				curDirEntPlusSize := fission.DirEntPlusFixedPortionSize + uint64(dirEntPlus.DirEnt.NameLen) + fission.DirEntAlignment - 1
				curDirEntPlusSize /= fission.DirEntAlignment
				curDirEntPlusSize *= fission.DirEntAlignment
				if curReadDirPlusOutSize+curDirEntPlusSize > uint64(readDirPlusIn.Size) {
					globalsUnlock()
					errno = 0
					return
				}
				readDirPlusOut.DirEntPlus = append(readDirPlusOut.DirEntPlus, dirEntPlus)
				curReadDirPlusOutSize += curDirEntPlusSize
			}
		}

		globalsUnlock()
		errno = 0
		return
	}

	// Try manifest serving (per-directory TSV, after B+Tree miss)
	if backend != nil && backend.manifestPath != "" && !fh.serveFromManifest && fh.manifestEntries == nil {
		partPath := manifestPartPath(backend.manifestPath, parentInode.objectPath)
		manifestEntries, manifestErr := readManifestPart(partPath)
		if manifestErr == nil && len(manifestEntries) > 0 {
			fh.serveFromManifest = true
			fh.manifestEntries = manifestEntries
			globals.logger.Printf("[TRACE] DoReadDirPlus: loaded manifest for inode %d (objectPath=%q, %d entries)",
				parentInode.inodeNumber, parentInode.objectPath, len(manifestEntries))
		}
	}

	if fh.serveFromManifest {
		globals.logger.Printf("[TRACE] DoReadDirPlus: serving from manifest for inode %d (objectPath=%q)",
			parentInode.inodeNumber, parentInode.objectPath)

		parentInodeVirtChildDirEntryMapStart, parentInodeVirtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(parentInode.inodeNumber)
		virtDotCount := parentInodeVirtChildDirEntryMapLimit - parentInodeVirtChildDirEntryMapStart
		totalManifestEntries := virtDotCount + uint64(len(fh.manifestEntries))

		for curOffset < totalManifestEntries {
			if curOffset < virtDotCount {
				childInodeBasename, childDirInfo, ok = globals.virtChildDirEntryMap.getByIndex(parentInodeVirtChildDirEntryMapStart + curOffset)
				if !ok {
					break
				}
				childInode, ok = globals.inodeMap.get(childDirInfo.InodeNumber)
				if !ok {
					break
				}
				curOffset++
				ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
				if !ok {
					globalsUnlock()
					errno = 0
					return
				}
			} else {
				mEntry := &fh.manifestEntries[curOffset-virtDotCount]
				if mEntry.Kind == "d" {
					childInode = parentInode.findChildDirInode(mEntry.Basename)
				} else {
					childInode = parentInode.findChildFileInode(mEntry.Basename, mEntry.ETag, mEntry.MTime, mEntry.Size)
				}
				childInode.convertToPhysInodeIfNecessary()
				childInodeBasename = childInode.basename
				curOffset++

				if !childInode.pendingDelete {
					ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
					if !ok {
						globalsUnlock()
						errno = 0
						return
					}
				}
			}
		}

		globalsUnlock()
		errno = 0
		return
	}

	globals.logger.Printf("[TRACE] DoReadDirPlus: serving from S3 for inode %d (objectPath=%q)", parentInode.inodeNumber, parentInode.objectPath)

	if fh.listDirectoryInProgress {
		globalsUnlock()
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

			globalsUnlock()

			listDirectoryOutput, err = listDirectoryWrapper(backend.context, listDirectoryInput)

			globalsLock("fission.go:2635:4:(*globalsStruct).DoReadDirPlus")

			fh.listDirectoryInProgress = false

			if err != nil {
				globalsUnlock()
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

			// Since we had to release the global lock (globalsUnlock) during listDirectoryWrapper() call, we must restart from where we first grabbed it

			goto Restart
		}

		// At this point, we know either we are still reading fh.{prev|next}ListDirectoryOutput's
		// or we are done with all of them and may proceed to return fh.listDirectorySubdirectoryList
		// & parentInode's virtChildDirEntryMap entries

		parentInodeVirtChildDirEntryMapStart, parentInodeVirtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(parentInode.inodeNumber)

		curOffsetInPrevListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset
		curOffsetInNextListDirectoryOutputCap = fh.nextListDirectoryOutputStartingOffset + fh.nextListDirectoryOutputFileLen
		curOffsetInListDirectorySubdirectoryListCap = curOffsetInNextListDirectoryOutputCap + uint64(len(fh.listDirectorySubdirectoryList))
		curOffsetInVirtChildInodeMapCap = curOffsetInListDirectorySubdirectoryListCap + (parentInodeVirtChildDirEntryMapLimit - parentInodeVirtChildDirEntryMapStart)

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
			virtChildDirEntryMapIndex = parentInodeVirtChildDirEntryMapStart + (curOffset - curOffsetInListDirectorySubdirectoryListCap)
			childInodeBasename, childDirInfo, ok = globals.virtChildDirEntryMap.getByIndex(virtChildDirEntryMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.getByIndex(virtChildDirEntryMapIndex) returned !ok")
			}
			childInode, ok = globals.inodeMap.get(childDirInfo.InodeNumber)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childDirInfo.InodeNumber) returned !ok [DoReadDirPlus() case 2]")
			}
		default:
			globalsUnlock()
			errno = 0
			return
		}

		curOffset++

		if !childInode.pendingDelete {
			ok = childInode.appendToReadDirPlusOut(uint64(readDirPlusIn.Size), readDirPlusOut, entryAttrValidSec, entryAttrValidNSec, curOffset, childInodeBasename, &curReadDirPlusOutSize)
			if !ok {
				globalsUnlock()
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
		globalsLock("fission.go:2772:3:funcLit@2770")
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
		globalsUnlock()
	}()

	globalsLock("fission.go:2791:2:(*globalsStruct).DoStatX")

	thisInode, ok = globals.inodeMap.get(inHeader.NodeID)
	if !ok {
		backend = nil
		globalsUnlock()
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
		globalsUnlock()
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

	globalsUnlock()

	errno = 0
	return
}
