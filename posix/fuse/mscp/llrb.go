package main

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/sortedmap"
)

// `stringSetStruct` defines a struct able to support string set operations
// (e.g. "is string in set?", "place string in set", and "index to Nth string
// in set") utilizing the sortedmap.LLRBTree functionality.
type stringSetStruct struct {
	desc string
	llrb sortedmap.LLRBTree
}

// `newStringSet` creates a stringSet with the requested description.
func newStringSet(desc string) (stringSet *stringSetStruct) {
	stringSet = &stringSetStruct{}
	stringSet.desc = desc
	stringSet.llrb = sortedmap.NewLLRBTree(sortedmap.CompareString, stringSet)

	return
}

// `GetByIndex` retrieves the string at the requested index of stringSet.
func (stringSet *stringSetStruct) GetByIndex(index int) (keyAsString string, ok bool) {
	keyAsKey, _, ok, err := stringSet.llrb.GetByIndex(index)
	if err != nil {
		globals.logger.Fatalf("stringSet.llrb.GetByIndex()) failed: %v", err)
	}
	if !ok {
		return
	}
	keyAsString, ok = keyAsKey.(string)
	if !ok {
		globals.logger.Fatalf("keyAsKey.(string) returned !ok")
	}
	return
}

// `IsSet` returns whether or not the string is in the stringSet.
func (stringSet *stringSetStruct) IsSet(keyAsString string) (isSet bool) {
	_, isSet, err := stringSet.llrb.GetByKey(keyAsString)
	if err != nil {
		globals.logger.Fatalf("stringSet.llrb.GetByKey() failed: %v", err)
	}
	return
}

// `Set` ensures string is now in the stringSet and returns whether it was previously.
func (stringSet *stringSetStruct) Set(keyAsString string) (wasSet bool) {
	wasSet = stringSet.IsSet(keyAsString)
	if !wasSet {
		ok, err := stringSet.llrb.Put(keyAsString, struct{}{})
		if err != nil {
			globals.logger.Fatalf("stringSet.llrb.Put() failed: %v", err)
		}
		if !ok {
			globals.logger.Fatalf("stringSet.llrb.Put() returned !ok")
		}
	}
	return
}

// `Clr` ensures string is now not in the stringSet and returns whether it was previously.
func (stringSet *stringSetStruct) Clr(keyAsString string) (wasSet bool) {
	wasSet = stringSet.IsSet(keyAsString)
	if wasSet {
		ok, err := stringSet.llrb.DeleteByKey(keyAsString)
		if err != nil {
			globals.logger.Fatalf("stringSet.llrb.DeleteByKey() failed: %v", err)
		}
		if !ok {
			globals.logger.Fatalf("stringSet.llrb.DeleteByKey() returned !ok")
		}
	}
	return
}

// `Len` returns how many strings are currently in stringSet.
func (stringSet *stringSetStruct) Len() (numberOfItems int) {
	numberOfItems, err := stringSet.llrb.Len()
	if err != nil {
		globals.logger.Fatalf("stringSet.llrb.Len() failed: %v", err)
	}
	return
}

// `DumpKey` is a callback to format the string in stringSet as a string.
func (stringSet *stringSetStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString, ok := key.(string)
	if ok {
		err = nil
	} else {
		err = errors.New("key.(string) returned !ok")
	}
	return
}

// `DumpValue` is a callback to format the empty struct value in stringSet as a string indicating the set element is set.
func (stringSet *stringSetStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsString = "IsSet"
	err = nil
	return
}

// `stringToUint64MapStruct` defines a struct able to support string to uint64 operations
// (e.g. "does string have a value?", "what is the value for this string?", "assign this
// value for this string?", and "index to Nth string and return it along with its value")
// utilizing the sortedmap.LLRBTree functionality.
type stringToUint64MapStruct struct {
	desc string
	llrb sortedmap.LLRBTree
}

// `newStringToUint64Map` creates a stringToUint64Map with the requested description.
func newStringToUint64Map(desc string) (stringToUint64Map *stringToUint64MapStruct) {
	stringToUint64Map = &stringToUint64MapStruct{}
	stringToUint64Map.desc = desc
	stringToUint64Map.llrb = sortedmap.NewLLRBTree(sortedmap.CompareString, stringToUint64Map)

	return
}

// `DeleteByKey` removes the string:uint64 element from stringToUint64Map.
func (stringToUint64Map *stringToUint64MapStruct) DeleteByKey(keyAsString string) (ok bool) {
	ok, err := stringToUint64Map.llrb.DeleteByKey(keyAsString)
	if err != nil {
		globals.logger.Fatalf("stringToUint64Map.llrb.DeleteByKey(keyAsString) failed: %v", err)
	}
	return
}

// `GetByIndex` retrieves the string key at the requested index of stringToUint64Map.
func (stringToUint64Map *stringToUint64MapStruct) GetByIndex(index int) (keyAsString string, valueAsUint64 uint64, ok bool) {
	keyAsKey, valueAsValue, ok, err := stringToUint64Map.llrb.GetByIndex(index)
	if err != nil {
		globals.logger.Fatalf("stringToUint64Map.llrb.GetByIndex(index) failed: %v", err)
	}
	if !ok {
		return
	}
	keyAsString, ok = keyAsKey.(string)
	if !ok {
		globals.logger.Fatalf("keyAsKey.(string) returned !ok")
	}
	valueAsUint64, ok = valueAsValue.(uint64)
	if !ok {
		globals.logger.Fatalf("valueAsValue.(uint64) returned !ok")
	}
	return
}

// `GetByKey` returns the uint64 value corresponding to the string key of stringToUint64Map.
func (stringToUint64Map *stringToUint64MapStruct) GetByKey(keyAsString string) (valueAsUint64 uint64, ok bool) {
	valueAsValue, ok, err := stringToUint64Map.llrb.GetByKey(keyAsString)
	if err != nil {
		globals.logger.Fatalf("stringToUint64Map.llrb.GetByKey(keyAsString) failed: %v", err)
	}
	if !ok {
		return
	}
	valueAsUint64, ok = valueAsValue.(uint64)
	if !ok {
		globals.logger.Fatalf("valueAsValue.(uint64) returned !ok")
	}
	return
}

// `Len` returns how many string:uint64 elements are in stringToUint64Map.
func (stringToUint64Map *stringToUint64MapStruct) Len() (numberOfItems int) {
	numberOfItems, err := stringToUint64Map.llrb.Len()
	if err != nil {
		globals.logger.Fatalf("stringToUint64Map.llrb.Len() failed: %v", err)
	}
	return
}

// `Put` sets the string's uint64 value in stringToUint64Map.
func (stringToUint64Map *stringToUint64MapStruct) Put(keyAsString string, valueAsUint64 uint64) (ok bool) {
	ok, err := stringToUint64Map.llrb.Put(keyAsString, valueAsUint64)
	if err != nil {
		globals.logger.Fatalf("stringToUint64Map.llrb.Put(keyAsString, valueAsUint64) failed: %v", err)
	}
	return
}

// `DumpKey` is a callback to format the string key in stringToUint64Map as a string.
func (*stringToUint64MapStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString, ok := key.(string)
	if ok {
		err = nil
	} else {
		err = errors.New("key.(string) returned !ok")
	}
	return
}

// `DumpValue` is a callback to format the uint64 value in stringToUint64Map as a string.
func (*stringToUint64MapStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsUint64, ok := value.(uint64)
	if !ok {
		err = errors.New("value.(uint64) returned !ok")
		return
	}
	valueAsString = fmt.Sprintf("%08X", valueAsUint64)
	err = nil
	return
}
