package plaitaexpr

import (
	"reflect"
	"slices"
	"sort"

	"github.com/spf13/cast"
)

func length(s []any) int {
	return len(s)
}

func index(slice []any, item any) int {
	for i, v := range slice {
		// comparable value
		if v == item {
			return i
		}
	}
	return -1
}

func slice(arr []any, start, end int) []any {
	arrLen := len(arr)
	subLen := end - start
	if subLen > arrLen || subLen < 0 || start >= arrLen || start < 0 {
		return nil
	}
	return arr[start:end]
}

func appendItem(slice []any, item any) []any {
	return append(slice, item)
}

func extend(slice1 []any, slice2 []any) []any {
	return append(slice1, slice2...)
}

func insert(slice []any, index int, item any) []any {
	// check index
	if index < 0 || index > len(slice) {
		return slice
	}
	return slices.Insert(slice, index, item)
}

func remove(slice []any, item any) []any {
	for i, v := range slice {
		if v == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func reverse(slice []any) []any {
	newSlice := make([]any, len(slice))
	copy(newSlice, slice)

	slices.Reverse(newSlice)
	return newSlice
}

func sortSlice(slice []any, args ...any) []any {
	var (
		desc    bool
		sortKey string
	)
	if len(args) > 0 {
		udfSortKey, ok := args[0].(string)
		if ok {
			sortKey = udfSortKey
		}
	}
	if len(args) > 1 {
		udfDesc, ok := args[1].(bool)
		if ok {
			desc = udfDesc
		}
	}
	newSlice := make([]any, len(slice))
	copy(newSlice, slice)

	sort.SliceStable(newSlice, func(i, j int) bool {
		aObj, bObj := newSlice[i], newSlice[j]
		return lessObj(aObj, bObj, desc, sortKey)
	})
	return newSlice
}

func lessObj(aObj any, bObj any, desc bool, sortKey string) bool {
	aValue := reflect.ValueOf(aObj)
	if aValue.Kind() == reflect.Ptr {
		aValue = aValue.Elem()
	}
	bValue := reflect.ValueOf(bObj)
	if bValue.Kind() == reflect.Ptr {
		bValue = bValue.Elem()
	}
	switch aValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		aInt := cast.ToInt64(aObj)
		bInt := cast.ToInt64(bObj)
		if desc {
			return aInt > bInt
		}
		return aInt < bInt
	case reflect.Float32, reflect.Float64:
		aFloat := cast.ToFloat64(aObj)
		bFloat := cast.ToFloat64(bObj)
		if desc {
			return aFloat > bFloat
		}
		return aFloat < bFloat
	case reflect.String:
		aStr := cast.ToString(aObj)
		bStr := cast.ToString(bObj)
		if desc {
			return aStr > bStr
		}
		return aStr < bStr
	case reflect.Struct:
		if sortKey == "" {
			return false
		}
		aSortField := aValue.FieldByName(sortKey)
		bSortField := bValue.FieldByName(sortKey)
		// only support single field(not nested)
		return lessObj(aSortField.Interface(), bSortField.Interface(), desc, "")
	case reflect.Map:
		if sortKey == "" {
			return false
		}
		aSortField := aValue.MapIndex(reflect.ValueOf(sortKey))
		bSortField := bValue.MapIndex(reflect.ValueOf(sortKey))
		// only support single field(not nested)
		return lessObj(aSortField.Interface(), bSortField.Interface(), desc, "")
	default:
		return false
	}
}

func getListItem(slice []any, index int) any {
	if index < 0 || index >= len(slice) {
		return nil
	}
	return slice[index]
}

func setListItem(slice []any, index int, item any) {
	if index < 0 || index >= len(slice) {
		return
	}
	slice[index] = item
}
