package misctools

import (
	"fmt"
	"time"
	"strings"
	r "reflect"
)

// GetIndexMap creates a name -> field_number map from a string slice header
func GetIndexMap(header []string) map[string]int {
	fieldMap := make(map[string]int, len(header))
	for i, field := range header {
		fieldMap[strings.ToLower(field)] = i
	}
	return fieldMap
}

// SplitArray splits the string by a given delimeter into a slice or returns nil for an empty string
func SplitArray(s string, delim string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, delim)
}

// GetUniqueDates makes a list of distinct dates in a slice of timestamps
func GetUniqueDates(input []time.Time) []time.Time {
	uniques := make([]time.Time, 0, len(input))
	uniquesMap := make(map[time.Time]bool)

	var d time.Time

	for _, t := range input {
		d = t.Truncate(24*time.Hour)
		if _, seen := uniquesMap[d]; !seen {
			uniquesMap[d] = true
			uniques = append(uniques, d)
		}
	}

	return uniques
}

// GetUniqueDatesMap makes a map distinct dates and their frequencies
func GetUniqueDatesMap(timelist []time.Time) map[time.Time]int {
	uniques := make(map[time.Time]int)
	
	var d time.Time

	for _, t := range timelist {
		d = t.Truncate(24*time.Hour)
		if _, seen := uniques[d]; !seen {
			uniques[d] = uniques[d] + 1
		} else {
			uniques[d] = 1
		}
	}

	return uniques
}

// DateSetUnion returns a slice with unique dates present in both input slices
func DateSetUnion(setA, setB []time.Time) []time.Time {
	union := make([]time.Time, 0)
	unionMap := make(map[time.Time]bool)
	setBMap := GetUniqueDatesMap(setB)

	for _, a := range setA {
		aDate := a.Truncate(24*time.Hour)
		if _, inB := setBMap[aDate]; inB {
			if _, seen := unionMap[aDate]; !seen {
				union = append(union, aDate)
				unionMap[aDate] = true
			}
		}
	}

	return union
}

// DateSetDiff returns a difference of input date sets Diff(A,B) = A - B
func DateSetDiff(setA, setB []time.Time) []time.Time {
	diff := make([]time.Time, 0)
	diffMap := make(map[time.Time]bool)
	setBMap := GetUniqueDatesMap(setB)

	for _, a := range setA {
		aDate := a.Truncate(24*time.Hour)
		if _, inB := setBMap[aDate]; !inB {
			if _, seen := diffMap[aDate]; !seen {
				diff = append(diff, aDate)
				diffMap[aDate] = true
			}
		}
	}

	return diff
}


// GetMap makes a frequency map of a data slice
func GetMap(data interface{}) (interface{}, error) {
	vals := r.ValueOf(data)
	if vals.Kind() != r.Slice || vals.Len() == 0 {
		return nil, TError{
			Msg:    "data is not a slice or has zero length",
			Origin: "GetMap",
			Code:   ErrGeneric,
			Err:    nil,
		}
	}

	freqMap := r.MakeMap(r.MapOf(r.TypeOf(data).Elem(), r.TypeOf(1)))
	
	for i := 0; i < vals.Len(); i++ {
		if freqMap.MapIndex(vals.Index(i)).IsValid() {
			freqMap.SetMapIndex(vals.Index(i), r.ValueOf(freqMap.MapIndex(vals.Index(i)).Interface().(int) + 1))
		} else {
			freqMap.SetMapIndex(vals.Index(i), r.ValueOf(1))
		}
	}

	return freqMap.Interface(), nil
}

// Unique returns an interface to a slice of unique values
func Unique(data interface{}) (interface{}, error) {
	vals := r.ValueOf(data)
	
	if vals.Kind() != r.Slice || vals.Len() == 0 {
		return nil, TError{
			Msg:    "data is not a slice or has zero length",
			Origin: "Unique",
			Code:   ErrGeneric,
			Err:    nil,
		}
	}

	freqMap, err := GetMap(data)
	if err != nil {
		return nil, TError{
			Msg:    "failed to get a map of unique values",
			Origin: "Unique",
			Code:   ErrGeneric,
			Err:    nil,
		}
	}
	freqMapR := r.ValueOf(freqMap)
	uniques := r.MakeSlice(r.TypeOf(data), freqMapR.Len(), freqMapR.Len())
	for i, elem := range freqMapR.MapKeys() {
		uniques.Index(i).Set(elem) 
	}

	return uniques.Interface(), nil
}

// GetFieldSlice returns a slice of time values of a field in a slice of structs
func GetFieldSlice(data interface{}, field string, transform func(interface{}) (interface{}, error)) (interface{}, error) {
	
	var err error
	var tmp interface{}
	var outType r.Type

	vals := r.ValueOf(data)
	
	if vals.Kind() != r.Slice || vals.Len() == 0 {
		return nil, TError{
			Msg:    "data is not a slice or has zero length",
			Origin: "GetFieldSlice",
			Code:   ErrGeneric,
			Err:    nil,
		}
	}

	if !vals.Index(0).FieldByName(field).IsValid(){
		return nil, TError{
			Msg:    fmt.Sprintf("custom transformation failed, field [%v] not found", field),
			Origin: "GetFieldSlice",
			Code:   ErrGeneric,
			Err:    err,
		}
	}

	if transform != nil {
		tmp, err = transform(vals.Index(0).FieldByName(field).Interface())
		if err != nil {
			return nil, TError{
				Msg:    "custom transformation failed",
				Origin: "GetFieldSlice",
				Code:   ErrGeneric,
				Err:    err,
			}
		}
		outType = r.TypeOf(tmp)
	} else {
		outType = vals.Index(0).FieldByName(field).Type()
	}

	outSlice := r.MakeSlice(r.SliceOf(outType), vals.Len(), vals.Len())
	// vals := r.MakeSlice(vals.Index(0).FieldByName(field).Type(), vals.Len(), vals.Len())

	for i := 0; i < vals.Len(); i++ {
		if transform != nil {
			tmpOut, err := transform(vals.Index(i).FieldByName(field).Interface())
			if err != nil {
				return nil, TError{
					Msg:    "custom transformation failed",
					Origin: "GetFieldSlice",
					Code:   ErrGeneric,
					Err:    err,
				}
			}
			outSlice.Index(i).Set(r.ValueOf(tmpOut))
		} else {
			// vals.Index(i).Set(vals.Index(i).FieldByName(field))
			outSlice.Index(i).Set(vals.Index(i).FieldByName(field))
		}
	}

	return outSlice.Interface(), nil
}


// FilterByDates returns a slice of unique values
func FilterByDates(data interface{}, field string, dates []time.Time, toTime func(interface{}) (time.Time, error)) (interface{}, error) {
	
	vals := r.ValueOf(data)
	
	if vals.Kind() != r.Slice || vals.Len() == 0 {
		return nil, TError{
			Msg:    "data is not a slice or has zero length",
			Origin: "FilterDates",
			Code:   ErrGeneric,
			Err:    nil,
		}
	}

	dateMap := GetUniqueDatesMap(dates)
	outData := r.MakeSlice(r.TypeOf(data), 0, int(vals.Len()/10))
	var t time.Time
	var err error
	var ok bool

	for i := 0; i < vals.Len(); i++ {
		if toTime != nil {
			t, err = toTime(vals.Index(i).FieldByName(field).Interface())
			if err != nil {
				return nil, TError{
					Msg:    fmt.Sprintf("could not cast data[%v].%v to timestamp with custom function", i, field),
					Origin: "FilterDates",
					Code:   ErrGeneric,
					Err:    nil,
				}
			}
		} else {
			t, ok = vals.Index(i).FieldByName(field).Interface().(time.Time)
			if !ok {
				return nil, TError{
					Msg:    fmt.Sprintf("could not cast data[%v].%v to timestamp", i, field),
					Origin: "FilterDates",
					Code:   ErrGeneric,
					Err:    nil,
				}
			} 
		}
		
		if _, ok := dateMap[t.UTC().Truncate(24*time.Hour)]; ok {
			outData = r.Append(outData, vals.Index(i))
		}
	}

	return outData.Interface(), nil
}

// Desc prints the value given and its type
func Desc(i interface{}) {
	fmt.Printf("%T -> %v\n", i, i)
}