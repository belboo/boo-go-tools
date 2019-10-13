package misctools

import (
	"fmt"
)

// TErrorCode is a TError codes ENUM
type TErrorCode string

// TErrorCode ENUM values
const (
	ErrOk				TErrorCode = ""
	ErrConfigError		TErrorCode = "job configuration error"
	ErrTableEmpty		TErrorCode = "table is empty"
	ErrTableNotFound	TErrorCode = "table not found"
	ErrCreateDataset 	TErrorCode = "error creating dataset"
	ErrTableNotPartitioned	TErrorCode = "table not partitioned"
	ErrCreateTable	 	TErrorCode = "error creating table"
	ErrDeleteDataset 	TErrorCode = "error deleting dataset"
	ErrDeleteTable	 	TErrorCode = "error deleting table"
	ErrGetDatasetMeta 	TErrorCode = "error fetching dataset meta"
	ErrGetTableMeta	 	TErrorCode = "error fetching table meta"
	ErrMetaMismatch	 	TErrorCode = "source and destination metas do not match"
	ErrDataQuery	 	TErrorCode = "data query failed"
	ErrDataUpload	 	TErrorCode = "data upload failed"
	ErrLoadGCS		 	TErrorCode = "error loading from GCS"
	ErrAPI 				TErrorCode = "generic BQ API error"
	ErrParse 			TErrorCode = "parse error"
	ErrSameDstSrc		TErrorCode = "destination and source match"
	ErrGeneric			TErrorCode = "somthing has gone south"
	ErrGCS				TErrorCode = "GCS related error"
)

// TError is a dummy type for custom error
type TError struct {
	Msg 	string
	Origin 	string
	Code 	TErrorCode
	Err 	error
}

// Error implemented to compy with error interface
func (e TError) Error() string {
	errMsg := fmt.Sprintf("Error: %v", e.Msg)
	if e.Origin != "" {
		errMsg += fmt.Sprintf(" in %v", e.Origin)
	}
	if e.Code != ErrOk {
		errMsg += fmt.Sprintf(" (Code: %v)", e.Code)
	}
	if e.Err != nil {
		errMsg += fmt.Sprintf("\n (Error: %v)", e.Err.Error())
	}
	return errMsg
}

// WithMsg returns a TError copy with new message
func (e TError) WithMsg(msg string) TError {
	e.Msg = msg
	return e
}

// WithOrigin returns a TError copy with new Origin
func (e TError) WithOrigin(origin string) TError {
	e.Origin = origin
	return e
}

// WithCode returns a TError copy with new Code
func (e TError) WithCode(code TErrorCode) TError {
	e.Code = code
	return e
}

// WithError returns a TError copy with new Code
func (e TError) WithError(err error) TError {
	e.Err = err
	return e
}

// WithFormatMsg returns a TError copy with new formatted message
func (e TError) WithFormatMsg(fmtstr string, args ...interface{}) TError {
	e.Msg = fmt.Sprintf(fmtstr, args...)
	return e
}