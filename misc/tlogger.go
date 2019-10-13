package misctools

import (
	"fmt"
)

// TLogParam is a dummy type TLogger parameters
type TLogParam int
// TLogger parameters ENUM
const (
	LogLocal 	TLogParam = 0
	LogRemote 	TLogParam = 1

	LogNone 	TLogParam = 10
	LogNormal 	TLogParam = 11
	LogVerbose	TLogParam = 12
)

// TLogger is a dummy interface for a logger and STDIO
type TLogger struct {
	LogType		TLogParam	
	LogLevel	TLogParam
}

// Log logs or prints a normal info message
func (tl *TLogger) Log(msg interface{}, endl ...bool) {
	newLine := true
	if len(endl) > 0 {
		newLine = false
	}

	switch tl.LogType {
		case LogLocal:
			if newLine {
				fmt.Println(msg)
			} else {
				fmt.Print(msg)
			}
		default:
			if newLine {
				fmt.Println(msg)
			} else {
				fmt.Print(msg)
			}
	} 
}

// Logf is a formatter interface to Log
func (tl *TLogger) Logf(fmtString string, fmtArgs ...interface{}) {
	tl.Log(fmt.Sprintf(fmtString, fmtArgs...), false)
}

// Err logs an error according to logger settings
func (tl *TLogger) Err(msg interface{}, endl ...bool) {
	newLine := true
	if len(endl) > 0 {
		newLine = false
	}

	switch tl.LogType {
		case LogLocal:
			if newLine {
				fmt.Printf("Error: %v\n", msg)
			} else {
				fmt.Printf("Error: %v", msg)
			}
		default:
			if newLine {
				fmt.Printf("Error: %v\n", msg)
			} else {
				fmt.Printf("Error: %v", msg)
			}
	}
}

// Errf is a formatter interface to Err
func (tl *TLogger) Errf(fmtString string, fmtArgs ...interface{}) {
	tl.Err(fmt.Sprintf(fmtString, fmtArgs...), false)
}