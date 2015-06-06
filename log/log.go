// log is a thin wrapper around the Go standard global logger
package log

import (
	"fmt"
	"path/filepath"
	stdLog "log"
	"os"
	"runtime"
)

var outputDebug bool

func Init(file *os.File, debug bool) {
	stdLog.SetFlags(stdLog.LstdFlags)
	stdLog.SetOutput(file)
	outputDebug = debug
}

func getPrefix(level string, file string, line int) string {
	return fmt.Sprintf("%v:%v: [%v] ", filepath.Base(file), line, level)
}

func Println(v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("INFO", file, line) + fmt.Sprint(v...)
	stdLog.Println(s)
}

func Printf(format string, v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("INFO", file, line) + fmt.Sprintf(format, v...)
	stdLog.Print(s)
}

func Debugln(v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("DEBUG", file, line) + fmt.Sprint(v...)
	stdLog.Println(s)
}

func Debugf(format string, v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("DEBUG", file, line) + fmt.Sprintf(format, v...)
	stdLog.Print(s)
}

func Warnln(v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("WARN", file, line) + fmt.Sprint(v...)
	stdLog.Println(s)
}

func Warnf(format string, v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("WARN", file, line) + fmt.Sprintf(format, v...)
	stdLog.Print(s)
}

func Errorln(v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("ERROR", file, line) + fmt.Sprint(v...)
	stdLog.Println(s)
}

func Errorf(format string, v ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	s := getPrefix("ERROR", file, line) + fmt.Sprintf(format, v...)
	stdLog.Print(s)
}
