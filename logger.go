package gocql

import (
	"bytes"
	"fmt"
	"log"
	"sync"
)

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type testLogger struct {
	mu      sync.Mutex
	capture bytes.Buffer
}

func (l *testLogger) Print(v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprint(&l.capture, v...)
}
func (l *testLogger) Printf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.capture, format, v...)
}
func (l *testLogger) Println(v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintln(&l.capture, v...)
}
func (l *testLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capture.String()
}

type defaultLogger struct{}

func (l *defaultLogger) Print(v ...interface{})                 { log.Print(v...) }
func (l *defaultLogger) Printf(format string, v ...interface{}) { log.Printf(format, v...) }
func (l *defaultLogger) Println(v ...interface{})               { log.Println(v...) }

// Logger for logging messages.
// Deprecated: Use ClusterConfig.Logger instead.
var Logger StdLogger = &defaultLogger{}
