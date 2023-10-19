/*
copyright 2023 Qizhi Huang(flaggyellow@qq.com)
*/

package src

import (
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	TOCONSOLE = 1 << iota
	TOLOGFILE
)

const (
	LEVEL_DEBUG = iota
	LEVEL_INFO
	LEVEL_WARNING
	LEVEL_ERROR
)

var LevelPrefixDict map[int]string = map[int]string{
	LEVEL_INFO:    "INFO: ",
	LEVEL_DEBUG:   "DEBUG: ",
	LEVEL_WARNING: "WARNING: ",
	LEVEL_ERROR:   "ERROR: ",
}

type Log struct {
	logDir        string
	logName       string
	fileWriter    *os.File
	fileLogger    *log.Logger
	consoleLogger *log.Logger
	errorLogger   *log.Logger
	debug         bool
}

var Logger *Log
var logger *Log

func init() {
	Logger = NewLogger()
	logger = Logger
}

func NewLogger() *Log {
	var err error
	tm := time.Unix(0, time.Now().UnixNano())
	timestr := tm.Format("2006-01-02_15-04-05")
	filename := "migrate_log_" + timestr + ".log"
	l := &Log{
		logDir:  "./logs",
		logName: filename,
		debug:   false,
	}
	logPath := filepath.Join(l.logDir, l.logName)

	err = os.MkdirAll(l.logDir, os.ModePerm)
	if err != nil {
		log.Fatalf("ERROR: failed to create log path: %v\n", err)
	}

	l.fileWriter, err = os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("ERROR: failed to open log file: %v\n", err)
	}

	l.fileLogger = log.New(l.fileWriter, "", log.LstdFlags)
	l.consoleLogger = log.New(os.Stdout, "", log.LstdFlags)
	l.errorLogger = log.New(os.Stderr, "\n", 0)

	return l
}

func (l *Log) SetDebug() {
	l.debug = true
}

func (l *Log) IsDebug() bool {
	return l.debug
}

func (l *Log) LogString(str string, target int, level int) {
	if level == LEVEL_DEBUG {
		if !l.debug {
			return
		}
	}
	prefix := LevelPrefixDict[level]
	if target&TOLOGFILE > 0 {
		l.fileLogger.Println(prefix, str)
	}
	if target&TOCONSOLE > 0 {
		if level < LEVEL_WARNING {
			l.consoleLogger.Println(str)
		} else if level >= LEVEL_WARNING {
			l.consoleLogger.Println(prefix, str)
		}
	}
}

func (l *Log) LogError(err error) {
	l.fileLogger.Println("ERROR: ", err.Error())
	l.errorLogger.Println("ERROR: ", err)
}

func (l *Log) Close() {
	l.fileLogger = nil
	l.fileWriter.Close()
	l.consoleLogger = nil
	l.errorLogger = nil
}
