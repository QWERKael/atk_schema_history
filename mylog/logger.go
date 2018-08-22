package mylog

import (
	"github.com/sirupsen/logrus"
	"os"
	"log"
)

var Log *logrus.Logger

func InitLogger(fileName string) *os.File {
	Log = logrus.New()
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0666)
	if err == nil {
		Log.Out = file
	} else {
		log.Println("Failed to Log to file, using default stderr")
	}
	//Log.WithFields(logrus.Fields{
	//	"animal": "walrus",
	//	"size":   10,
	//}).Info("A group of walrus emerges from the ocean")
	return file
}

func CloseLogger(file *os.File) {
	file.Close()
}
