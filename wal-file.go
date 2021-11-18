/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:38:09
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-18 21:14:27
 */
package alfheimdbwal

import (
	"log"
	"os"
	"syscall"

	"github.com/sirupsen/logrus"
)

type AlfheimDBWALFile struct {
	File *os.File
	Pos  int64
}

func NewAlfheimDBWALFile(filename string) *AlfheimDBWALFile {
	aFile := new(AlfheimDBWALFile)
	var err error
	aFile.File, err = os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		logrus.Fatal("Open file error, ", err)
	}
	return aFile
}

func (aFile *AlfheimDBWALFile) SeekLog() {
	_, err := aFile.File.Seek(aFile.Pos, 0)
	if err != nil {
		logrus.Fatal("seek error, ", err)
	}

}

func (aFile *AlfheimDBWALFile) WriteLog(data []byte) error {
	length := 0
	for len(data) > length {
		l, err := aFile.File.Write(data)
		if err != nil {
			log.Fatal("Write file err, ", err)
		}
		length = l + length
	}
	syscall.Fsync(int(aFile.File.Fd()))
	//aFile.File.Sync()
	return nil
}
