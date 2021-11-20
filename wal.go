/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:24:19
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-21 01:14:47
 */
package alfheimdbwal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
)

type AlfheimDBWAL struct {
	FileIndex   *skiplist.SkipList
	AFiles      map[int64]*AlfheimDBWALFile
	MinIndex    int64
	MaxIndex    int64
	MaxItems    int64
	Dirname     string
	IsBigEndian bool
	Mutex       *sync.Mutex
}

func NewWAL(waldir string) *AlfheimDBWAL {
	wal := new(AlfheimDBWAL)
	wal.Dirname = waldir
	wal.Mutex = new(sync.Mutex)
	wal.BuildDirIndex()
	return wal
}

func (wal *AlfheimDBWAL) BuildDirIndex() {
	sList := skiplist.New(skiplist.Int64)
	fileMap := make(map[int64]*AlfheimDBWALFile)
	files, err := ioutil.ReadDir(wal.Dirname)
	if err != nil {
		logrus.Fatal("Read wal dir error, ", err)
	}

	aFileChan := make(chan *AlfheimDBWALFile, len(files))
	for _, file := range files {
		go GoFuncNewAlfheimDBWALFile(file.Name(), sList, fileMap, aFileChan)
		i := 0
		for i != len(files) {
			aFile := <-aFileChan
			sList.Set(aFile.MinIndex, aFile)
			fileMap[aFile.MinIndex] = aFile
		}
	}

	wal.Mutex.Lock()
	wal.MinIndex = -1
	wal.MaxIndex = 0
	wal.FileIndex = sList
	wal.AFiles = fileMap
	for _, v := range wal.AFiles {
		wal.RefreshMinAndMaxIndex(v)
	}
	wal.Mutex.Unlock()
	return
}

func GoFuncNewAlfheimDBWALFile(filename string, sList *skiplist.SkipList, fileMap map[int64]*AlfheimDBWALFile, aFileChan chan *AlfheimDBWALFile) {
	aFile := NewAlfheimDBWALFile(filename)
	aFileChan <- aFile
}

func (wal *AlfheimDBWAL) WriteLog(lItem *LogItem, data []byte) {
	wal.Mutex.Lock()
	defer wal.Mutex.Unlock()
	if wal.FileIndex.Len() == 0 || wal.FileIndex.Back().Value.(*AlfheimDBWALFile).LogIndex.Len() >= int(wal.MaxItems) {
		aFile := wal.CreateNewFile()
		aFile.WriteLog(lItem, data)
		wal.FileIndex.Set(aFile.MinIndex, aFile)
		wal.AFiles[aFile.MinIndex] = aFile
		wal.RefreshMinAndMaxIndex(aFile)
		return
	}

	elem := wal.FileIndex.Back()
	aFile := elem.Value.(*AlfheimDBWALFile)
	aFile.WriteLog(lItem, data)
	wal.RefreshMinAndMaxIndex(aFile)
	return
}

func (wal *AlfheimDBWAL) BatchWriteLog(lItems []*LogItem, data []byte) {
	wal.Mutex.Lock()
	defer wal.Mutex.Unlock()
	if wal.FileIndex.Len() == 0 || wal.FileIndex.Back().Value.(*AlfheimDBWALFile).LogIndex.Len() >= int(wal.MaxItems) {
		aFile := wal.CreateNewFile()
		aFile.BatchWriteLogs(lItems, data)
		wal.FileIndex.Set(aFile.MinIndex, aFile)
		wal.AFiles[aFile.MinIndex] = aFile
		wal.RefreshMinAndMaxIndex(aFile)
		return
	}

	elem := wal.FileIndex.Back()
	aFile := elem.Value.(*AlfheimDBWALFile)
	aFile.BatchWriteLogs(lItems, data)
	wal.RefreshMinAndMaxIndex(aFile)
	return
}

func (wal *AlfheimDBWAL) GetLog(index int64) []byte {
	wal.Mutex.Lock()
	defer wal.Mutex.Unlock()
	if wal.FileIndex.Len() == 0 {
		return nil
	}
	if index < wal.MinIndex || index > wal.MaxIndex {
		return nil
	}
	elem := wal.FileIndex.Find(index)
	if elem == nil {
		elem = wal.FileIndex.Back()
	} else {
		elem = elem.Prev()
	}
	aFile := elem.Value.(*AlfheimDBWALFile)
	return aFile.ReadLog(index)
}

func (wal *AlfheimDBWAL) CreateNewFile() *AlfheimDBWALFile {
	fileName := fmt.Sprintf("log_%d.dat", time.Now().Unix())
	fullName := filepath.Join(wal.Dirname, fileName)
	return NewAlfheimDBWALFile(fullName)
}

func (wal *AlfheimDBWAL) RefreshMinAndMaxIndex(aFile *AlfheimDBWALFile) {
	if wal.MinIndex == -1 {
		wal.MinIndex = aFile.MinIndex
	} else if aFile.MinIndex < wal.MinIndex {
		wal.MinIndex = aFile.MinIndex
	}
	if aFile.MaxIndex > wal.MaxIndex {
		wal.MaxIndex = aFile.MaxIndex
	}
}

func (wal *AlfheimDBWAL) RefreshAllMinAndMaxIndex() {
	for _, v := range wal.AFiles {
		wal.RefreshMinAndMaxIndex(v)
	}
}
func (wal *AlfheimDBWAL) TruncateLog(start, end int64) {
	wal.Mutex.Lock()
	defer wal.Mutex.Unlock()
	if wal.FileIndex.Len() == 0 {
		return
	}
	for {
		elem := wal.FileIndex.Find(start)
		if elem == nil {
			break
		}
		aFile := elem.Value.(*AlfheimDBWALFile)
		index := elem.Value.(int64)
		switch aFile.TruncateLog(start, end) {
		case NO_TRUNCATED | TRUNCATED_OK:
			if aFile.LogIndex.Len() == 0 {
				aFile.Close()
				err := os.Remove(aFile.File.Name())
				if err != nil {
					logrus.Fatal("Remove file error, ", err)
				}
				wal.FileIndex.Remove(index)
				delete(wal.AFiles, index)
			}
		case REMOVE_FILE:
			aFile.Close()
			err := os.Remove(aFile.File.Name())
			if err != nil {
				logrus.Fatal("Remove file error, ", err)
			}
			wal.FileIndex.Remove(index)
			delete(wal.AFiles, index)
		default:
			logrus.Fatal("Unknow truncate stat")
		}
		start = index
	}
	wal.RefreshAllMinAndMaxIndex()
}
