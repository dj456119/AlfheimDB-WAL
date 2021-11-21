/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:24:19
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-21 18:48:38
 */
package alfheimdbwal

import (
	"fmt"
	"io/ioutil"
	"log"
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
	wal.MaxItems = 1000000
	wal.Mutex = new(sync.Mutex)
	wal.BuildDirIndex()
	return wal
}

//Range dir build log file index
func (wal *AlfheimDBWAL) BuildDirIndex() {
	sList := skiplist.New(skiplist.Int64)
	fileMap := make(map[int64]*AlfheimDBWALFile)
	files, err := ioutil.ReadDir(wal.Dirname)
	if err != nil {
		logrus.Fatal("Read wal dir error, ", err)
	}

	aFileChan := make(chan *AlfheimDBWALFile)
	for _, file := range files {
		go GoFuncNewAlfheimDBWALFile(filepath.Join(wal.Dirname, file.Name()), sList, fileMap, aFileChan)
		i := 0
		for i != len(files) {
			aFile := <-aFileChan
			i++
			if aFile.LogIndex.Len() == 0 {
				logrus.Info("File is empty, remove: ", aFile.Filename)
				aFile.Close()
				err := os.Remove(aFile.Filename)
				if err != nil {
					log.Fatal("Init file remove error, ", err)
				}
				continue
			}
			sList.Set(aFile.MinIndex, aFile)
			fileMap[aFile.MinIndex] = aFile
		}
	}

	wal.Mutex.Lock()
	wal.MinIndex = -1
	wal.MaxIndex = 0
	wal.FileIndex = sList
	wal.AFiles = fileMap
	wal.RefreshAllMinAndMaxIndex()
	wal.Mutex.Unlock()
	return
}

func GoFuncNewAlfheimDBWALFile(filename string, sList *skiplist.SkipList, fileMap map[int64]*AlfheimDBWALFile, aFileChan chan *AlfheimDBWALFile) {
	aFile := NewAlfheimDBWALFile(filename)
	aFileChan <- aFile
}

//write single log
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

//batch write log
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

//Time complexity:
//Find file in skipList , if i = len(files) => T(i) = O(logi)
//Read log in file, T(j) = O(1)
//T(i,j) = O(logi) + O(1) = O(logi)
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

//log file name: log_${unixtimestamp}.dat
func (wal *AlfheimDBWAL) CreateNewFile() *AlfheimDBWALFile {
	fileName := fmt.Sprintf("log_%d.dat", time.Now().Unix())
	fullName := filepath.Join(wal.Dirname, fileName)
	return NewAlfheimDBWALFile(fullName)
}

//refresh min and max index
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

//refresh min and max index from all file
func (wal *AlfheimDBWAL) RefreshAllMinAndMaxIndex() {
	for _, v := range wal.AFiles {
		wal.RefreshMinAndMaxIndex(v)
	}
	logrus.Info("The min log is:", wal.MinIndex, ", max log is:", wal.MaxIndex)
}

//truncate log, [start, end]
func (wal *AlfheimDBWAL) TruncateLog(start, end int64) {
	wal.Mutex.Lock()
	defer wal.Mutex.Unlock()
	if wal.FileIndex.Len() == 0 {
		return
	}
	tempStart := start
	for {
		//Get afile from skiplist
		elem := wal.FileIndex.Find(tempStart)
		if elem == nil {
			break
		}

		aFile := elem.Value.(*AlfheimDBWALFile)
		index := elem.Value.(int64)
		//index out of range [start, end]
		if index > end {
			break
		}

		//truncate log
		switch aFile.TruncateLog(start, end) {
		case NO_TRUNCATED | TRUNCATED_OK:
			// remove file
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
			//remove file
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
		start = tempStart
	}
	//refresh min and max index from all file
	wal.RefreshAllMinAndMaxIndex()
}
