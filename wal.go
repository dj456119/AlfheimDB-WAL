/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:24:19
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-26 15:14:05
 */
package alfheimdbwal

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	wal.MaxItems = 1000
	wal.IsBigEndian = true
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
	matchCount := 0
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "log") {
			logrus.Info("No match file name: ", file.Name())
			continue
		}
		matchCount++
		go GoFuncNewAlfheimDBWALFile(filepath.Join(wal.Dirname, file.Name()), sList, fileMap, aFileChan)
	}

	for i := 0; i != matchCount; {
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

	if lItem == nil || len(data) == 0 {
		logrus.Warn("Empty logs written.")
		return
	}
	if wal.FileIndex.Len() == 0 || wal.FileIndex.Back().Value.(*AlfheimDBWALFile).LogIndex.Len() >= int(wal.MaxItems) {
		aFile := wal.CreateNewFile(lItem.Index)
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
	if len(lItems) == 0 || len(data) == 0 {
		logrus.Warn("Empty logs written.")
		return
	}
	firstIndex := lItems[0].Index
	if wal.FileIndex.Len() == 0 || wal.FileIndex.Back().Value.(*AlfheimDBWALFile).LogIndex.Len() >= int(wal.MaxItems) {
		aFile := wal.CreateNewFile(firstIndex)
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
		if index != elem.Key().(int64) {
			elem = elem.Prev()
		}

	}
	aFile := elem.Value.(*AlfheimDBWALFile)
	return aFile.ReadLog(index)
}

//log file name: log_${unixtimestamp}_index.dat
func (wal *AlfheimDBWAL) CreateNewFile(index int64) *AlfheimDBWALFile {
	fileName := fmt.Sprintf("log_%d_%d.dat", time.Now().Unix(), index)
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
	wal.MinIndex = -1
	wal.MaxIndex = 0
	for _, v := range wal.AFiles {
		wal.RefreshMinAndMaxIndex(v)
	}
	logrus.Info("The min log is:", wal.MinIndex, ", max log is:", wal.MaxIndex)
}

//truncate log, [start, end]
func (wal *AlfheimDBWAL) TruncateLog(start, end int64) {
	wal.Mutex.Lock()
	defer wal.Mutex.Unlock()

	RangeAlfheimDBWALFile(wal.FileIndex, start, end,
		func(key int64, aFile *AlfheimDBWALFile) bool {
			logrus.Info("Truncate file: ", aFile.Filename)
			//truncate logx
			flag := aFile.TruncateLog(start, end)
			switch flag {
			case NO_TRUNCATED:
				fallthrough
			case TRUNCATED_OK:
				fallthrough
			case REMOVE_FILE:
				// need remove
				if aFile.LogIndex.Len() == 0 || flag == REMOVE_FILE {
					aFile.Close()
					err := os.Remove(aFile.File.Name())
					if err != nil {
						logrus.Fatal("Remove file error, ", err)
					}
					logrus.Info("File remove: ", aFile.Filename)
					delete(wal.AFiles, aFile.MinIndex)
					return false
				}
			default:
				logrus.Fatal("Unknow truncate stat: ", flag)
			}
			return true
		})

	//refresh min and max index from all file
	wal.RefreshAllMinAndMaxIndex()
}

func RangeAlfheimDBWALFile(sList *skiplist.SkipList, startIndex, endIndex int64, exec func(key int64, value *AlfheimDBWALFile) bool) {
	if sList.Len() == 0 {
		return
	}

	if sList.Front().Key().(int64) > endIndex || sList.Back().Value.(*AlfheimDBWALFile).MaxIndex < startIndex {
		return
	}

	var firstAFileElem *skiplist.Element

	//If startIndex == aFile.minIndex, the startIndex must in this file
	firstAFileElem = sList.Find(startIndex)

	//If firstAFileElem == nil, have no elem's minIndex greater then startIndex, so elem must in last file
	if firstAFileElem == nil {
		firstAFileElem = sList.Back()
	} else {
		//If firstAFileElem != nil, compare firstAFileElem's minIndex and startIndex, if minIndex == startIndex, the startIndex must in firstAFileElem, if not, the startIndex in the Elem's prev file
		if firstAFileElem.Key().(int64) != startIndex {
			firstAFileElem = firstAFileElem.Prev()
			//If elem's prev is nil, startIndex must in first file
			if firstAFileElem == nil {
				firstAFileElem = sList.Front()
			}
		}
	}

	for {
		b := exec(firstAFileElem.Key().(int64), firstAFileElem.Value.(*AlfheimDBWALFile))
		next := firstAFileElem.Next()
		if !b {
			sList.Remove(firstAFileElem.Key())
		}
		if next == nil || next.Key().(int64) > endIndex {
			break
		}
		firstAFileElem = next
	}
}
