/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:24:19
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-20 00:20:55
 */
package alfheimdbwal

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
)

type AlfheimDBWAL struct {
	FileIndex *skiplist.SkipList
	AFiles    map[int64]*AlfheimDBWALFile
	MinIndex  int64
	MaxIndex  int64
	MaxItems  int64
	Dirname   string
}

func NewWAL(waldir string) *AlfheimDBWAL {
	wal := new(AlfheimDBWAL)
	wal.Dirname = waldir
	wal.MinIndex = -1
	sList := skiplist.New(skiplist.Int64)
	fileMap := make(map[int64]*AlfheimDBWALFile)
	files, err := ioutil.ReadDir(waldir)
	if err != nil {
		logrus.Fatal("Read wal dir error, ", err)
	}
	for _, file := range files {
		aFile := NewAlfheimDBWALFile(file.Name())
		sList.Set(aFile.MinIndex, aFile)
		fileMap[aFile.MinIndex] = aFile
		if aFile.MinIndex < wal.MinIndex {
			wal.MinIndex = aFile.MinIndex
		}
		if aFile.MaxIndex > wal.MaxIndex {
			wal.MaxIndex = aFile.MaxIndex
		}
	}
	return wal
}

func (wal *AlfheimDBWAL) WriteLog(lItem *LogItem, data []byte) {
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
	fileName := fmt.Sprintf("log.%d.dat", time.Now().Unix())
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
