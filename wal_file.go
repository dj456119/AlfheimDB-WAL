/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:38:09
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-20 00:08:34
 */
package alfheimdbwal

import (
	"log"
	"os"
	"sync"
	"syscall"

	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
)

type AlfheimDBWALFile struct {
	Mutex    *sync.Mutex
	File     *os.File
	Pos      int64
	LogItems map[int64]*LogItem
	LogIndex *skiplist.SkipList
	MaxIndex int64
	MinIndex int64
	Filename string
}

func NewAlfheimDBWALFile(filename string) *AlfheimDBWALFile {
	aFile := new(AlfheimDBWALFile)
	aFile.MinIndex = -1
	var err error
	aFile.Filename = filename
	aFile.File, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		logrus.Fatal("Open file error, ", err)
	}
	logrus.Info("Init wal file, ", filename)
	skiplist.New(skiplist.Int64)
	aFile.Mutex = new(sync.Mutex)
	aFile.BuildLogIndex()
	return aFile
}

func (aFile *AlfheimDBWALFile) ReadLog(index int64) []byte {
	if index > aFile.MaxIndex {
		return nil
	}
	if index < aFile.MinIndex {
		return nil
	}
	if lItem, ok := aFile.LogItems[index]; ok {
		buff := make([]byte, lItem.Length)
		aFile.File.Seek(int64(lItem.Pos), 0)
		count := 0
		for count != int(lItem.Length) {
			tempBuf := buff[count:]
			n, err := aFile.File.Read(tempBuf)
			if err != nil {
				logrus.Fatal("Read file error, ", err)
			}
			count = count + n
		}
		return buff
	}
	return nil
}

func (aFile *AlfheimDBWALFile) TruncateLog(start, end int64) error {
	if aFile.MaxIndex < start {
		return nil
	}
	if aFile.MinIndex > end {
		return nil
	}
	if aFile.MaxIndex < end && aFile.MinIndex <= start {
		err := aFile.File.Close()
		if err != nil {
			log.Fatal("TruncateLog error, ", err)
		}
		err = os.Remove(aFile.Filename)
		if err != nil {
			log.Fatal("TruncateLog error, ", err)
		}
	}
	lItem := aFile.LogIndex.Find(start)
	if lItem == nil {
		logrus.Fatal("TruncateLog error, can't find start log")
	}
	truncateLogPos := lItem.Value.(*LogItem).Pos - 8 - 8
	err := aFile.File.Truncate(int64(truncateLogPos))
	if err != nil {
		log.Fatal("TruncateLog error, ", truncateLogPos, err)
	}
	aFile.BuildLogIndex()
	return nil
}

func WriteFile(file os.File, pos int64, data []byte) error {
	_, err := file.Seek(pos, 0)
	if err != nil {
		logrus.Fatal("Seek file error", err)
	}
	length := 0
	for {
		l, err := file.Write(data)
		if err != nil {
			log.Fatal("Write file err, ", err)
		}
		length = l + length
		if length == len(data) {
			break
		}
		data = data[length:]
	}
	err = syscall.Fsync(int(file.Fd()))
	if err != nil {
		logrus.Fatal("Sync disk error, ", err)
	}
	return nil
}

func (aFile *AlfheimDBWALFile) WriteLog(lItem *LogItem, data []byte) error {
	WriteFile(*aFile.File, aFile.Pos, data)
	lItem.Pos = uint64(aFile.Pos) + 8 + 8
	aFile.Pos = int64(lItem.Pos) + int64(lItem.Length)
	aFile.LogIndex.Set(lItem.Index, lItem)
	aFile.LogItems[lItem.Index] = lItem
	if lItem.Index > aFile.MaxIndex {
		aFile.MaxIndex = lItem.Index
	}
	if lItem.Index < aFile.MinIndex {
		aFile.MinIndex = lItem.Index
	}
	return nil
}

func (aFile *AlfheimDBWALFile) BatchWriteLogs(lItems []*LogItem, data []byte) error {
	WriteFile(*aFile.File, aFile.Pos, data)
	for _, lItem := range lItems {
		lItem.Pos = uint64(aFile.Pos) + 8 + 8
		aFile.Pos = int64(lItem.Pos) + int64(lItem.Length)
		aFile.LogIndex.Set(lItem.Index, lItem)
		aFile.LogItems[lItem.Index] = lItem
		if lItem.Index > aFile.MaxIndex {
			aFile.MaxIndex = lItem.Index
		}
		if lItem.Index < aFile.MinIndex {
			aFile.MinIndex = lItem.Index
		}
	}
	return nil
}

func (aFile *AlfheimDBWALFile) BuildLogIndex() {
	var pos, allLength int64
	logItems := make(map[int64]*LogItem)
	fileStat, err := aFile.File.Stat()

	if err != nil {
		logrus.Fatal("Read file stat error, ", err)
	}
	sList := skiplist.New(skiplist.Int64)

	buff := make([]byte, 16)
	indexCount := 0
	for allLength < fileStat.Size() {
		tempBuff := buff[0:]
		lItem := new(LogItem)
		_, err := aFile.File.Seek(pos, 0)
		if err != nil {
			logrus.Fatal("Seek file error, ", err)
		}
		readCount := 0

		for readCount != 16 {
			n, err := aFile.File.Read(buff)
			if err != nil {
				logrus.Fatal("Read file error, ", err)
			}
			if n == 0 || n == -1 {
				logrus.Fatal("File have dirty data")
			}
			readCount = readCount + n
		}
		lItem.Length = ReadInt64FromBuff(tempBuff, true)
		tempBuff = buff[8:]
		lItem.Index = int64(ReadInt64FromBuff(tempBuff, true))
		allLength = allLength + int64(lItem.Length) + 8 + 8
		lItem.Pos = uint64(pos) + 8 + 8
		logItems[lItem.Index] = lItem
		sList.Set(lItem.Index, lItem)
		pos = allLength
		indexCount++
		if lItem.Index > aFile.MaxIndex {
			aFile.MaxIndex = lItem.Index
		}
		if aFile.MinIndex == -1 {
			aFile.MinIndex = lItem.Index
		} else if lItem.Index < aFile.MinIndex {
			aFile.MinIndex = lItem.Index
		}
	}
	logrus.Info("All load log item count : ", indexCount)
	aFile.Pos = allLength
	aFile.LogItems = logItems
	aFile.LogIndex = sList
	return
}
