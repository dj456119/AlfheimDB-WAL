/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:38:09
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-20 21:37:41
 */
package alfheimdbwal

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"syscall"

	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
)

// WAL file struct in storage:
// ┌───────────┬───────────┐
// │ 1M header │   logs    │
// └───────────┴───────────┘
// The file header struct:
// ┌───────────────┬─────────────────────────────┐
// │ Length 8Bytes │             Data            │
// └───────────────┴─────────────────────────────┘
// The log item struct:
// ┌───────────────┬──────────────┬─────────────────────────────────┐
// │ Length 8Bytes │ Index 8Bytes │              Data               │
// └───────────────┴──────────────┴─────────────────────────────────┘
type AlfheimDBWALFile struct {
	Mutex        *sync.Mutex
	File         *os.File
	Pos          int64
	LogItems     map[int64]*LogItem
	LogIndex     *skiplist.SkipList
	MaxIndex     int64
	MinIndex     int64
	Filename     string
	Header       *AlfheimDBWALFileHeader
	HeaderLength int64
}

type AlfheimDBWALFileHeader struct {
	TruncateArea []*TruncateArea `json:"truncate_area"`
}

//[start, end)
type TruncateArea struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

func NewAlfheimDBWALFile(filename string) *AlfheimDBWALFile {
	aFile := new(AlfheimDBWALFile)
	aFile.MinIndex = -1
	aFile.Filename = filename
	// 1M header
	aFile.HeaderLength = 1 << 20

	aFile.Mutex = new(sync.Mutex)
	aFile.BuildLogIndex()
	return aFile
}

func (aFile *AlfheimDBWALFile) LoadFileHeader() {
	header := new(AlfheimDBWALFileHeader)
	lengthBytes := make([]byte, 8)
	ReadFile(*aFile.File, 0, 8, lengthBytes)
	length := ReadInt64FromBuff(lengthBytes, true)
	buff := make([]byte, length)
	ReadFile(*aFile.File, 8, int64(length), buff)
	err := json.Unmarshal(buff, header)
	if err != nil {
		logrus.Fatal("Load file header error, ", err)
	}
	if header.TruncateArea == nil {
		header.TruncateArea = []*TruncateArea{}
	}
	aFile.Header = header
}

func (aFile *AlfheimDBWALFile) SaveFileHeader() {
	b, err := json.Marshal(aFile.Header)
	if err != nil {
		logrus.Fatal("Save file header error, ", err)
	}
	buff := make([]byte, len(b)+8)
	WriteInt64ToBuff(buff, int64(len(b)), true)
	copy(buff[8:], b)
	WriteFile(*aFile.File, 0, buff)
}

//true: ths pos is Truncated
func (aFile *AlfheimDBWALFile) FilterTruncated(pos int64) bool {
	for _, t := range aFile.Header.TruncateArea {
		if pos >= t.Start && pos < t.End {
			return true
		}
	}
	return false
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
		ReadFile(*aFile.File, int64(lItem.Pos), int64(lItem.Length), buff)
		return buff
	}
	return nil
}

const (
	NO_TRUNCATED = -1
	REMOVE_FILE  = 1
	TRUNCATED_OK = 0
)

func (aFile *AlfheimDBWALFile) TruncateLog(start, end int64) int {
	if aFile.LogIndex.Len() == 0 {
		return REMOVE_FILE
	}
	if aFile.MaxIndex < start {
		return NO_TRUNCATED
	}
	if aFile.MinIndex > end {
		return NO_TRUNCATED
	}

	// The log min index is 5, max index is 13
	// If start in (-,5] && end in [13,-)
	// Need truncate all log, so we remove this file
	// ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
	// │5│6│7│8│9│10│11│12│13│
	// └─┴─┴─┴─┴─┴──┴──┴──┴──┘
	if aFile.MaxIndex <= end && aFile.MinIndex >= start {
		return REMOVE_FILE
	}
	// The log min index is 5, max index is 13
	// If start in [5,13) && end in [13,-)
	// Need truncate from start to last log
	// ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
	// │5│6│7│8│9│10│11│12│13│
	// └─┴─┴─┴─┴─┴──┴──┴──┴──┘
	if aFile.MaxIndex <= end && aFile.MinIndex <= start {
		lItem := aFile.LogIndex.Find(start)
		if lItem == nil {
			return NO_TRUNCATED
		}
		truncateLogPos := lItem.Value.(*LogItem).Pos - 8 - 8
		err := aFile.File.Truncate(int64(truncateLogPos))
		if err != nil {
			log.Fatal("TruncateLog error, ", truncateLogPos, err)
		}
		aFile.Close()
		aFile.BuildLogIndex()
		return TRUNCATED_OK
	}

	// The log min index is 5, max index is 13
	// If start in (-,5] && end in [5,13]
	// Need truncate from the first log to end
	// Put these pos into TruncateArea
	// ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
	// │5│6│7│8│9│10│11│12│13│
	// └─┴─┴─┴─┴─┴──┴──┴──┴──┘
	if aFile.MaxIndex >= end && start <= aFile.MinIndex {
		elem := aFile.LogIndex.Find(end)
		if elem == nil {
			logrus.Fatal("TruncateLog error, ", start, end, aFile.MaxIndex, aFile.MinIndex)
		}
		lItem := elem.Value.(LogItem)
		ta := TruncateArea{Start: 0 + aFile.HeaderLength, End: int64(lItem.Pos) + int64(lItem.Length)}
		aFile.Header.TruncateArea = append(aFile.Header.TruncateArea, &ta)
		aFile.SaveFileHeader()
		aFile.Close()
		aFile.BuildLogIndex()
		return TRUNCATED_OK
	}

	// The log min index is 5, max index is 13
	// If start in [5,13] && end in [5,13]
	// Need truncate from start to end
	// ┌─┬─┬─┬─┬─┬──┬──┬──┬──┐
	// │5│6│7│8│9│10│11│12│13│
	// └─┴─┴─┴─┴─┴──┴──┴──┴──┘
	// Put these pos into TruncateArea
	if aFile.MaxIndex >= end && start >= aFile.MinIndex {
		startElem := aFile.LogIndex.Find(start)
		if startElem == nil {
			logrus.Fatal("TruncateLog error, ", start, end, aFile.MaxIndex, aFile.MinIndex)
		}
		startlItem := startElem.Value.(LogItem)

		endElem := aFile.LogIndex.Find(end)
		if endElem == nil {
			logrus.Fatal("TruncateLog error, ", start, end, aFile.MaxIndex, aFile.MinIndex)
		}
		endlItem := endElem.Value.(LogItem)

		ta := TruncateArea{Start: int64(startlItem.Pos) - 16, End: int64(endlItem.Pos) + int64(endlItem.Length)}
		aFile.Header.TruncateArea = append(aFile.Header.TruncateArea, &ta)
		aFile.SaveFileHeader()
		aFile.Close()
		aFile.BuildLogIndex()
		return TRUNCATED_OK
	}
	logrus.Fatal("Unknow truncateLog error, ", start, end, aFile.MaxIndex, aFile.MinIndex)
	return TRUNCATED_OK
}

func WriteFile(file os.File, pos int64, data []byte) {
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
}

func ReadFile(file os.File, pos, length int64, buff []byte) {
	_, err := file.Seek(pos, 0)
	if err != nil {
		logrus.Fatal("Seek file error, ", err)
	}
	var readCount int64
	for readCount != length {
		n, err := file.Read(buff[readCount:])
		if err != nil {
			logrus.Fatal("Read file error, ", err)
		}
		if n == 0 || n == -1 {
			logrus.Fatal("File have dirty data")
		}
		readCount = readCount + int64(n)
	}
}

func (aFile *AlfheimDBWALFile) WriteLog(lItem *LogItem, data []byte) {
	WriteFile(*aFile.File, aFile.Pos, data)
	lItem.Pos = uint64(aFile.Pos) + 8 + 8
	aFile.Pos = int64(lItem.Pos) + int64(lItem.Length)
	aFile.LogIndex.Set(lItem.Index, lItem)
	aFile.LogItems[lItem.Index] = lItem
	aFile.RefreshMinAndMaxIndex(lItem)
}

func (aFile *AlfheimDBWALFile) BatchWriteLogs(lItems []*LogItem, data []byte) {
	WriteFile(*aFile.File, aFile.Pos, data)
	for _, lItem := range lItems {
		lItem.Pos = uint64(aFile.Pos) + 8 + 8
		aFile.Pos = int64(lItem.Pos) + int64(lItem.Length)
		aFile.LogIndex.Set(lItem.Index, lItem)
		aFile.LogItems[lItem.Index] = lItem
		aFile.RefreshMinAndMaxIndex(lItem)
	}
}

func (aFile *AlfheimDBWALFile) BuildLogIndex() {
	var err error

	//open file with os.O_RDWR and os.O_CREATE, 644
	aFile.File, err = os.OpenFile(aFile.Filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logrus.Fatal("Open file error, ", err)
	}
	logrus.Info("Init wal file, ", aFile.Filename)

	//load file header
	aFile.LoadFileHeader()
	var pos, allLength int64
	pos = aFile.HeaderLength

	//get file stat
	fileStat, err := aFile.File.Stat()
	if err != nil {
		logrus.Fatal("Read file stat error, ", err)
	}

	//tree index
	sList := skiplist.New(skiplist.Int64)
	//map index
	logItems := make(map[int64]*LogItem)

	buff := make([]byte, 16)
	indexCount := 0

	for allLength < fileStat.Size() {
		lItem := new(LogItem)

		//Read length
		ReadFile(*aFile.File, pos, int64(len(buff)), buff)
		lItem.Length = ReadInt64FromBuff(buff, true)

		//Read index
		lItem.Index = int64(ReadInt64FromBuff(buff[8:], true))
		allLength = allLength + int64(lItem.Length) + 8 + 8
		lItem.Pos = uint64(pos) + 8 + 8

		pos = allLength

		//filter if log is truncated
		if aFile.FilterTruncated(int64(lItem.Pos)) {
			logrus.Info("Log is Truncated: ", *lItem)
			continue
		}

		//if log is not truncated, set index
		indexCount++
		logItems[lItem.Index] = lItem
		sList.Set(lItem.Index, lItem)
		aFile.RefreshMinAndMaxIndex(lItem)
	}
	logrus.Info("All load log item count : ", indexCount)
	aFile.Pos = allLength
	aFile.LogItems = logItems
	aFile.LogIndex = sList
	return
}

func (aFile *AlfheimDBWALFile) Close() {
	err := aFile.File.Close()
	if err != nil {
		logrus.Fatal("File close error, ", err)
	}
}

func (aFile *AlfheimDBWALFile) RefreshMinAndMaxIndex(lItem *LogItem) {
	if lItem.Index > aFile.MaxIndex {
		aFile.MaxIndex = lItem.Index
	}
	if aFile.MinIndex == -1 {
		aFile.MinIndex = lItem.Index
	} else if lItem.Index < aFile.MinIndex {
		aFile.MinIndex = lItem.Index
	}
}
