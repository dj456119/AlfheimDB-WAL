/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-20 11:47:46
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-29 23:52:12
 */

package alfheimdbwal

import "encoding/binary"

func NewLogItemBuff(index int64, data []byte, buff []byte, isBigEndian bool) *LogItem {
	length := len(data)
	WriteInt64ToBuff(buff, int64(length), isBigEndian)
	WriteInt64ToBuff(buff[8:], index, isBigEndian)
	copy(buff[16:], data)
	lItem := new(LogItem)
	lItem.Index = index
	lItem.Length = uint64(length)
	return lItem
}

func CreateWriteBuff(writeBuff []byte, exec func(args ...interface{}) (int64, []byte), args ...interface{}) (*LogItem, []byte) {
	index, buff := exec(args)
	lItem := NewLogItemBuff(index, buff, writeBuff, true)
	return lItem, writeBuff[:8+8+len(buff)]
}

func CreateBatchWriteBuff(batchWriteBuff []byte, execs []func(args ...interface{}) (int64, []byte), args ...[]interface{}) ([]*LogItem, []byte) {
	pos := 0
	lItems := make([]*LogItem, len(execs))
	for i, exec := range execs {
		index, buff := exec(args[i])
		lItem := NewLogItemBuff(index, buff, batchWriteBuff[pos:], true)
		lItems[i] = lItem
		pos = pos + 8 + 8 + len(buff)
	}
	return lItems, batchWriteBuff[:pos]
}

func WriteInt64ToBuff(buff []byte, data int64, isBigEndian bool) {
	if isBigEndian {
		binary.BigEndian.PutUint64(buff, uint64(data))
	} else {
		binary.LittleEndian.PutUint64(buff, uint64(data))
	}
}

func ReadInt64FromBuff(buff []byte, isBigEndian bool) uint64 {
	if isBigEndian {
		return binary.BigEndian.Uint64(buff)
	} else {
		return binary.LittleEndian.Uint64(buff)
	}
}
