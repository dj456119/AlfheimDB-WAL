/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-20 11:47:46
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-20 12:09:23
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
