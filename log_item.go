/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-19 12:41:33
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-19 17:57:15
 */
package alfheimdbwal

import (
	"encoding/binary"
)

type LogItem struct {
	Length uint64
	Index  int64
	Pos    uint64
}

func WriteInt64ToBuff(nowpos int, buff []byte, data int64, isBigEndian bool) {
	i := nowpos + 8
	tempBuff := buff[nowpos : i+1]
	if isBigEndian {
		binary.BigEndian.PutUint64(tempBuff, uint64(data))
	} else {
		binary.LittleEndian.PutUint64(tempBuff, uint64(data))
	}
}

func ReadInt64FromBuff(buff []byte, isBigEndian bool) uint64 {
	if isBigEndian {
		return binary.BigEndian.Uint64(buff)
	} else {
		return binary.LittleEndian.Uint64(buff)
	}
}
