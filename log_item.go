/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-19 12:41:33
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-20 12:00:45
 */
package alfheimdbwal

type LogItem struct {
	Length uint64
	Index  int64
	Pos    uint64
}
