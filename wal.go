/*
 * @Descripttion:
 * @version:
 * @Author: cm.d
 * @Date: 2021-11-18 19:24:19
 * @LastEditors: cm.d
 * @LastEditTime: 2021-11-18 19:37:11
 */
package alfheimdbwal

import "fmt"

func NewWAL(waldir string) *AlfheimDBWAL {
	wal := new(AlfheimDBWAL)
	return wal
}

type AlfheimDBWAL struct {
}

func Write() {
	fmt.Println("write ok")
}
