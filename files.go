package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
)

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}

	return fp.Sync()
}

func randomInt() int {
	var buf [8]byte
	rand.Read(buf[:])
	return int(binary.LittleEndian.Uint64(buf[:]))
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_EXCL|os.O_CREATE, 0664)
	if err != nil {
		return err
	}

	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}

	err = fp.Sync()
	if err != nil {
		return err
	}

	return os.Rename(tmp, path)
}
