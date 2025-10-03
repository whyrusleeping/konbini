package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

func storeLastSeq(filename string, seq int) error {
	data := fmt.Sprint(seq)
	return ioutil.WriteFile(filename, []byte(data), 0644)
}

func loadLastSeq(filename string) (int, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return 0, err
	}

	seqStr := strings.TrimSpace(string(data))
	seq, err := strconv.Atoi(seqStr)
	if err != nil {
		return 0, err
	}

	return seq, nil
}
