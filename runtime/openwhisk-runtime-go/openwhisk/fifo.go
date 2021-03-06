package openwhisk

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// LibdMessage is message for communication with c backend
type LibdMessage struct {
	cmd    string
	params []string
}

// write command into fifo
func (ap *ActionProxy) fifoWrite(msg LibdMessage) error {
	// TODO: insert operations to call libd functions from fifo
	if ap.fifoFile == nil {
		return fmt.Errorf("Broken Fifo")
	}

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("Json encode error")
	}

	msgSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(msgSize, uint32(len(jsonMsg)))

	fullMsg := append(msgSize, jsonMsg...)
	ap.fifoFile.Write(fullMsg)
	return nil
}
