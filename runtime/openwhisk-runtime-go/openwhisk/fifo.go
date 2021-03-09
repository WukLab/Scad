package openwhisk

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// LibdMessage is message for communication with c backend
type LibdMessage struct {
	cmd    string
	body   string
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

func (ap *ActionProxy) handleLibdRequest(w http.ResponseWriter, r *http.Request) {
	Debug("enter handle Libd Request Function")
	// Parse body
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Error reading request body: %v", err))
		return
	}
	bodyStr := string(body)

	// Parse route
	fields := strings.Split(r.URL.Path, "/")[1:]

	if fields[0] == "action" {
		if len(fields) == 2 && r.Method == "POST" {
			// app.post('/action/:aid', addAction);
			serverIP := strings.Split(r.RemoteAddr, ":")[0]
			serverPort := ap.env["__OW_INVOKER_API_PORT"]
			serverURL := fmt.Sprintf("%s:%s", serverIP, serverPort)

			params := []string{serverURL}

			ap.fifoWrite(LibdMessage{cmd: "ACTADD", params: params, body: bodyStr})

			sendOK(w)
			return
		} else if len(fields) == 3 && fields[2] == "transport" && r.Method == "POST" {
			// app.post('/action/:aid/transport', addTransport);
			ap.fifoWrite(LibdMessage{cmd: "TRANSADD", body: bodyStr})

			sendOK(w)
			return
		} else if len(fields) == 4 && fields[2] == "transport" {
			if r.Method == "PUT" {
				// app.put ('/action/:aid/transport/:tname', platformFactory.wrapEndpoint(service.configTransport));
				ap.fifoWrite(LibdMessage{cmd: "TRANSCONF", body: bodyStr})

				sendOK(w)
				return
			} else if r.Method == "GET" {
				// app.get ('/action/:aid/transport/:tname', platformFactory.wrapEndpoint(service.configTransport));
				sendError(w, 404, "Request not implemented")
				return
			}
		}
	}

	// If we cannot find the route
	sendError(w, 404, "Request path not found")
}
