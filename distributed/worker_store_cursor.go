package distributed

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

type workerCursor struct {
	UpdatedAt time.Time `json:"u"`
	ID        string    `json:"i"`
}

func encodeWorkerCursor(c workerCursor) string {
	data, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(data)
}

func decodeWorkerCursor(str string) (workerCursor, error) {
	var c workerCursor
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}
