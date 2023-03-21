package models

import (
	"time"
)

type OPCDA struct {
	TagName    string      `json:"tag_name"`
	TagType    string      `json:"tag_type"`
	TagValue   interface{} `json:"tag_value"`
	TagQuality int16       `json:"quality"`
	ReadAt     time.Time   `json:"read_at"`
}
