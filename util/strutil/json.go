package strutil

import "encoding/json"

func DumpJson(s interface{}) string {
	d, _ := json.Marshal(s)
	return string(d)
}
