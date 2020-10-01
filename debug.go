package csql

import (
	"encoding/json"
)

func jsonify(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func debugf(format string, args ...interface{}) {
	// fmt.Printf(format, args...)
}
