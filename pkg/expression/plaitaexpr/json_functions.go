package plaitaexpr

import "encoding/json"

func jsonLoads(s string) (any, error) {
	var obj any
	err := json.Unmarshal([]byte(s), &obj)
	return obj, err
}

func jsonDumps(obj any) (string, error) {
	data, err := json.Marshal(obj)
	return string(data), err
}
