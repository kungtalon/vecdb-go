package common

import "encoding/json"

func JSONUnmarshal[T any](data []byte) (T, error) {
	var result T

	err := json.Unmarshal(data, &result)

	return result, err
}
