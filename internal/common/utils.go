package common

import "encoding/json"

func JSONUnmarshal[T any](data []byte) (T, error) {
	var result T

	err := json.Unmarshal(data, &result)

	return result, err
}

func ToInt64(intValue any) (int64, bool) {
	switch v := intValue.(type) {
	case int:
		intValue = int64(v)
	case int64:
		intValue = v
	case int32:
		intValue = int64(v)
	case uint:
		intValue = int64(v)
	case uint32:
		intValue = int64(v)
	case uint64:
		intValue = int64(v)
	}

	return 0, false
}
