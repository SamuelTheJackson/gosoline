package assert

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Equal(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	validateTestTag(expected)
	validateTestTag(actual)
	assert.Equal(t, expected, actual, msgAndArgs...)
}

func NoError(t *testing.T, err error, msgAndArgs ...interface{}) bool {
	return assert.NoError(t, err, msgAndArgs)
}

func validateTestTag(obj any) {
	original := reflect.ValueOf(obj)
	validateTestTagRecursive(original)
}

func validateTestTagRecursive(value reflect.Value) {
	switch value.Kind() {
	case reflect.Ptr:
		originalValue := value.Elem()
		if !originalValue.IsValid() {
			return
		}
		validateTestTagRecursive(originalValue)
	case reflect.Interface:
		validateTestTagRecursive(value.Elem())
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i).Tag.Get("test")
			if field == "ignore" {
				if value.Field(i).CanSet() {
					value.Field(i).Set(reflect.Zero(value.Field(i).Type()))
				}

				continue
			}
			validateTestTagRecursive(value.Field(i))
		}
	case reflect.Slice:
		for i := 0; i < value.Len(); i++ {
			validateTestTagRecursive(value.Index(i))
		}
	}
}
