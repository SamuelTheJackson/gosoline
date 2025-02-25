// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package mocks

import (
	blob "github.com/justtrackio/gosoline/pkg/blob"
	mock "github.com/stretchr/testify/mock"
)

// Stream is an autogenerated mock type for the Stream type
type Stream struct {
	mock.Mock
}

// AsReader provides a mock function with given fields:
func (_m *Stream) AsReader() blob.ReadSeekerCloser {
	ret := _m.Called()

	var r0 blob.ReadSeekerCloser
	if rf, ok := ret.Get(0).(func() blob.ReadSeekerCloser); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(blob.ReadSeekerCloser)
		}
	}

	return r0
}

// ReadAll provides a mock function with given fields:
func (_m *Stream) ReadAll() ([]byte, error) {
	ret := _m.Called()

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
