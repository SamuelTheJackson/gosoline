// Code generated by mockery v2.12.1. DO NOT EDIT.

package mocks

import (
	context "context"

	kafka "github.com/segmentio/kafka-go"
	mock "github.com/stretchr/testify/mock"

	testing "testing"
)

// Reader is an autogenerated mock type for the Reader type
type Reader struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *Reader) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CommitMessages provides a mock function with given fields: _a0, _a1
func (_m *Reader) CommitMessages(_a0 context.Context, _a1 ...kafka.Message) error {
	_va := make([]interface{}, len(_a1))
	for _i := range _a1 {
		_va[_i] = _a1[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, ...kafka.Message) error); ok {
		r0 = rf(_a0, _a1...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// FetchMessage provides a mock function with given fields: _a0
func (_m *Reader) FetchMessage(_a0 context.Context) (kafka.Message, error) {
	ret := _m.Called(_a0)

	var r0 kafka.Message
	if rf, ok := ret.Get(0).(func(context.Context) kafka.Message); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(kafka.Message)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadMessage provides a mock function with given fields: _a0
func (_m *Reader) ReadMessage(_a0 context.Context) (kafka.Message, error) {
	ret := _m.Called(_a0)

	var r0 kafka.Message
	if rf, ok := ret.Get(0).(func(context.Context) kafka.Message); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(kafka.Message)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Stats provides a mock function with given fields:
func (_m *Reader) Stats() kafka.ReaderStats {
	ret := _m.Called()

	var r0 kafka.ReaderStats
	if rf, ok := ret.Get(0).(func() kafka.ReaderStats); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(kafka.ReaderStats)
	}

	return r0
}

// NewReader creates a new instance of Reader. It also registers the testing.TB interface on the mock and a cleanup function to assert the mocks expectations.
func NewReader(t testing.TB) *Reader {
	mock := &Reader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
