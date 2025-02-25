// Code generated by mockery v2.13.1. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// SizedStore is an autogenerated mock type for the SizedStore type
type SizedStore[T interface{}] struct {
	mock.Mock
}

// Contains provides a mock function with given fields: ctx, key
func (_m *SizedStore[T]) Contains(ctx context.Context, key interface{}) (bool, error) {
	ret := _m.Called(ctx, key)

	var r0 bool
	if rf, ok := ret.Get(0).(func(context.Context, interface{}) bool); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, interface{}) error); ok {
		r1 = rf(ctx, key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Delete provides a mock function with given fields: ctx, key
func (_m *SizedStore[T]) Delete(ctx context.Context, key interface{}) error {
	ret := _m.Called(ctx, key)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, interface{}) error); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteBatch provides a mock function with given fields: ctx, keys
func (_m *SizedStore[T]) DeleteBatch(ctx context.Context, keys interface{}) error {
	ret := _m.Called(ctx, keys)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, interface{}) error); ok {
		r0 = rf(ctx, keys)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// EstimateSize provides a mock function with given fields:
func (_m *SizedStore[T]) EstimateSize() *int64 {
	ret := _m.Called()

	var r0 *int64
	if rf, ok := ret.Get(0).(func() *int64); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*int64)
		}
	}

	return r0
}

// Get provides a mock function with given fields: ctx, key, value
func (_m *SizedStore[T]) Get(ctx context.Context, key interface{}, value *T) (bool, error) {
	ret := _m.Called(ctx, key, value)

	var r0 bool
	if rf, ok := ret.Get(0).(func(context.Context, interface{}, *T) bool); ok {
		r0 = rf(ctx, key, value)
	} else {
		r0 = ret.Get(0).(bool)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, interface{}, *T) error); ok {
		r1 = rf(ctx, key, value)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetBatch provides a mock function with given fields: ctx, keys, values
func (_m *SizedStore[T]) GetBatch(ctx context.Context, keys interface{}, values interface{}) ([]interface{}, error) {
	ret := _m.Called(ctx, keys, values)

	var r0 []interface{}
	if rf, ok := ret.Get(0).(func(context.Context, interface{}, interface{}) []interface{}); ok {
		r0 = rf(ctx, keys, values)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]interface{})
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, interface{}, interface{}) error); ok {
		r1 = rf(ctx, keys, values)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Put provides a mock function with given fields: ctx, key, value
func (_m *SizedStore[T]) Put(ctx context.Context, key interface{}, value T) error {
	ret := _m.Called(ctx, key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, interface{}, T) error); ok {
		r0 = rf(ctx, key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PutBatch provides a mock function with given fields: ctx, values
func (_m *SizedStore[T]) PutBatch(ctx context.Context, values interface{}) error {
	ret := _m.Called(ctx, values)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, interface{}) error); ok {
		r0 = rf(ctx, values)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewSizedStore interface {
	mock.TestingT
	Cleanup(func())
}

// NewSizedStore creates a new instance of SizedStore. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewSizedStore[T interface{}](t mockConstructorTestingTNewSizedStore) *SizedStore[T] {
	mock := &SizedStore[T]{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
