// Code generated by mockery v2.13.1. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// Producer is an autogenerated mock type for the Producer type
type Producer struct {
	mock.Mock
}

// Write provides a mock function with given fields: ctx, models, attributeSets
func (_m *Producer) Write(ctx context.Context, models interface{}, attributeSets ...map[string]interface{}) error {
	_va := make([]interface{}, len(attributeSets))
	for _i := range attributeSets {
		_va[_i] = attributeSets[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, models)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, interface{}, ...map[string]interface{}) error); ok {
		r0 = rf(ctx, models, attributeSets...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// WriteOne provides a mock function with given fields: ctx, model, attributeSets
func (_m *Producer) WriteOne(ctx context.Context, model interface{}, attributeSets ...map[string]interface{}) error {
	_va := make([]interface{}, len(attributeSets))
	for _i := range attributeSets {
		_va[_i] = attributeSets[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, model)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, interface{}, ...map[string]interface{}) error); ok {
		r0 = rf(ctx, model, attributeSets...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewProducer interface {
	mock.TestingT
	Cleanup(func())
}

// NewProducer creates a new instance of Producer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewProducer(t mockConstructorTestingTNewProducer) *Producer {
	mock := &Producer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
