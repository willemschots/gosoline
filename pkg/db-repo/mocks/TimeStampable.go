// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"
import time "time"

// TimeStampable is an autogenerated mock type for the TimeStampable type
type TimeStampable struct {
	mock.Mock
}

// SetCreatedAt provides a mock function with given fields: createdAt
func (_m *TimeStampable) SetCreatedAt(createdAt *time.Time) {
	_m.Called(createdAt)
}

// SetUpdatedAt provides a mock function with given fields: updatedAt
func (_m *TimeStampable) SetUpdatedAt(updatedAt *time.Time) {
	_m.Called(updatedAt)
}
