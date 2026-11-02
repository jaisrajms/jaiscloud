package model

import "fmt"

// Stubs mirroring jaiscloud/internal/model for the analyzer test.

type ErrorDetail struct {
	Type     string
	Reason   string
	Domain   string
	Metadata map[string]string
}

type ProviderError struct {
	Code       string
	Message    string
	HTTPStatus int
	Status     string
	Data       map[string]any
	Details    []ErrorDetail
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("%s: %s (HTTP %d)", e.Code, e.Message, e.HTTPStatus)
}

func NewProviderError(code, message string, httpStatus int) *ProviderError {
	return &ProviderError{Code: code, Message: message, HTTPStatus: httpStatus}
}

func (e *ProviderError) WithStatus(status string) *ProviderError {
	e.Status = status
	return e
}

func (e *ProviderError) WithDetails(details []ErrorDetail) *ProviderError {
	e.Details = details
	return e
}
