package pubsubmodel

import "errors"

var (
	ErrNotFound         = errors.New("pubsub resource not found")
	ErrAlreadyExists    = errors.New("pubsub resource already exists")
	ErrMissingReference = errors.New("pubsub resource references missing dependency")
	ErrReferenced       = errors.New("pubsub resource is still referenced")
)
