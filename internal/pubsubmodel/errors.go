package pubsubmodel

import "errors"

var (
	ErrNotFound         = errors.New("pubsub resource not found")
	ErrMissingReference = errors.New("pubsub resource references missing dependency")
	ErrReferenced       = errors.New("pubsub resource is still referenced")
)
