package metadata

import (
	"context"

	"github.com/jeroenrinzema/commander/types"
)

// NewHeaderContext creates a new context with Header attached. If used
// in conjunction with AppendToHeaderContext, NewHeaderContext will
// overwrite any previously-appended metadata.
func NewHeaderContext(ctx context.Context, header types.Header) context.Context {
	return context.WithValue(ctx, CtxHeader, header)
}

// AppendToHeaderContext returns a new context with the provided Header merged
// with any existing metadata in the context.
func AppendToHeaderContext(ctx context.Context, kv types.Header) context.Context {
	header, has := ctx.Value(CtxHeader).(types.Header)
	if !has {
		header = types.Header{}
	}

	for key, value := range kv {
		header[key] = value
	}

	return NewHeaderContext(ctx, header)
}

// HeaderFromContext returns the Header in ctx if it exists.
// The returned Header should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func HeaderFromContext(ctx context.Context) (header types.Header, ok bool) {
	header, ok = ctx.Value(CtxHeader).(types.Header)
	return
}

// NewRetriesContext creates a new context with Retries attached. If used
// NewRetriesContext will overwrite any previously-appended metadata.
func NewRetriesContext(ctx context.Context, retries types.Retries) context.Context {
	return context.WithValue(ctx, CtxRetries, retries)
}

// RetriesFromContext returns the Retries in ctx if it exists.
// The returned Retries should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func RetriesFromContext(ctx context.Context) (retries types.Retries, ok bool) {
	retries, ok = ctx.Value(CtxRetries).(types.Retries)
	return
}

// NewParentTimestampContext creates a new context with ParentTimestamp attached. If used
// NewParentTimestampContext will overwrite any previously-appended metadata.
func NewParentTimestampContext(ctx context.Context, timestamp types.ParentTimestamp) context.Context {
	return context.WithValue(ctx, CtxParentTimestamp, timestamp)
}

// ParentTimestampFromContext returns the ParentTimestamp in ctx if it exists.
// The returned ParentTimestamp should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func ParentTimestampFromContext(ctx context.Context) (timestamp types.ParentTimestamp, ok bool) {
	timestamp, ok = ctx.Value(CtxParentTimestamp).(types.ParentTimestamp)
	return
}

// NewParentIDContext creates a new context with ParentID attached. If used
// NewParentIDContext will overwrite any previously-appended metadata.
func NewParentIDContext(ctx context.Context, id types.ParentID) context.Context {
	return context.WithValue(ctx, CtxParentID, id)
}

// ParentIDFromContext returns the ParentID in ctx if it exists.
// The returned ParentID should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func ParentIDFromContext(ctx context.Context) (id types.ParentID, ok bool) {
	id, ok = ctx.Value(CtxParentID).(types.ParentID)
	return
}

// NewStatusCodeContext creates a new context with Status attached. If used
// NewStatusCodeContext will overwrite any previously-appended metadata.
func NewStatusCodeContext(ctx context.Context, code types.StatusCode) context.Context {
	return context.WithValue(ctx, CtxStatus, code)
}

// StatusCodeFromContext returns the Status in ctx if it exists.
// The returned Status should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func StatusCodeFromContext(ctx context.Context) (status types.StatusCode, ok bool) {
	status, ok = ctx.Value(CtxStatus).(types.StatusCode)
	return
}
