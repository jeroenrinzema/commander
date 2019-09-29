package types

import (
	"context"
)

// NewHeaderContext creates a new context with Header attached. If used
// in conjunction with AppendToHeaderContext, NewHeaderContext will
// overwrite any previously-appended types.
func NewHeaderContext(ctx context.Context, header Header) context.Context {
	return context.WithValue(ctx, CtxHeader, header)
}

// AppendToHeaderContext returns a new context with the provided Header merged
// with any existing metadata in the context.
func AppendToHeaderContext(ctx context.Context, kv Header) context.Context {
	header, has := ctx.Value(CtxHeader).(Header)
	if !has {
		header = Header{}
	}

	for key, value := range kv {
		header[key] = value
	}

	return NewHeaderContext(ctx, header)
}

// HeaderFromContext returns the Header in ctx if it exists.
// The returned Header should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func HeaderFromContext(ctx context.Context) (header Header, ok bool) {
	header, ok = ctx.Value(CtxHeader).(Header)
	return
}

// NewRetriesContext creates a new context with Retries attached. If used
// NewRetriesContext will overwrite any previously-appended
func NewRetriesContext(ctx context.Context, retries Retries) context.Context {
	return context.WithValue(ctx, CtxRetries, retries)
}

// RetriesFromContext returns the Retries in ctx if it exists.
// The returned Retries should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func RetriesFromContext(ctx context.Context) (retries Retries, ok bool) {
	retries, ok = ctx.Value(CtxRetries).(Retries)
	return
}

// NewParentTimestampContext creates a new context with ParentTimestamp attached. If used
// NewParentTimestampContext will overwrite any previously-appended
func NewParentTimestampContext(ctx context.Context, timestamp ParentTimestamp) context.Context {
	return context.WithValue(ctx, CtxParentTimestamp, timestamp)
}

// ParentTimestampFromContext returns the ParentTimestamp in ctx if it exists.
// The returned ParentTimestamp should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func ParentTimestampFromContext(ctx context.Context) (timestamp ParentTimestamp, ok bool) {
	timestamp, ok = ctx.Value(CtxParentTimestamp).(ParentTimestamp)
	return
}

// NewParentIDContext creates a new context with ParentID attached. If used
// NewParentIDContext will overwrite any previously-appended
func NewParentIDContext(ctx context.Context, id ParentID) context.Context {
	return context.WithValue(ctx, CtxParentID, id)
}

// ParentIDFromContext returns the ParentID in ctx if it exists.
// The returned ParentID should not be modified. Writing to it may cause races. Modification should be made to copies of the returned Header.
func ParentIDFromContext(ctx context.Context) (id ParentID, ok bool) {
	id, ok = ctx.Value(CtxParentID).(ParentID)
	return
}
