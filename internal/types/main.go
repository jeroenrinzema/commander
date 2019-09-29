package types

// CtxKey type
type CtxKey string

func (k CtxKey) String() string {
	return string(k)
}

// Available context key types
const (
	CtxHeader          = CtxKey("header")
	CtxRetries         = CtxKey("retries")
	CtxOrigin          = CtxKey("origin")
	CtxParentTimestamp = CtxKey("parent-timestamp")
	CtxParentID        = CtxKey("parent-id")
)
