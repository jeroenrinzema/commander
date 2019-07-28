package metadata

// Key typed context key
type Key string

func (k Key) String() string {
	return string(k)
}

// Available context key types
const (
	CtxHeader          = Key("header")
	CtxStatus          = Key("status")
	CtxRetries         = Key("retries")
	CtxOrigin          = Key("origin")
	CtxParentTimestamp = Key("parent-timestamp")
	CtxParentID        = Key("parent-id")
)
