package metadata

import (
	"strings"
	"time"
)

const (
	// HeaderValueDivider represents the UTF-8 value that is used to divide values
	HeaderValueDivider = ";"
)

// HeaderValue a slice of header values
type HeaderValue []string

// String returns the header values separated by a ";"
func (h HeaderValue) String() string {
	return strings.Join(h, HeaderValueDivider)
}

// Header is a mapping from metadata keys to values.
type Header map[string]HeaderValue

// Retries representation of the amount of attempted retries
type Retries int32

// ParentTimestamp parent message creation time
type ParentTimestamp time.Time

// ParentID parent message id
type ParentID string

// Key representation of message key
type Key []byte
