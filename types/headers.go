package types

import "time"

// Header is a mapping from metadata keys to values. Users should use the following two convenience functions New and Pairs to generate Header.
type Header map[string][]string

// Retries representation of the ammount of attempted retries
type Retries int32

// ParentTimestamp parent message creation time
type ParentTimestamp time.Time

// ParentID parent message id
type ParentID string

// Key representation of message key
type Key []byte
