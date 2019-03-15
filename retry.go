package commander

// Retry allowes a given method to be retried x amount of times.
type Retry struct {
	Amount  int `json:"amount"`
	Retries int
}

// Attempt tries to attempt the given method for the given amount of retries.
// If the method still fails after the set limit is a error returned.
func (retry *Retry) Attempt(method func() error) error {
	err := method()

	if err == nil {
		return nil
	}

	if retry.Retries >= retry.Amount {
		return err
	}

	retry.Retries++
	return retry.Attempt(method)
}
