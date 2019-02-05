package commander

// Retry allowes a given method to be retried x ammount of times.
type Retry struct {
	Ammount int `json:"ammount"`
	Retries int
}

// Attempt tries to attempt the given method for the given ammount of retries.
// If the method still fails after the set limit is a error returned.
func (retry *Retry) Attempt(method func() error) error {
	err := method()

	if err == nil {
		return nil
	}

	if retry.Retries >= retry.Ammount {
		return err
	}

	retry.Retries++
	return retry.Attempt(method)
}
