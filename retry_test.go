package commander

import (
	"errors"
	"testing"
)

// TestRetry tries to retry a method x amount of times
func TestRetry(t *testing.T) {
	retry := Retry{
		Amount: 10,
	}

	err := retry.Attempt(func() error {
		return nil
	})

	if err != nil {
		t.Error(err)
	}
}

// TestRetryFail tests if a retry attempt is failing
func TestRetryFail(t *testing.T) {
	retry := Retry{
		Amount: 1,
	}

	err := retry.Attempt(func() error {
		return errors.New("failed")
	})

	if err == nil {
		t.Error("No error is thrown")
	}
}

// TestRetryNotFail tests if a retry attempt is not failing after the second attempt
func TestRetryNotFail(t *testing.T) {
	retry := Retry{
		Amount: 5,
	}

	count := 0
	err := retry.Attempt(func() error {
		count++

		if count == 2 {
			return nil
		}

		return errors.New("failed")
	})

	if err != nil {
		t.Error("A error is thrown")
	}

	if retry.Retries != 1 {
		t.Errorf("Retry did not retry 1 time but: %d", retry.Retries)
	}
}
