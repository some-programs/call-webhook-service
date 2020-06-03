package stanctx

import "errors"

// SubscriptionError .
type SubscriptionError struct {
	Msg     string // message overrides
	StanErr error  // stan error, wither
	CtxErr  error  // error from Context.Err if there is one
}

func (s SubscriptionError) Error() string {
	if s.Msg != "" {
		return s.Msg
	}
	if s.StanErr != nil {
		return s.StanErr.Error()
	}
	if s.CtxErr != nil {
		return s.CtxErr.Error()
	}
	return ""
}

func (s SubscriptionError) Err() error {
	if s.StanErr != nil {
		return s.StanErr
	}
	if s.CtxErr != nil {
		return s.CtxErr
	}
	return s
}

// ErrSubscriptionEnded is returned as the SubscriptionError.Err if the subscription ends without a known error
var ErrSubscriptionEnded = errors.New("the subsciption ended")
