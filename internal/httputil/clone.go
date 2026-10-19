package httputil

import "net/url"

// CloneURL returns a clone of the provided URL.
// copy from: net/http/clone.go
func CloneURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}

	u2 := new(url.URL)
	*u2 = *u
	if u.User != nil {
		u2.User = new(url.Userinfo)
		*u2.User = *u.User
	}
	return u2
}
