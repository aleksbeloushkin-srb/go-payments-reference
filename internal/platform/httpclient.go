package platform

import (
	"net"
	"net/http"
	"time"
)

// NewHTTPClient returns an http.Client for one upstream: more idle
// connections per host than the default of two, and short connect and TLS
// timeouts. Client.Timeout is only a backstop; every call carries its own
// context deadline.
func NewHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{
		Timeout: time.Minute,
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         dialer.DialContext,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     60 * time.Second,
			TLSHandshakeTimeout: 3 * time.Second,
		},
	}
}
