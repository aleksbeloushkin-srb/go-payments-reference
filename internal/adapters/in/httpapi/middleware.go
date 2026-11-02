package httpapi

import (
	"crypto/rand"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/aleksbeloushkin-srb/go-payments-reference/internal/platform"
)

// chain wraps h so that the first middleware is the outermost.
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// requestIDHeader carries the request id in both directions.
const requestIDHeader = "X-Request-ID"

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// requestID puts a request id in the context and the response. A
// well-formed incoming id is kept so a caller can correlate its logs with
// ours; anything else is replaced. Behind a public edge, let the edge proxy
// overwrite the header so clients cannot inject ids.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if !validRequestID.MatchString(id) {
			id = rand.Text()
		}
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(platform.WithRequestID(r.Context(), id)))
	})
}

// statusWriter records the status code and body size for the access log.
type statusWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer (Flush,
// SetWriteDeadline) through this wrapper.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// logging writes one access-log line per request. It logs the route pattern
// and the path, never the query string or the body.
func logging(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			log.InfoContext(r.Context(), "http request",
				"method", r.Method,
				"route", r.Pattern, // set by ServeMux on this same *Request
				"path", r.URL.Path,
				"status", sw.status,
				"bytes", sw.bytes,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}

// recoverer turns a handler panic into a logged 500. It sits inside logging
// so the access log records the 500. http.ErrAbortHandler is re-panicked:
// it is net/http's way to abort a response on purpose.
func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sw := &statusWriter{ResponseWriter: w}
			defer func() {
				p := recover()
				if p == nil {
					return
				}
				if p == http.ErrAbortHandler {
					panic(p)
				}
				log.ErrorContext(r.Context(), "panic recovered", "panic", p, "stack", string(debug.Stack()))
				if !sw.wroteHeader {
					writeJSON(sw, http.StatusInternalServerError, errorBody{Code: "internal", Message: "internal error"})
				}
			}()
			next.ServeHTTP(sw, r)
		})
	}
}
