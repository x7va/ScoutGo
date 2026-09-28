package httpclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// MiddlewareConfig holds configuration for response handling and rate limiting.
type MiddlewareConfig struct {
	SuccessStatusCodes []int         // HTTP status codes considered successful (default: 200)
	RateLimitBackoff   time.Duration // Default backoff when no Retry-After is provided
	EnableAutoRotation bool          // Automatically rotate proxy/token on rate limits
	StopOnSuccess      bool          // Stop execution on first success
}

// Middleware implements response handling and rate limiting logic.
type Middleware struct {
	config  MiddlewareConfig
	rotator *Rotator
}

// NewMiddleware creates a new middleware instance with the given configuration.
func NewMiddleware(config MiddlewareConfig, rotator *Rotator) *Middleware {
	if len(config.SuccessStatusCodes) == 0 {
		config.SuccessStatusCodes = []int{200} // Default to 200 OK
	}
	if config.RateLimitBackoff == 0 {
		config.RateLimitBackoff = 5 * time.Second // Default 5s backoff
	}

	return &Middleware{
		config:  config,
		rotator: rotator,
	}
}

// HandleSuccess processes successful responses and determines if execution should stop.
func (m *Middleware) HandleSuccess(resp *http.Response, body []byte) (identifier string, shouldStop bool) {
	statusCode := resp.StatusCode

	// Only process valid JSON responses for Discord username checking
	if len(body) > 0 && statusCode >= 200 && statusCode < 300 {
		var discordResponse map[string]interface{}
		if err := json.Unmarshal(body, &discordResponse); err == nil {
			// taken defaults to true if field is missing
			var taken bool = true
			if takenField, exists := discordResponse["taken"]; exists {
				if takenBool, ok := takenField.(bool); ok {
					taken = takenBool
				}
			}

			available := !taken

			if available {
				return "available", m.config.StopOnSuccess
			} else {
				return "taken", false
			}
		}
	}

	// Fallback for non-Discord responses
	for _, code := range m.config.SuccessStatusCodes {
		if statusCode == code {
			return fmt.Sprintf("status_%d", statusCode), m.config.StopOnSuccess
		}
	}

	return "", false
}

// HandleRateLimit processes rate limit responses (429) and Retry-After headers.
func (m *Middleware) HandleRateLimit(resp *http.Response, proxy string) (delay time.Duration, shouldRotate bool) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return 0, false
	}

	// Parse Retry-After header
	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter != "" {
		if seconds, err := strconv.Atoi(retryAfter); err == nil {
			delay = time.Duration(seconds) * time.Second
		} else {
			if retryTime, err := http.ParseTime(retryAfter); err == nil {
				delay = time.Until(retryTime)
				if delay < 0 {
					delay = 0
				}
			} else {
				delay = m.config.RateLimitBackoff
			}
		}
	} else {
		delay = m.config.RateLimitBackoff
	}

	// Ensure minimum delay
	if delay < 1*time.Second {
		delay = 5 * time.Second
	}

	// Mark proxy as rate-limited
	if m.rotator != nil && proxy != "" && proxy != "direct" {
		m.rotator.MarkProxyRateLimited(proxy, delay)
	}

	shouldRotate = m.config.EnableAutoRotation
	return delay, shouldRotate
}

// RotateCredentials forces a rotation of proxy and token.
func (m *Middleware) RotateCredentials() {
	if m.rotator != nil {
		m.rotator.NextProxy()
		m.rotator.NextToken()
	}
}

// ProcessResponse is the main entry point for middleware response processing.
func (m *Middleware) ProcessResponse(resp *http.Response, body []byte, err error, proxy string) (identifier string, shouldStop bool, shouldRotate bool, delay time.Duration) {
	if err != nil {
		return "", false, false, 0
	}

	statusCode := resp.StatusCode

	// Check for rate limit FIRST
	if statusCode == http.StatusTooManyRequests {
		delay, shouldRotate := m.HandleRateLimit(resp, proxy)
		return "", false, shouldRotate, delay
	}

	// Process Discord username response
	identifier, shouldStopSuccess := m.HandleSuccess(resp, body)

	if identifier != "" {
		return identifier, shouldStopSuccess, false, 0
	}

	// Handle other error status codes
	if statusCode >= 400 {
		return "", false, false, 0
	}

	return "", false, false, 0
}
