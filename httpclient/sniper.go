package httpclient

// ScoutGo HTTP Client - By @x7va

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Result represents the outcome of a single HTTP request execution.
// It contains the target identifier, HTTP status code, latency in milliseconds,
// and any error that occurred during the request.
type Result struct {
	Target     string        // Target identifier (URL or endpoint)
	Status     int           // HTTP status code (0 if request failed)
	Latency    time.Duration // Request latency in milliseconds
	Error      error         // Error if request failed, nil otherwise
	Proxy      string        // Proxy used for this request (if any)
	Token      string        // Token used for this request (truncated for logging)
	Identifier string        // Extracted identifier from successful response (if available)
	Delay      time.Duration // Rate limit delay if applicable
}

// SniperConfig holds configuration for the Sniper execution engine.
type SniperConfig struct {
	WorkerCount    int               // Number of concurrent worker goroutines
	HTTPMethod     string            // HTTP method to use (GET, POST, PATCH, PUT, DELETE)
	JSONPayload    interface{}       // JSON payload to send with request (for POST/PATCH/PUT)
	RequestTimeout time.Duration     // Per-request timeout
	Headers        map[string]string // Additional headers to include
	Middleware     *Middleware       // Response handler and rate limiting middleware
	BaseURL        string            // Base URL for constructing full target URLs
	RequestDelay   int               // Delay between requests in seconds (when no proxies)
}

// Sniper is the core execution engine that manages concurrent HTTP request processing.
// It uses a worker pool pattern with goroutines and channels for high-throughput execution.
type Sniper struct {
	config      SniperConfig
	rotator     *Rotator
	targets     []string
	workers     int
	resultsChan chan Result
	wg          sync.WaitGroup
	middleware  *Middleware
	metrics     *DashboardMetrics
	baseURL     string // Base URL for constructing full target URLs
	useProxies  bool   // Whether proxies are being used
}

// NewSniper creates a new Sniper instance with the given configuration and rotator.
// It initializes the worker pool and results channel based on the configuration.
func NewSniper(config SniperConfig, rotator *Rotator) *Sniper {
	if config.WorkerCount <= 0 {
		config.WorkerCount = 10 // Default to 10 workers
	}
	if config.HTTPMethod == "" {
		config.HTTPMethod = "GET" // Default to GET
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 30 * time.Second // Default 30s timeout
	}
	if config.RequestDelay == 0 {
		config.RequestDelay = 3 // Default 3 second delay when no proxies
	}

	// Initialize middleware if not provided
	var middleware *Middleware
	if config.Middleware == nil {
		middleware = NewMiddleware(MiddlewareConfig{
			SuccessStatusCodes: []int{200},
			RateLimitBackoff:   5 * time.Second,
			EnableAutoRotation: true,
			StopOnSuccess:      false,
		}, rotator)
	} else {
		middleware = config.Middleware
	}

	// Initialize metrics
	metrics := &DashboardMetrics{}

	return &Sniper{
		config:      config,
		rotator:     rotator,
		workers:     config.WorkerCount,
		resultsChan: make(chan Result, config.WorkerCount*2), // Buffered channel
		middleware:  middleware,
		metrics:     metrics,
		baseURL:     config.BaseURL,
		useProxies:  true, // Will be set based on actual proxy availability
	}
}

// SetTargets directly sets the targets from a slice (for random username generation)
func (s *Sniper) SetTargets(targets []string) {
	s.targets = targets
}

// LoadTargets reads target URLs from a file (data/targets.txt or data/names_to_check.txt).
// Each line should contain a single target URL or endpoint.
// Empty lines and lines starting with # or // are ignored.
func (s *Sniper) LoadTargets(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open targets file: %w", err)
	}
	defer file.Close()

	var targets []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		targets = append(targets, line)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading targets file: %w", err)
	}

	if len(targets) == 0 {
		return fmt.Errorf("no targets found in %s", filename)
	}

	s.targets = targets
	return nil
}

// SetProxyUsage sets whether proxies are being used
func (s *Sniper) SetProxyUsage(useProxies bool) {
	s.useProxies = useProxies
}

// Results returns the read-only results channel.
// Consumers can read from this channel to receive request results as they complete.
func (s *Sniper) Results() <-chan Result {
	return s.resultsChan
}

// Execute starts the concurrent execution engine.
// It launches worker goroutines that process targets from the loaded list.
// The method blocks until all targets are processed, then closes the results channel.
func (s *Sniper) Execute() {
	if len(s.targets) == 0 {
		close(s.resultsChan)
		return
	}

	// Create a buffered channel for targets
	targetChan := make(chan string, s.workers)

	// Launch worker goroutines
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker(targetChan, i)
	}

	// Feed targets to workers
	go func() {
		for _, target := range s.targets {
			targetChan <- target
		}
		close(targetChan)
	}()

	// Wait for all workers to complete
	go func() {
		s.wg.Wait()
		close(s.resultsChan)
	}()
}

// worker is a goroutine that processes targets from the channel.
// Each worker pulls a unique proxy and token pair for each request,
// executes the HTTP request, and sends results down the results channel.
func (s *Sniper) worker(targetChan <-chan string, workerID int) {
	defer s.wg.Done()

	for target := range targetChan {
		// Add delay if not using proxies (rate limiting)
		if !s.useProxies && s.config.RequestDelay > 0 {
			time.Sleep(time.Duration(s.config.RequestDelay) * time.Second)
		}

		result := s.executeRequest(target, workerID)

		// Update metrics based on result
		s.metrics.IncrementTotalChecks()

		if result.Error == nil {
			// For Discord username checking, check identifier FIRST regardless of status code
			// This matches the Python code logic which processes JSON regardless of HTTP status
			if result.Identifier == "available" {
				s.metrics.IncrementAvailableStatus()
				s.metrics.IncrementSuccessfulClaims()
				// Save hit to results/hits.txt
				saveHit(result.Target)
				// Format: [Available] username RPS: X/s | resp: {'taken': False} | proxy: address
				elapsed := time.Since(s.metrics.StartTime)
				var rps float64
				if elapsed.Seconds() > 0 {
					rps = float64(s.metrics.TotalChecks.Load()) / elapsed.Seconds()
				}
				proxyAddr := result.Proxy
				if proxyAddr == "" {
					proxyAddr = "direct"
				} else if len(proxyAddr) > 30 {
					proxyAddr = proxyAddr[:30]
				}
				fmt.Printf("%s[Available]%s %s, RPS : %.0f / s, resp : {'taken': False}, proxy : %s\n", "\033[32m", "\033[0m", result.Target, rps, proxyAddr)
			} else if result.Identifier == "taken" {
				// Format: [Taken] username RPS: X/s | resp: {'taken': True} | proxy: address
				elapsed := time.Since(s.metrics.StartTime)
				var rps float64
				if elapsed.Seconds() > 0 {
					rps = float64(s.metrics.TotalChecks.Load()) / elapsed.Seconds()
				}
				proxyAddr := result.Proxy
				if proxyAddr == "" {
					proxyAddr = "direct"
				} else if len(proxyAddr) > 30 {
					proxyAddr = proxyAddr[:30]
				}
				fmt.Printf("%s[Taken]%s %s, RPS : %.0f / s, resp : {'taken': True}, proxy : %s\n", "\033[31m", "\033[0m", result.Target, rps, proxyAddr)
				s.metrics.IncrementTakenStatus()
			} else if result.Identifier == "error" {
				s.metrics.IncrementErrors()
			} else if result.Status == 429 {
				s.metrics.IncrementRateLimits()
				// Track rate limit with timer manager using unique ID
				s.rotator.TrackRateLimit(result.Target, result.Proxy, result.Delay, 0)
			} else if result.Status >= 200 && result.Status < 300 {
				s.metrics.IncrementAvailableStatus()
				if result.Identifier != "" {
					s.metrics.IncrementSuccessfulClaims()
				}
			} else {
				s.metrics.IncrementErrors()
			}
		} else {
			s.metrics.IncrementErrors()
			// Mark proxy as failed on errors
			errMsg := result.Error.Error()
			if strings.Contains(errMsg, "proxy") || strings.Contains(errMsg, "socks") || strings.Contains(errMsg, "connect") || strings.Contains(errMsg, "timeout") {
				if s.rotator != nil && result.Proxy != "" && result.Proxy != "direct" {
					s.rotator.MarkProxyFailed(result.Proxy)
				}
			}
		}

		s.resultsChan <- result
	}
}

// executeRequest performs a single HTTP request with the given target.
// It measures latency, handles errors, and returns a Result struct.
func (s *Sniper) executeRequest(target string, workerID int) Result {
	startTime := time.Now()

	// Get client with proxy and token
	client, token, proxy, err := s.rotator.GetClientWithProxyAndToken()
	if err != nil {
		return Result{
			Target:  target,
			Status:  0,
			Latency: time.Since(startTime),
			Error:   fmt.Errorf("failed to get client with proxy: %w", err),
			Proxy:   proxy,
		}
	}

	// Use base URL
	fullURL := s.baseURL
	if fullURL == "" {
		fullURL = target
	}

	// Prepare request body with username for Discord
	var body io.Reader
	if s.config.JSONPayload != nil && (s.config.HTTPMethod == "POST" || s.config.HTTPMethod == "PATCH" || s.config.HTTPMethod == "PUT") {
		payloadCopy := make(map[string]interface{})
		if pv, ok := s.config.JSONPayload.(map[string]interface{}); ok {
			for k, v := range pv {
				if strVal, isStr := v.(string); isStr && strVal == "TARGET_PLACEHOLDER" {
					payloadCopy[k] = target
				} else {
					payloadCopy[k] = v
				}
			}
		} else {
			payloadCopy["username"] = target
		}

		jsonData, err := json.Marshal(payloadCopy)
		if err != nil {
			return Result{
				Target:  target,
				Status:  0,
				Latency: time.Since(startTime),
				Error:   fmt.Errorf("failed to marshal JSON payload: %w", err),
			}
		}

		body = bytes.NewReader(jsonData)
	}

	// Create HTTP request
	req, err := http.NewRequest(s.config.HTTPMethod, fullURL, body)
	if err != nil {
		return Result{
			Target:  target,
			Status:  0,
			Latency: time.Since(startTime),
			Error:   fmt.Errorf("failed to create request: %w", err),
		}
	}

	// Set headers
	if token != "" && !strings.Contains(s.baseURL, "unauthed") {
		req.Header.Set("Authorization", token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	for key, value := range s.config.Headers {
		req.Header.Set(key, value)
	}

	// Set timeout
	client.Timeout = s.config.RequestTimeout

	// Execute request
	resp, err := client.Do(req)
	latency := time.Since(startTime)

	if err != nil {
		return Result{
			Target:  target,
			Status:  0,
			Latency: latency,
			Error:   fmt.Errorf("request failed: %w", err),
		}
	}
	defer resp.Body.Close()

	// Read response body
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		responseBody = []byte{}
	}

	// Process response through middleware
	var identifier string
	var shouldRotate bool
	var delay time.Duration

	if s.middleware != nil {
		identifier, _, shouldRotate, delay = s.middleware.ProcessResponse(resp, responseBody, nil, proxy)
	}

	// Rotate credentials if requested
	if shouldRotate {
		s.middleware.RotateCredentials()
		s.metrics.IncrementProxySwitches()
	}

	return Result{
		Target:     target,
		Status:     resp.StatusCode,
		Latency:    latency,
		Error:      nil,
		Proxy:      proxy,
		Token:      TruncateToken(token, 10),
		Identifier: identifier,
		Delay:      delay,
	}
}

// TruncateToken returns a truncated version of the token for logging purposes.
// This prevents sensitive credentials from appearing in logs.
// Exported to allow use by other packages.
func TruncateToken(token string, maxLen int) string {
	if token == "" {
		return ""
	}
	if len(token) <= maxLen {
		return token
	}
	return token[:maxLen] + "..."
}

// saveHit saves a successful username hit to results/hits.txt
func saveHit(username string) error {
	if err := os.MkdirAll("results", 0755); err != nil {
		return err
	}

	file, err := os.OpenFile("results/hits.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(username + "\n")
	return err
}
