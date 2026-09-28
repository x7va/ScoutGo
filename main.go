package main

// ScoutGo - Discord Username Availability Checker
// By @x7va

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"scoutgo/httpclient"
)

// ============================================================================
// CONFIGURATION
// ============================================================================

// Config holds all configuration parameters for ScoutGo
type Config struct {
	// Worker and Request Settings
	Workers      int               `json:"workers"`
	Method       string            `json:"method"`
	Timeout      int               `json:"timeout"`
	BaseURL      string            `json:"base_url"`
	Headers      map[string]string `json:"headers"`
	RequestDelay int               `json:"request_delay"`

	// Proxy Settings
	UseProxies            bool   `json:"use_proxies"`
	ProxyFile             string `json:"proxy_file"`
	AutoRemoveDeadProxies bool   `json:"auto_remove_dead_proxies"`
	ReuseProxies          bool   `json:"reuse_proxies"`
	RemoveProxies         bool   `json:"remove_proxies"`

	// Token Settings
	UseTokens    bool   `json:"use_tokens"`
	TokenFile    string `json:"token_file"`
	AutoRotation bool   `json:"auto_rotation"`

	// Target Settings
	TargetFile        string `json:"target_file"`
	GenerateUsernames bool   `json:"generate_usernames"`
	UsernameCount     int    `json:"username_count"`
	UsernameLength    int    `json:"username_length"`

	// Response Handling
	SuccessCodes     []int `json:"success_codes"`
	RateLimitBackoff int   `json:"rate_limit_backoff"`
	StopOnSuccess    bool  `json:"stop_on_success"`

	// Claim Settings
	ClaimConcurrency int `json:"claim_concurrency"`

	// Webhook Settings
	WebhookURL string `json:"webhook_url"`

	// Payload for POST requests
	Payload map[string]interface{} `json:"json_payload"`

	// GitHub Settings
	UseGitHub   bool   `json:"use_github"`
	GitHubToken string `json:"github_token"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		Workers:               10,
		Method:                "POST",
		Timeout:               30,
		BaseURL:               "https://discord.com/api/v9/unique-username/username-attempt-unauthed",
		ProxyFile:             "data/proxies.txt",
		TokenFile:             "data/tokens.txt",
		TargetFile:            "data/names_to_check.txt",
		UsernameCount:         100,
		UsernameLength:        4,
		RequestDelay:          1,
		SuccessCodes:          []int{200},
		RateLimitBackoff:      5,
		AutoRotation:          true,
		StopOnSuccess:         false,
		AutoRemoveDeadProxies: true,
		ReuseProxies:          true,
		RemoveProxies:         true,
		Headers:               map[string]string{},
		ClaimConcurrency:      1,
		WebhookURL:            "",
		Payload: map[string]interface{}{
			"username": "TARGET_PLACEHOLDER",
		},
		UseGitHub:   false,
		GitHubToken: "",
	}
}

// validateConfig validates the configuration parameters
func validateConfig(config *Config) error {
	if config.Workers <= 0 {
		return fmt.Errorf("workers must be greater than 0")
	}
	if config.Workers > 1000 {
		fmt.Printf("Warning: High worker count (%d) may impact system performance\n", config.Workers)
	}

	validMethods := map[string]bool{
		"GET": true, "POST": true, "PATCH": true, "PUT": true, "DELETE": true,
	}
	if !validMethods[config.Method] {
		return fmt.Errorf("invalid HTTP method: %s", config.Method)
	}

	if config.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}

	if config.UseProxies && config.ProxyFile == "" {
		return fmt.Errorf("proxy file path cannot be empty when using proxies")
	}

	if config.UseTokens && config.TokenFile == "" {
		return fmt.Errorf("token file path cannot be empty when using tokens")
	}

	if !config.GenerateUsernames && config.TargetFile == "" {
		return fmt.Errorf("target file path cannot be empty when not generating usernames")
	}

	if config.GenerateUsernames && config.UsernameCount <= 0 {
		return fmt.Errorf("username count must be greater than 0")
	}

	if config.GenerateUsernames && config.UsernameLength < 2 {
		return fmt.Errorf("username length must be at least 2 for Discord compatibility")
	}

	if config.BaseURL == "" {
		return fmt.Errorf("base URL is required for Discord API")
	}

	return nil
}

// ============================================================================
// PROXY CHECKER
// ============================================================================

// runProxyCheck tests proxy connectivity to Discord
func runProxyCheck() {
	fmt.Println("\n=== Proxy Checker Mode ===")
	fmt.Print("Enter proxy file path (default: data/proxies.txt): ")
	var proxyFile string
	fmt.Scanln(&proxyFile)
	if proxyFile == "" {
		proxyFile = "data/proxies.txt"
	}

	file, err := os.Open(proxyFile)
	if err != nil {
		fmt.Printf("Error opening proxy file: %v\n", err)
		return
	}
	defer file.Close()

	var proxies []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			proxies = append(proxies, line)
		}
	}

	if len(proxies) == 0 {
		fmt.Println("No proxies found in file")
		return
	}

	fmt.Printf("Loaded %d proxies. Starting check...\n\n", len(proxies))

	var wg sync.WaitGroup
	var mu sync.Mutex
	working := 0
	failed := 0
	workers := 10
	var workingProxies []string

	semaphore := make(chan struct{}, workers)

	for _, proxyStr := range proxies {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			proxyURL, err := url.Parse(p)
			if err != nil {
				mu.Lock()
				failed++
				fmt.Printf("[FAIL] %s - Invalid URL\n", p)
				mu.Unlock()
				return
			}

			client := &http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyURL(proxyURL),
				},
				Timeout: 10 * time.Second,
			}

			resp, err := client.Get("https://discord.com")
			if err != nil {
				mu.Lock()
				failed++
				fmt.Printf("[FAIL] %s - %v\n", p, err)
				mu.Unlock()
				return
			}
			resp.Body.Close()

			mu.Lock()
			working++
			workingProxies = append(workingProxies, p)
			fmt.Printf("[WORKING] %s - Status: %d\n", p, resp.StatusCode)
			mu.Unlock()
		}(proxyStr)
	}

	wg.Wait()

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Total: %d | Working: %d | Failed: %d | Success Rate: %.1f%%\n",
		len(proxies), working, failed, float64(working)/float64(len(proxies))*100)

	if working > 0 {
		fmt.Print("\nSave working proxies to file? (y/n, default: y): ")
		var save string
		fmt.Scanln(&save)
		if save == "" || save == "y" || save == "Y" {
			fmt.Print("Output file path (default: results/working_proxies.txt): ")
			var outputFile string
			fmt.Scanln(&outputFile)
			if outputFile == "" {
				outputFile = "results/working_proxies.txt"
			}

			file, err := os.Create(outputFile)
			if err != nil {
				fmt.Printf("Error creating output file: %v\n", err)
				return
			}
			defer file.Close()

			for _, proxy := range workingProxies {
				file.WriteString(proxy + "\n")
			}

			fmt.Printf("Saved %d working proxies to %s\n", working, outputFile)
		}
	}
}

// ============================================================================
// ACCOUNT AND CLAIM TYPES
// ============================================================================

// UsernameDrop represents a scheduled username drop
type UsernameDrop struct {
	Username string
	DropTime time.Time
}

// Account represents a Discord account with its token and status
type Account struct {
	Token         string
	Password      string
	LastUsed      time.Time
	FailCount     int
	SuccessCount  int
	CooldownUntil time.Time
	InCooldown    bool
}

// AccountPool manages multiple Discord accounts for rotation
type AccountPool struct {
	accounts     []*Account
	currentIndex int
	mu           sync.Mutex
}

// NewAccountPool creates a new account pool from tokens and passwords
func NewAccountPool(tokens []string, passwords []string) *AccountPool {
	pool := &AccountPool{
		accounts: make([]*Account, 0, len(tokens)),
	}

	for i, token := range tokens {
		password := ""
		if i < len(passwords) {
			password = passwords[i]
		}
		pool.accounts = append(pool.accounts, &Account{
			Token:    token,
			Password: password,
		})
	}

	return pool
}

// GetNextAvailableToken returns the next available token for claiming
func (p *AccountPool) GetNextAvailableToken() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()

	for i := 0; i < len(p.accounts); i++ {
		account := p.accounts[p.currentIndex]

		if account.InCooldown && now.Before(account.CooldownUntil) {
			p.currentIndex = (p.currentIndex + 1) % len(p.accounts)
			continue
		}

		if account.InCooldown && now.After(account.CooldownUntil) {
			account.InCooldown = false
			account.FailCount = 0
		}

		account.LastUsed = now
		token := account.Token
		p.currentIndex = (p.currentIndex + 1) % len(p.accounts)
		return token
	}

	earliestAccount := p.accounts[0]
	for _, account := range p.accounts {
		if account.CooldownUntil.Before(earliestAccount.CooldownUntil) {
			earliestAccount = account
		}
	}

	earliestAccount.LastUsed = now
	return earliestAccount.Token
}

// GetNextAvailableTokenWithPassword returns the next available token and password for claiming
func (p *AccountPool) GetNextAvailableTokenWithPassword() (string, string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()

	for i := 0; i < len(p.accounts); i++ {
		account := p.accounts[p.currentIndex]

		if account.InCooldown && now.Before(account.CooldownUntil) {
			p.currentIndex = (p.currentIndex + 1) % len(p.accounts)
			continue
		}

		if account.InCooldown && now.After(account.CooldownUntil) {
			account.InCooldown = false
			account.FailCount = 0
		}

		account.LastUsed = now
		token := account.Token
		password := account.Password
		p.currentIndex = (p.currentIndex + 1) % len(p.accounts)
		return token, password
	}

	earliestAccount := p.accounts[0]
	for _, account := range p.accounts {
		if account.CooldownUntil.Before(earliestAccount.CooldownUntil) {
			earliestAccount = account
		}
	}

	earliestAccount.LastUsed = now
	return earliestAccount.Token, earliestAccount.Password
}

// MarkTokenFailed marks a token as failed and puts it in cooldown
func (p *AccountPool) MarkTokenFailed(token string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, account := range p.accounts {
		if account.Token == token {
			account.FailCount++

			cooldownDuration := time.Duration(account.FailCount) * 5 * time.Minute
			account.CooldownUntil = time.Now().Add(cooldownDuration)
			account.InCooldown = true

			fmt.Printf("[ACCOUNT] Token marked as failed (fail count: %d, cooldown: %s)\n",
				account.FailCount, cooldownDuration)
			break
		}
	}
}

// MarkTokenSuccess marks a token as successful
func (p *AccountPool) MarkTokenSuccess(token string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, account := range p.accounts {
		if account.Token == token {
			account.SuccessCount++
			account.FailCount = 0
			account.InCooldown = false
			break
		}
	}
}

// ============================================================================
// WEBHOOK NOTIFICATIONS
// ============================================================================

// sendWebhookAlert sends a notification to Discord webhook with embed support
func sendWebhookAlert(webhookURL, username string, success bool, errorMsg string) {
	embed := map[string]interface{}{
		"title": "Username Claim Result",
		"color": map[bool]int{true: 0x00FF00, false: 0xFF0000}[success],
		"fields": []map[string]interface{}{
			{
				"name":  "Username",
				"value": "@" + username,
			},
			{
				"name":  "Status",
				"value": map[bool]string{true: "✅ SUCCESS", false: "❌ FAILED"}[success],
			},
		},
		"timestamp": time.Now().Format(time.RFC3339),
	}

	if !success && errorMsg != "" {
		embed["fields"] = append(embed["fields"].([]map[string]interface{}), map[string]interface{}{
			"name":  "Error",
			"value": errorMsg,
		})
	}

	payload := map[string]interface{}{
		"embeds": []map[string]interface{}{embed},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", webhookURL, bytes.NewReader(jsonData))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

func main() {
	// Interactive configuration menu
	config := interactiveConfig()

	// Validate configuration
	if err := validateConfig(&config); err != nil {
		fmt.Printf("Configuration error: %v\n", err)
		os.Exit(1)
	}

	// Determine file paths based on config
	proxyFile := ""
	tokenFile := ""
	if config.UseProxies {
		proxyFile = config.ProxyFile
		if proxyFile == "" {
			proxyFile = "data/proxies.txt"
		}
	}
	if config.UseTokens {
		tokenFile = config.TokenFile
		if tokenFile == "" {
			tokenFile = "data/tokens.txt"
		}
	}

	rotator, err := httpclient.NewRotator(proxyFile, tokenFile)
	if err != nil {
		fmt.Printf("Failed to initialize rotator: %v\n", err)
		os.Exit(1)
	}

	rotator.StartTimerManager()

	if config.UseProxies {
		fmt.Printf("Proxies: %d\n", rotator.ProxyCount())
	}
	if config.UseTokens {
		fmt.Printf("Tokens: %d\n", rotator.TokenCount())
	}

	// Create middleware configuration
	middlewareConfig := httpclient.MiddlewareConfig{
		SuccessStatusCodes: []int{200},
		RateLimitBackoff:   5 * time.Second,
		EnableAutoRotation: true,
		StopOnSuccess:      false,
	}

	middleware := httpclient.NewMiddleware(middlewareConfig, rotator)

	// Create sniper configuration
	sniperConfig := httpclient.SniperConfig{
		WorkerCount:    config.Workers,
		HTTPMethod:     config.Method,
		RequestTimeout: time.Duration(config.Timeout) * time.Second,
		Headers:        config.Headers,
		Middleware:     middleware,
		BaseURL:        config.BaseURL,
		RequestDelay:   config.RequestDelay,
		JSONPayload:    config.Payload,
	}

	sniper := httpclient.NewSniper(sniperConfig, rotator)
	sniper.SetProxyUsage(config.UseProxies && rotator.ProxyCount() > 0)

	// Generate or load targets
	if config.GenerateUsernames {
		usernames := generateRandomUsernames(config.UsernameCount, config.UsernameLength)
		sniper.SetTargets(usernames)
	} else {
		targetFile := config.TargetFile
		if targetFile == "" {
			targetFile = "data/names_to_check.txt"
		}
		if err := sniper.LoadTargets(targetFile); err != nil {
			fmt.Printf("Failed to load targets: %v\n", err)
			os.Exit(1)
		}
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Process results before starting execution to avoid race condition
	resultCount := 0
	done := make(chan bool)

	go func() {
		for range sniper.Results() {
			resultCount++
		}
		done <- true
	}()

	sniper.Execute()

	select {
	case <-done:
		fmt.Printf("Execution completed! (%d checks)\n", resultCount)
		rotator.StopTimerManager()
	case <-sigChan:
		fmt.Printf("Processed %d results before shutdown\n", resultCount)
		rotator.StopTimerManager()
		// Drain remaining results
		go func() {
			for range sniper.Results() {
			}
		}()
		time.Sleep(100 * time.Millisecond)
	}
}

// interactiveConfig provides an interactive menu for configuration
func interactiveConfig() Config {
	config := DefaultConfig()

	fmt.Println("\n=== ScoutGo Configuration ===")

	// Mode selection
	fmt.Println("\nSelect mode:")
	fmt.Println("1. Discord Username Checker")
	fmt.Println("2. Proxy Checker")
	fmt.Println("3. Claim Mode")
	fmt.Println("4. GitHub Username Checker")
	fmt.Println("5. Proxy Scraper Integration")
	fmt.Println("6. Clean & Check Proxies")
	fmt.Print("Mode (1/2/3/4/5/6): ")
	var mode string
	fmt.Scanln(&mode)

	// Proxy check mode
	if mode == "2" {
		runProxyCheck()
		os.Exit(0)
	}

	// Claim mode
	if mode == "3" {
		runClaimMode()
		os.Exit(0)
	}

	// GitHub checker mode
	if mode == "4" {
		runGitHubChecker()
		os.Exit(0)
	}

	// Proxy scraper integration mode
	if mode == "5" {
		runProxyScraperIntegration()
		os.Exit(0)
	}

	// Clean & check proxies mode (combined)
	if mode == "6" {
		runCleanAndCheckProxies()
		os.Exit(0)
	}

	// Proxy selection
	fmt.Print("Use proxies? (y/n/x): ")
	var useProxies string
	fmt.Scanln(&useProxies)

	// Check if results/working_proxies.txt exists and offer to use it
	if useProxies == "y" || useProxies == "Y" {
		if _, err := os.Stat("results/working_proxies.txt"); err == nil {
			fmt.Print("Found results/working_proxies.txt from proxy check. Use it? (y/n, default: y): ")
			var useWorking string
			fmt.Scanln(&useWorking)
			if useWorking == "" || useWorking == "y" || useWorking == "Y" {
				config.ProxyFile = "results/working_proxies.txt"
				fmt.Println("Using results/working_proxies.txt")
			}
		}
	}

	// Check for quick setup shortcut
	if useProxies == "x" || useProxies == "X" {
		config.UseProxies = true
		config.UseTokens = true
		config.GenerateUsernames = true
		config.UsernameCount = 100
		config.UsernameLength = 4
		config.StopOnSuccess = true
	} else {
		config.UseProxies = (useProxies == "y" || useProxies == "Y")

		// Token selection
		fmt.Print("Use tokens? (y/n): ")
		var useTokens string
		fmt.Scanln(&useTokens)
		config.UseTokens = (useTokens == "y" || useTokens == "Y")

		// Username generation vs file
		fmt.Print("Generate random usernames? (y/n): ")
		var generateUsernames string
		fmt.Scanln(&generateUsernames)
		config.GenerateUsernames = (generateUsernames == "y" || generateUsernames == "Y")

		if config.GenerateUsernames {
			fmt.Print("Number of usernames to generate: ")
			fmt.Scanln(&config.UsernameCount)
			fmt.Print("Username length (characters, min 2 for Discord): ")
			fmt.Scanln(&config.UsernameLength)
			if config.UsernameLength < 2 {
				config.UsernameLength = 2 // Minimum for Discord
				fmt.Println("Username length set to minimum of 2 for Discord compatibility")
			}
		}
	}

	// Worker count (always prompt for this)
	fmt.Printf("Worker count (default %d): ", config.Workers)
	var workers int
	_, err := fmt.Scanln(&workers)
	if err == nil && workers > 0 {
		config.Workers = workers
	}

	// Stop on success (only prompt if not using quick setup)
	if !(useProxies == "x" || useProxies == "X") {
		fmt.Print("Stop on first success? (y/n): ")
		var stopOnSuccess string
		fmt.Scanln(&stopOnSuccess)
		config.StopOnSuccess = (stopOnSuccess == "y" || stopOnSuccess == "Y")
	}

	fmt.Println("\n=== Configuration Summary ===")
	fmt.Printf("Proxies: %v\n", config.UseProxies)
	fmt.Printf("Tokens: %v\n", config.UseTokens)
	if config.GenerateUsernames {
		fmt.Printf("Generate %d usernames of length %d\n", config.UsernameCount, config.UsernameLength)
	} else {
		fmt.Printf("Load targets from: %s\n", config.TargetFile)
	}
	fmt.Printf("Workers: %d\n", config.Workers)
	fmt.Printf("Stop on success: %v\n", config.StopOnSuccess)
	if !config.UseProxies {
		fmt.Printf("Request delay: %d seconds (no proxy mode)\n", config.RequestDelay)
	}
	fmt.Println()

	return config
}

// runClaimMode handles the scheduled username claiming functionality
func runClaimMode() {
	fmt.Println("\n=== Discord Username Claim Mode ===")
	fmt.Println("This mode claims usernames at exact drop times from your logs")

	// Ask for authentication method
	fmt.Println("Authentication method:")
	fmt.Println("1. Use existing tokens from data/tokens.txt")
	fmt.Println("2. Login with email/password (get fresh token)")
	fmt.Print("Select method (1/2): ")
	var authMethod string
	fmt.Scanln(&authMethod)

	var tokens []string
	var passwords []string
	var webhookURL string

	// Try to load webhook URL from data/webhook.txt
	if webhookData, err := os.ReadFile("data/webhook.txt"); err == nil {
		webhookURL = strings.TrimSpace(string(webhookData))
		if webhookURL != "" {
			fmt.Printf("Loaded webhook URL from data/webhook.txt\n")
		}
	}

	if authMethod == "2" {
		fmt.Print("Enter Discord email: ")
		var email string
		fmt.Scanln(&email)

		fmt.Print("Enter Discord password: ")
		var password string
		fmt.Scanln(&password)

		fmt.Println("Logging in to Discord...")
		fmt.Println("Note: If captcha appears, you'll need to solve it manually")
		token, err := loginToDiscordWithCaptcha(email, password)
		if err != nil {
			fmt.Printf("Failed to login to Discord: %v\n", err)
			return
		}
		fmt.Printf("Successfully logged in! Token: %s...\n", token[:10])

		// Save the token to data/tokens.txt
		fmt.Print("Save this token to data/tokens.txt? (y/n): ")
		var saveChoice string
		fmt.Scanln(&saveChoice)
		if saveChoice == "y" || saveChoice == "Y" {
			err := saveToken(token)
			if err != nil {
				fmt.Printf("Failed to save token: %v\n", err)
			} else {
				fmt.Println("Token saved to data/tokens.txt")
			}
		}

		tokens = []string{token}
	} else {
		tokenFile := "data/tokens.txt"
		if _, err := os.Stat(tokenFile); os.IsNotExist(err) {
			fmt.Print("Enter token file path: ")
			fmt.Scanln(&tokenFile)
		}

		var err error
		tokens, err = loadTokens(tokenFile)
		if err != nil {
			fmt.Printf("Failed to load tokens: %v\n", err)
			return
		}

		if len(tokens) == 0 {
			fmt.Printf("No tokens found in %s\n", tokenFile)
			return
		}
		fmt.Printf("Using tokens from %s\n", tokenFile)

		// Load passwords
		passwordFile := "data/passwords.txt"
		if _, err := os.Stat(passwordFile); err == nil {
			passwords, err = loadPasswords(passwordFile)
			if err != nil {
				fmt.Printf("Failed to load passwords: %v\n", err)
				passwords = []string{}
			} else {
				fmt.Printf("Loaded %d passwords from %s\n", len(passwords), passwordFile)
			}
		} else {
			fmt.Println("No data/passwords.txt found - username changes may fail")
			passwords = []string{}
		}
	}

	var selectedToken string

	if len(tokens) == 1 {
		selectedToken = tokens[0]
		account, _ := getDiscordAccount(selectedToken)
		fmt.Printf("Using 1 token (account: %s)\n", account)
	} else {
		fmt.Printf("Found %d tokens:\n", len(tokens))
		for i, token := range tokens {
			account, err := getDiscordAccount(token)
			if err != nil {
				fmt.Printf("%d. Unknown account (token error)\n", i+1)
			} else {
				fmt.Printf("%d. %s\n", i+1, account)
			}
		}
		fmt.Printf("Select token (1-%d, or 0 for all): ", len(tokens))
		var selection int
		fmt.Scanln(&selection)

		if selection >= 1 && selection <= len(tokens) {
			selectedToken = tokens[selection-1]
			account, _ := getDiscordAccount(selectedToken)
			fmt.Printf("Using token %d (account: %s)\n", selection, account)
		} else {
			// Default to first token for automated claiming
			selectedToken = tokens[0]
			account, _ := getDiscordAccount(selectedToken)
			fmt.Printf("Using first token for automated claiming (account: %s)\n", account)
		}
	}

	fmt.Println("Claim mode:")
	fmt.Println("1. Test with custom username (immediate)")
	fmt.Println("2. Use scheduled drops from file (automated claim)")
	fmt.Print("Select mode (1/2): ")
	var claimMode string
	fmt.Scanln(&claimMode)

	var drops []UsernameDrop

	if claimMode == "1" {
		fmt.Print("Enter username to test: ")
		var testUsername string
		fmt.Scanln(&testUsername)

		drops = []UsernameDrop{
			{Username: testUsername, DropTime: time.Now().Add(1 * time.Second)},
		}
		fmt.Printf("Test mode: Will attempt to claim '%s' immediately\n", testUsername)

		// Ask for automated vs manual claiming
		fmt.Println("\nClaim method:")
		fmt.Println("1. Automated claiming (using pomelo API)")
		fmt.Println("2. Manual claiming (original method)")
		fmt.Print("Select method (1/2): ")
		var claimMethod string
		fmt.Scanln(&claimMethod)

		if claimMethod == "1" {
			// Automated claiming using pomelo API
			fmt.Println("\n=== Automated Claim Mode (Pomelo API) ===")
			fmt.Println("Using Discord's pomelo endpoint for automated username claiming")
			fmt.Println()

			// Validate token first
			fmt.Println("Validating token...")
			err := warmUpSession(selectedToken)
			if err != nil {
				fmt.Printf("Token validation failed: %v\n", err)
				return
			}
			fmt.Println("Token validated successfully!")

			// Execute automated claim
			fmt.Printf("\n=== Claiming: %s ===\n", testUsername)
			err = executePomeloClaim(testUsername, selectedToken)

			if err != nil {
				fmt.Printf("❌ Claim failed for %s: %v\n", testUsername, err)
			} else {
				fmt.Printf("✅ Successfully claimed: %s\n", testUsername)
			}
			return
		}
		// If claimMethod == "2", continue with original manual claiming logic

		if webhookURL == "" {
			fmt.Print("Enter Discord webhook URL for notifications (or press Enter to skip): ")
			fmt.Scanln(&webhookURL)
		}
	} else {
		logFile := "data/targets.txt"
		if _, err := os.Stat(logFile); os.IsNotExist(err) {
			fmt.Print("Enter log file path: ")
			fmt.Scanln(&logFile)
		}

		var err error
		drops, err = parseDiscordTimestamps(logFile)
		if err != nil {
			fmt.Printf("Failed to parse log file: %v\n", err)
			return
		}

		fmt.Printf("Parsed %d username drops from %s\n", len(drops), logFile)

		// Ask for automated vs manual claiming
		fmt.Println("\nClaim method:")
		fmt.Println("1. Automated claiming (using pomelo API)")
		fmt.Println("2. Manual claiming (original method)")
		fmt.Print("Select method (1/2): ")
		var claimMethod string
		fmt.Scanln(&claimMethod)

		if claimMethod == "1" {
			// Automated claiming using pomelo API
			fmt.Println("\n=== Automated Claim Mode (Pomelo API) ===")
			fmt.Println("Using Discord's pomelo endpoint for automated username claiming")
			fmt.Println()

			// Validate token first
			fmt.Println("Validating token...")
			err := warmUpSession(selectedToken)
			if err != nil {
				fmt.Printf("Token validation failed: %v\n", err)
				return
			}
			fmt.Println("Token validated successfully!")

			// Process drops with automated claiming
			for i, drop := range drops {
				timeUntilDrop := time.Until(drop.DropTime)

				if timeUntilDrop > 0 {
					fmt.Printf("\n=== Drop %d/%d: %s ===\n", i+1, len(drops), drop.Username)
					fmt.Printf("Scheduled time: %s\n", drop.DropTime.Format(time.RFC3339))

					// Show countdown with final alerts
					for timeUntilDrop > 0 {
						if timeUntilDrop <= 10*time.Second && timeUntilDrop > 5*time.Second {
							fmt.Printf("\r⚠️  GET READY: %-20s", formatDuration(timeUntilDrop))
						} else if timeUntilDrop <= 5*time.Second && timeUntilDrop > 1*time.Second {
							fmt.Printf("\r🚨 ALMOST TIME: %-20s", formatDuration(timeUntilDrop))
						} else if timeUntilDrop <= 1*time.Second {
							fmt.Printf("\r🔥 CLAIMING: %-20s", formatDuration(timeUntilDrop))
						} else {
							fmt.Printf("\rTime remaining: %-20s", formatDuration(timeUntilDrop))
						}

						time.Sleep(100 * time.Millisecond)
						timeUntilDrop = time.Until(drop.DropTime)
					}

					fmt.Println("\n🎯 EXECUTING AUTOMATED CLAIM 🎯")

					// Execute automated claim using pomelo API
					err := executePomeloClaim(drop.Username, selectedToken)

					if err != nil {
						fmt.Printf("❌ Claim failed for %s: %v\n", drop.Username, err)
						if webhookURL != "" {
							sendWebhookAlert(webhookURL, drop.Username, false, err.Error())
						}
					} else {
						fmt.Printf("✅ Successfully claimed: %s\n", drop.Username)
						if webhookURL != "" {
							sendWebhookAlert(webhookURL, drop.Username, true, "")
						}
					}

					// Wait before next attempt if there are more drops
					if i < len(drops)-1 {
						nextDropTime := time.Until(drops[i+1].DropTime)
						if nextDropTime > 15*time.Second {
							fmt.Println("Waiting 15 seconds before next attempt...")
							time.Sleep(15 * time.Second)
						}
					}
				} else {
					fmt.Printf("Skipping %s - drop time already passed\n", drop.Username)
				}
			}

			fmt.Println("\n=== All automated claims completed ===")
			return
		}
		// If claimMethod == "2", continue with original manual claiming logic
	}

	if webhookURL == "" {
		fmt.Print("Enter Discord webhook URL for notifications (or press Enter to skip): ")
		fmt.Scanln(&webhookURL)
	}

	fmt.Println("\n=== Claim Configuration ===")
	fmt.Println("Parallel claim attempts for same drop time: ENABLED")
	fmt.Println("Account rotation: ENABLED")
	fmt.Println("Millisecond precision: ENABLED")
	fmt.Println("Pre-authentication: ENABLED")
	fmt.Println()

	fmt.Println("\n=== Scheduling Claims ===")

	// Find the earliest drop time and wait until then
	if len(drops) > 0 {
		earliestTime := drops[0].DropTime
		timeUntilFirst := time.Until(earliestTime)

		if timeUntilFirst > 0 {
			fmt.Printf("\n=== Waiting for first drop: %s at %s ===\n", drops[0].Username, earliestTime.Format(time.RFC3339))

			// Show countdown every second
			for timeUntilFirst > 0 {
				fmt.Printf("\rTime remaining: %-20s", formatDuration(timeUntilFirst))
				time.Sleep(1 * time.Second)
				timeUntilFirst = time.Until(earliestTime)
			}
			fmt.Println() // New line after countdown completes
		}

		// Initialize account pool for rotation (only if using multiple tokens)
		var accountPool *AccountPool
		if selectedToken == "" && len(tokens) > 0 {
			accountPool = NewAccountPool(tokens, passwords)

			// Warm up sessions before claiming
			fmt.Println("\n=== Pre-authentication Phase ===")
			err := warmUpAllSessions(accountPool)
			if err != nil {
				fmt.Printf("Warning: Some sessions failed to warm up: %v\n", err)
			}
			fmt.Println()
		}

		// Process claims in order
		for i, drop := range drops {
			var token string
			var password string

			if selectedToken != "" {
				token = selectedToken
				// For single token, use first password if available
				if len(passwords) > 0 {
					password = passwords[0]
				}
			} else if accountPool != nil {
				// Use account rotation
				token, password = accountPool.GetNextAvailableTokenWithPassword()
			} else {
				// Fallback to round-robin if no pool
				token = tokens[i%len(tokens)]
				if i < len(passwords) {
					password = passwords[i]
				}
			}

			// Real-time countdown for each drop
			timeUntilDrop := time.Until(drop.DropTime)

			if timeUntilDrop > 0 {
				fmt.Printf("\n=== Next: %s ===\n", drop.Username)

				// Show countdown every second
				for timeUntilDrop > 0 {
					// Clear the line and show updated countdown
					fmt.Printf("\rTime remaining: %-20s", formatDuration(timeUntilDrop))
					time.Sleep(1 * time.Second)
					timeUntilDrop = time.Until(drop.DropTime)
				}
				fmt.Println() // New line after countdown completes
			}

			// Execute claim with millisecond precision
			err := executeClaimAtExactTime(drop.Username, drop.DropTime, token, password, webhookURL)

			// Update account pool based on result
			if selectedToken == "" && accountPool != nil {
				if err != nil {
					accountPool.MarkTokenFailed(token)
				} else {
					accountPool.MarkTokenSuccess(token)
				}
			}
		}
	}

	fmt.Println("\nClaims scheduled. Press Ctrl+C to stop.")
	fmt.Println("Waiting for drop times...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	fmt.Println("\nShutting down...")
}

// ============================================================================
// GITHUB USERNAME CHECKER
// ============================================================================

// runGitHubChecker checks GitHub username availability
func runGitHubChecker() {
	fmt.Println("\n=== GitHub Username Checker ===")
	fmt.Println("Checking GitHub username availability using GitHub API")

	// Ask for GitHub token (optional but recommended for higher rate limits)
	fmt.Print("Enter GitHub personal access token (optional, press Enter to skip): ")
	var githubToken string
	fmt.Scanln(&githubToken)

	if githubToken == "" {
		fmt.Println("Note: Without a token, you'll be limited to 60 requests/hour from a single IP")
		fmt.Println("Get a token from: https://github.com/settings/tokens")
	}

	// Username generation vs file
	fmt.Print("Generate random usernames? (y/n): ")
	var generateUsernames string
	fmt.Scanln(&generateUsernames)

	var usernames []string
	var usernameCount int
	var usernameLength int

	if generateUsernames == "y" || generateUsernames == "Y" {
		fmt.Print("Number of usernames to generate: ")
		fmt.Scanln(&usernameCount)
		fmt.Print("Username length (characters, min 1 for GitHub): ")
		fmt.Scanln(&usernameLength)
		if usernameLength < 1 {
			usernameLength = 1 // Minimum for GitHub
			fmt.Println("Username length set to minimum of 1 for GitHub compatibility")
		}
		if usernameLength > 39 {
			usernameLength = 39 // Maximum for GitHub
			fmt.Println("Username length set to maximum of 39 for GitHub compatibility")
		}

		usernames = generateRandomGitHubUsernames(usernameCount, usernameLength)
		fmt.Printf("Generated %d usernames of length %d\n", len(usernames), usernameLength)
	} else {
		// Ask for target file
		fmt.Print("Enter target file path (default: data/names_to_check.txt): ")
		var targetFile string
		fmt.Scanln(&targetFile)
		if targetFile == "" {
			targetFile = "data/names_to_check.txt"
		}

		// Load targets
		file, err := os.Open(targetFile)
		if err != nil {
			fmt.Printf("Error opening target file: %v\n", err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" && !strings.HasPrefix(line, "#") {
				usernames = append(usernames, line)
			}
		}

		if len(usernames) == 0 {
			fmt.Println("No usernames found in file")
			return
		}

		fmt.Printf("Loaded %d usernames from %s\n", len(usernames), targetFile)
	}

	// Ask for worker count
	fmt.Print("Worker count (default: 10): ")
	var workers int
	fmt.Scanln(&workers)
	if workers <= 0 {
		workers = 10
	}

	// Ask for proxies
	fmt.Print("Use proxies? (y/n): ")
	var useProxies string
	fmt.Scanln(&useProxies)
	var proxyFile string
	if useProxies == "y" || useProxies == "Y" {
		fmt.Print("Enter proxy file path (default: data/proxies.txt): ")
		fmt.Scanln(&proxyFile)
		if proxyFile == "" {
			proxyFile = "data/proxies.txt"
		}
	}

	// Initialize rotator for proxies if needed
	var rotator *httpclient.Rotator
	var err error
	if useProxies == "y" || useProxies == "Y" {
		rotator, err = httpclient.NewRotator(proxyFile, "")
		if err != nil {
			fmt.Printf("Failed to initialize rotator: %v\n", err)
			return
		}
		fmt.Printf("Loaded %d proxies\n", rotator.ProxyCount())
	}

	fmt.Println("\n=== Starting GitHub Username Check ===")

	var wg sync.WaitGroup
	var mu sync.Mutex
	available := 0
	taken := 0
	errors := 0
	semaphore := make(chan struct{}, workers)

	for _, username := range usernames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			availableCheck, err := checkGitHubUsername(name, githubToken, rotator)

			mu.Lock()
			if err != nil {
				errors++
				fmt.Printf("[ERROR] %s - %v\n", name, err)
			} else if availableCheck {
				available++
				fmt.Printf("[AVAILABLE] %s\n", name)
				// Save to results
				saveGitHubHit(name)
			} else {
				taken++
				fmt.Printf("[TAKEN] %s\n", name)
			}
			mu.Unlock()

			// Rate limiting - GitHub API allows 60 requests/hour without auth, 5000/hour with auth
			time.Sleep(1 * time.Second)
		}(username)
	}

	wg.Wait()

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Total: %d | Available: %d | Taken: %d | Errors: %d\n", len(usernames), available, taken, errors)
	fmt.Printf("Available usernames saved to results/github_hits.txt\n")
}

// checkGitHubUsername checks if a GitHub username is available
func checkGitHubUsername(username, token string, rotator *httpclient.Rotator) (bool, error) {
	// GitHub API endpoint for user lookup
	url := fmt.Sprintf("https://api.github.com/users/%s", username)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false, err
	}

	// Set headers
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "ScoutGo-UsernameChecker")
	if token != "" {
		req.Header.Set("Authorization", "token "+token)
	}

	var client *http.Client
	if rotator != nil {
		clientStr, proxy, err := rotator.GetClientWithProxy()
		if err != nil {
			return false, err
		}
		client = clientStr
		if proxy != "" {
			fmt.Printf("Using proxy: %s\n", proxy)
		}
	} else {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	// If we get 404, the username is available
	if resp.StatusCode == 404 {
		return true, nil
	}

	// If we get 200, the username is taken
	if resp.StatusCode == 200 {
		return false, nil
	}

	// Handle rate limiting
	if resp.StatusCode == 403 {
		return false, fmt.Errorf("rate limited (403 Forbidden)")
	}

	return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// saveGitHubHit saves an available GitHub username to results/github_hits.txt
func saveGitHubHit(username string) error {
	if err := os.MkdirAll("results", 0755); err != nil {
		return err
	}

	file, err := os.OpenFile("results/github_hits.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(username + "\n")
	return err
}

// generateRandomGitHubUsernames generates random GitHub usernames of specified length
func generateRandomGitHubUsernames(count, length int) []string {
	charset := "abcdefghijklmnopqrstuvwxyz0123456789-"

	usernames := make([]string, 0, count)
	attempts := 0
	maxAttempts := count * 10

	for len(usernames) < count && attempts < maxAttempts {
		attempts++

		username := make([]byte, length)
		for j := 0; j < length; j++ {
			username[j] = charset[rand.Intn(len(charset))]
		}

		usernameStr := string(username)

		if isValidGitHubUsername(usernameStr) {
			usernames = append(usernames, usernameStr)
		}
	}

	if len(usernames) < count {
		fmt.Printf("Warning: Only generated %d valid usernames out of %d requested\n", len(usernames), count)
	}

	return usernames
}

// isValidGitHubUsername validates a username against GitHub's requirements
func isValidGitHubUsername(username string) bool {
	// GitHub usernames: 1-39 characters, alphanumeric and hyphens only
	// Cannot start or end with hyphen, no consecutive hyphens
	if len(username) < 1 || len(username) > 39 {
		return false
	}

	if username[0] == '-' || username[len(username)-1] == '-' {
		return false
	}

	prevWasHyphen := false
	for _, char := range username {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '-') {
			return false
		}

		if char == '-' {
			if prevWasHyphen {
				return false
			}
			prevWasHyphen = true
		} else {
			prevWasHyphen = false
		}
	}

	return true
}

// runProxyScraperIntegration scrapes proxies from online sources and checks them
func runProxyScraperIntegration() {
	fmt.Println("\n=== Proxy Scraper Integration ===")
	fmt.Println("This mode scrapes working proxies from online sources")

	// Common proxy sources
	sources := []string{
		"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http",
		"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5",
		"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
		"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
		"https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
		"https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
	}

	fmt.Printf("Scraping proxies from %d sources...\n", len(sources))

	// Collect all proxies
	var allProxies []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, source := range sources {
		wg.Add(1)
		go func(idx int, url string) {
			defer wg.Done()
			fmt.Printf("[%d/%d] Fetching: %s\n", idx+1, len(sources), url)

			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Get(url)
			if err != nil {
				fmt.Printf("[%d/%d] Failed to fetch: %v\n", idx+1, len(sources), err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				fmt.Printf("[%d/%d] HTTP %d for %s\n", idx+1, len(sources), resp.StatusCode, url)
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("[%d/%d] Failed to read response: %v\n", idx+1, len(sources), err)
				return
			}

			// Parse proxies from response
			lines := strings.Split(string(body), "\n")
			var sourceProxies []string
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "#") {
					// Add protocol prefix if missing
					if !strings.Contains(line, "://") {
						if strings.Contains(url, "socks5") {
							line = "socks5://" + line
						} else {
							line = "http://" + line
						}
					}
					sourceProxies = append(sourceProxies, line)
				}
			}

			mu.Lock()
			allProxies = append(allProxies, sourceProxies...)
			mu.Unlock()

			fmt.Printf("[%d/%d] Found %d proxies\n", idx+1, len(sources), len(sourceProxies))
		}(i, source)
	}

	wg.Wait()

	// Remove duplicates
	uniqueProxies := make(map[string]bool)
	var deduplicatedProxies []string
	for _, proxy := range allProxies {
		if !uniqueProxies[proxy] {
			uniqueProxies[proxy] = true
			deduplicatedProxies = append(deduplicatedProxies, proxy)
		}
	}

	fmt.Printf("\nCollected %d unique proxies from all sources\n", len(deduplicatedProxies))

	if len(deduplicatedProxies) == 0 {
		fmt.Println("No proxies found from any source")
		return
	}

	// Filter out SOCKS4 proxies
	var filteredProxies []string
	for _, proxy := range deduplicatedProxies {
		if !strings.HasPrefix(proxy, "socks4://") {
			filteredProxies = append(filteredProxies, proxy)
		}
	}

	if len(filteredProxies) < len(deduplicatedProxies) {
		fmt.Printf("Filtered out %d SOCKS4 proxies\n", len(deduplicatedProxies)-len(filteredProxies))
	}

	fmt.Printf("Checking %d proxies against Discord...\n", len(filteredProxies))

	// Check proxies against Discord
	var workingProxies []string
	var checkWg sync.WaitGroup
	var checkMu sync.Mutex
	workers := 50
	semaphore := make(chan struct{}, workers)

	for _, proxy := range filteredProxies {
		checkWg.Add(1)
		go func(p string) {
			defer checkWg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if checkProxyAgainstDiscord(p) {
				checkMu.Lock()
				workingProxies = append(workingProxies, p)
				checkMu.Unlock()
				fmt.Printf("[WORKING] %s\n", p)
			}
		}(proxy)
	}

	checkWg.Wait()

	fmt.Printf("\nFound %d working proxies out of %d tested\n", len(workingProxies), len(filteredProxies))

	if len(workingProxies) == 0 {
		fmt.Println("No working proxies found")
		return
	}

	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		fmt.Printf("Error creating data directory: %v\n", err)
		return
	}

	// Save working proxies
	proxyFile := "data/proxies.txt"
	outFile, err := os.Create(proxyFile)
	if err != nil {
		fmt.Printf("Error creating proxies file: %v\n", err)
		return
	}
	defer outFile.Close()

	for _, proxy := range workingProxies {
		outFile.WriteString(proxy + "\n")
	}

	fmt.Printf("Successfully saved %d working proxies to %s\n", len(workingProxies), proxyFile)

	// Ask if user wants to run proxy checker
	fmt.Print("\nRun proxy checker on imported proxies? (y/n): ")
	var runChecker string
	fmt.Scanln(&runChecker)
	if runChecker == "y" || runChecker == "Y" {
		runProxyCheck()
	}
}

// checkProxyAgainstDiscord tests if a proxy can connect to Discord
func checkProxyAgainstDiscord(proxyStr string) bool {
	proxyURL, err := url.Parse(proxyStr)
	if err != nil {
		return false
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get("https://discord.com")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}

// runCleanAndCheckProxies combines all three proxy cleaning functions:
// 1. Simple proxy checking (general connectivity)
// 2. Remove duplicates
// 3. File-based utility with input/output
// 4. Folder support - processes all .txt files in a folder
func runCleanAndCheckProxies() {
	fmt.Println("\n=== Clean & Check Proxies ===")
	fmt.Println("This mode removes duplicates, checks connectivity, and saves cleaned proxies")
	fmt.Println("You can specify a single file or a folder containing multiple .txt files")

	fmt.Print("Enter input path (file or folder, default: data/proxies.txt): ")
	var inputPath string
	fmt.Scanln(&inputPath)
	if inputPath == "" {
		inputPath = "data/proxies.txt"
	}

	var proxies []string
	var filesProcessed int
	var isFolder bool

	// Check if it's a folder or file
	info, err := os.Stat(inputPath)
	if err != nil {
		fmt.Printf("Error accessing path: %v\n", err)
		return
	}

	isFolder = info.IsDir()

	if isFolder {
		// Process all .txt files in the folder
		fmt.Printf("Processing folder: %s\n", inputPath)
		entries, err := os.ReadDir(inputPath)
		if err != nil {
			fmt.Printf("Error reading folder: %v\n", err)
			return
		}

		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".txt") {
				filePath := inputPath + "/" + entry.Name()
				fileProxies, err := loadProxiesFromFile(filePath)
				if err != nil {
					fmt.Printf("Error reading %s: %v\n", entry.Name(), err)
					continue
				}
				proxies = append(proxies, fileProxies...)
				filesProcessed++
				fmt.Printf("Loaded %d proxies from %s\n", len(fileProxies), entry.Name())
			}
		}

		if filesProcessed == 0 {
			fmt.Println("No .txt files found in the folder")
			return
		}

		fmt.Printf("Processed %d files, total proxies: %d\n", filesProcessed, len(proxies))
	} else {
		// Process single file
		fmt.Printf("Processing file: %s\n", inputPath)
		proxies, err = loadProxiesFromFile(inputPath)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			return
		}
		filesProcessed = 1
		fmt.Printf("Loaded %d proxies from file\n", len(proxies))
	}

	if len(proxies) == 0 {
		fmt.Println("No proxies found")
		return
	}

	// Remove duplicates
	uniqueProxies := make(map[string]bool)
	var deduplicatedProxies []string
	for _, proxy := range proxies {
		if !uniqueProxies[proxy] {
			uniqueProxies[proxy] = true
			deduplicatedProxies = append(deduplicatedProxies, proxy)
		}
	}

	duplicatesRemoved := len(proxies) - len(deduplicatedProxies)
	if duplicatesRemoved > 0 {
		fmt.Printf("Removed %d duplicate proxies\n", duplicatesRemoved)
	} else {
		fmt.Println("No duplicates found")
	}

	// Check connectivity
	fmt.Printf("Checking connectivity for %d unique proxies...\n", len(deduplicatedProxies))
	var workingProxies []string
	var wg sync.WaitGroup
	var mu sync.Mutex
	workers := 50
	semaphore := make(chan struct{}, workers)

	for _, proxy := range deduplicatedProxies {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if checkProxyConnectivity(p) {
				mu.Lock()
				workingProxies = append(workingProxies, p)
				mu.Unlock()
				fmt.Printf("[WORKING] %s\n", p)
			} else {
				fmt.Printf("[FAILED] %s\n", p)
			}
		}(proxy)
	}

	wg.Wait()

	fmt.Printf("\n=== Results ===\n")
	if isFolder {
		fmt.Printf("Files processed: %d | Original: %d | Unique: %d | Working: %d | Failed: %d\n",
			filesProcessed, len(proxies), len(deduplicatedProxies), len(workingProxies), len(deduplicatedProxies)-len(workingProxies))
	} else {
		fmt.Printf("Original: %d | Unique: %d | Working: %d | Failed: %d\n",
			len(proxies), len(deduplicatedProxies), len(workingProxies), len(deduplicatedProxies)-len(workingProxies))
	}

	if len(workingProxies) == 0 {
		fmt.Println("No working proxies found")
		return
	}

	// Save cleaned file
	fmt.Print("Enter output file path (default: results/cleaned_proxies.txt): ")
	var outputFile string
	fmt.Scanln(&outputFile)
	if outputFile == "" {
		outputFile = "results/cleaned_proxies.txt"
	}

	if err := os.MkdirAll("results", 0755); err != nil {
		fmt.Printf("Error creating results directory: %v\n", err)
		return
	}

	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outFile.Close()

	for _, proxy := range workingProxies {
		outFile.WriteString(proxy + "\n")
	}

	fmt.Printf("Successfully saved %d cleaned proxies to %s\n", len(workingProxies), outputFile)

	// Optional: Copy to data/proxies.txt for immediate use
	fmt.Print("Copy cleaned proxies to data/proxies.txt for immediate use? (y/n): ")
	var copyChoice string
	fmt.Scanln(&copyChoice)
	if copyChoice == "y" || copyChoice == "Y" {
		if err := os.MkdirAll("data", 0755); err != nil {
			fmt.Printf("Error creating data directory: %v\n", err)
			return
		}

		dataFile, err := os.Create("data/proxies.txt")
		if err != nil {
			fmt.Printf("Error creating data/proxies.txt: %v\n", err)
			return
		}
		defer dataFile.Close()

		for _, proxy := range workingProxies {
			dataFile.WriteString(proxy + "\n")
		}

		fmt.Printf("Copied %d proxies to data/proxies.txt\n", len(workingProxies))
	}
}

// checkProxyConnectivity tests if a proxy can connect to the internet
func checkProxyConnectivity(proxyStr string) bool {
	proxyURL, err := url.Parse(proxyStr)
	if err != nil {
		return false
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
		Timeout: 10 * time.Second,
	}

	// Try to connect to a reliable endpoint
	resp, err := client.Get("https://www.google.com")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}

// loadProxiesFromFile loads proxies from a single file
func loadProxiesFromFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var proxies []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			proxies = append(proxies, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return proxies, nil
}

// generateRandomUsernames generates random usernames of specified length
func generateRandomUsernames(count, length int) []string {
	charset := "abcdefghijklmnopqrstuvwxyz0123456789_"

	usernames := make([]string, 0, count)
	attempts := 0
	maxAttempts := count * 10

	for len(usernames) < count && attempts < maxAttempts {
		attempts++

		username := make([]byte, length)
		for j := 0; j < length; j++ {
			username[j] = charset[rand.Intn(len(charset))]
		}

		usernameStr := string(username)

		if isValidDiscordUsername(usernameStr) {
			usernames = append(usernames, usernameStr)
		}
	}

	if len(usernames) < count {
		fmt.Printf("Warning: Only generated %d valid usernames out of %d requested\n", len(usernames), count)
	}

	return usernames
}

// isValidDiscordUsername validates a username against Discord's requirements
func isValidDiscordUsername(username string) bool {
	if len(username) < 2 || len(username) > 32 {
		return false
	}

	if username[0] == '_' || username[len(username)-1] == '_' {
		return false
	}

	prevWasUnderscore := false
	for _, char := range username {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_') {
			return false
		}

		if char == '_' {
			if prevWasUnderscore {
				return false
			}
			prevWasUnderscore = true
		} else {
			prevWasUnderscore = false
		}
	}

	return true
}

// parseDiscordTimestamps parses Discord timestamp format from log file
func parseDiscordTimestamps(filename string) ([]UsernameDrop, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var drops []UsernameDrop
	scanner := bufio.NewScanner(file)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "##") || strings.HasPrefix(line, "-#") {
			continue
		}

		usernameStart := strings.Index(line, "**")
		if usernameStart == -1 {
			continue
		}
		usernameEnd := strings.Index(line[usernameStart+2:], "**")
		if usernameEnd == -1 {
			continue
		}
		username := line[usernameStart+2 : usernameStart+2+usernameEnd]

		timestampStart := strings.Index(line, "<t:")
		if timestampStart == -1 {
			continue
		}
		timestampEnd := strings.Index(line[timestampStart+3:], ":R>")
		if timestampEnd == -1 {
			continue
		}
		timestampStr := line[timestampStart+3 : timestampStart+3+timestampEnd]

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			continue
		}

		dropTime := time.Unix(timestamp, 0).Add(30*24*time.Hour + 1*time.Hour)
		drops = append(drops, UsernameDrop{
			Username: username,
			DropTime: dropTime,
		})
	}

	fmt.Printf("Processed %d lines, found %d valid drops\n", lineCount, len(drops))

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Sort drops by time (earliest first)
	sort.Slice(drops, func(i, j int) bool {
		return drops[i].DropTime.Before(drops[j].DropTime)
	})

	return drops, nil
}

// loadTokens loads authorization tokens from a file
func loadTokens(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var tokens []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		tokens = append(tokens, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return tokens, nil
}

// saveToken saves a token to the tokens file
func saveToken(token string) error {
	file, err := os.OpenFile("data/tokens.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(token + "\n")
	return err
}

// loadPasswords loads passwords from a file
func loadPasswords(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var passwords []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		passwords = append(passwords, line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return passwords, nil
}

// loginToDiscordWithCaptcha logs in to Discord with email/password and handles captcha manually
func loginToDiscordWithCaptcha(email, password string) (string, error) {
	loginURL := "https://discord.com/api/v9/auth/login"

	payload := map[string]string{
		"login":    email,
		"password": password,
	}

	token, captchaRequired, captchaSitekey, err := attemptLogin(loginURL, payload, "")
	if err != nil {
		return "", err
	}
	if !captchaRequired {
		return token, nil
	}

	fmt.Println("\n=== CAPTCHA REQUIRED ===")
	fmt.Println("Discord requires hCaptcha verification.")
	fmt.Println("Captcha sitekey:", captchaSitekey)
	fmt.Println("\nTo solve the captcha:")
	fmt.Println("1. Open: https://accounts.hcaptcha.com/demo")
	fmt.Println("2. Enter the sitekey above")
	fmt.Println("3. Solve the captcha")
	fmt.Println("4. Copy the response token")
	fmt.Print("\nEnter captcha token (or 'cancel' to abort): ")
	var captchaSolution string
	fmt.Scanln(&captchaSolution)

	if captchaSolution == "cancel" {
		return "", fmt.Errorf("login cancelled by user")
	}

	token, captchaRequired, _, err = attemptLogin(loginURL, payload, captchaSolution)
	if err != nil {
		return "", err
	}
	if captchaRequired {
		return "", fmt.Errorf("captcha solution failed")
	}

	return token, nil
}

// attemptLogin attempts to login with optional captcha token
func attemptLogin(loginURL string, payload map[string]string, captchaToken string) (string, bool, string, error) {
	var jsonData []byte
	var err error
	if false {
		jsonData, err = json.Marshal(payload)
	} else {
		jsonData, err = json.Marshal(payload)
	}
	if err != nil {
		return "", false, "", err
	}

	req, err := http.NewRequest("POST", loginURL, bytes.NewReader(jsonData))
	if err != nil {
		return "", false, "", err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	if captchaToken != "" {
		req.Header.Set("X-Captcha-Token", captchaToken)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", false, "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == 400 {
		var errorResponse map[string]interface{}
		if err := json.Unmarshal(body, &errorResponse); err == nil {
			if captchaKey, ok := errorResponse["captcha_key"].([]interface{}); ok && len(captchaKey) > 0 {
				if captchaString, ok := captchaKey[0].(string); ok && captchaString == "captcha-required" {
					if sitekey, ok := errorResponse["captcha_sitekey"].(string); ok {
						return "", true, sitekey, nil
					}
				}
			}
		}
		return "", false, "", fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(body))
	}

	if resp.StatusCode != 200 {
		return "", false, "", fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(body))
	}

	var loginResponse map[string]interface{}
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&loginResponse); err != nil {
		return "", false, "", err
	}

	token, ok := loginResponse["token"].(string)
	if !ok {
		return "", false, "", fmt.Errorf("no token in login response")
	}

	return token, false, "", nil
}

// getDiscordAccount gets the account username for a given Discord token
func getDiscordAccount(token string) (string, error) {
	req, err := http.NewRequest("GET", "https://discord.com/api/v9/users/@me", nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var userData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&userData); err != nil {
		return "", err
	}

	username, ok := userData["username"].(string)
	if !ok {
		return "", fmt.Errorf("no username in response")
	}

	discriminator, ok := userData["discriminator"].(string)
	if !ok {
		return username, fmt.Errorf("no discriminator in response")
	}

	return fmt.Sprintf("%s#%s", username, discriminator), nil
}

// formatDuration formats a time duration in a human-readable way
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	hours := d / time.Hour
	d -= hours * time.Hour
	minutes := d / time.Minute
	d -= minutes * time.Minute
	seconds := d / time.Second

	if hours > 0 {
		return fmt.Sprintf("%02dh %02dm %02ds", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%02dm %02ds", minutes, seconds)
	}
	return fmt.Sprintf("%02ds", seconds)
}

// warmUpSession validates a token and establishes an active session
func warmUpSession(token string) error {
	req, err := http.NewRequest("GET", "https://discord.com/api/v9/users/@me", nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		if strings.Contains(string(body), "Unknown Session") || resp.StatusCode == 401 {
			return fmt.Errorf("token expired or invalid (Unknown Session)")
		}
		return fmt.Errorf("token validation failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// warmUpAllSessions validates all tokens in the pool before claiming
func warmUpAllSessions(pool *AccountPool) error {
	fmt.Println("=== Warming up sessions ===")

	pool.mu.Lock()
	defer pool.mu.Unlock()

	validCount := 0
	for i, account := range pool.accounts {
		fmt.Printf("Account %d: ", i+1)
		err := warmUpSession(account.Token)
		if err != nil {
			fmt.Printf("FAILED - %v\n", err)
			account.InCooldown = true
			account.CooldownUntil = time.Now().Add(24 * time.Hour)
		} else {
			fmt.Printf("OK\n")
			validCount++
		}
	}

	fmt.Printf("\nSummary: %d/%d accounts valid\n", validCount, len(pool.accounts))

	if validCount == 0 {
		return fmt.Errorf("no valid tokens found - all tokens are expired or invalid")
	}

	return nil
}

// executePomeloClaim attempts to claim username using Discord's pomelo API endpoint
func executePomeloClaim(username, token string) error {
	pomeloURL := "https://discord.com/api/v10/users/@me/pomelo"

	payload := map[string]string{
		"username": username,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", pomeloURL, bytes.NewReader(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case 200:
		return nil
	case 429:
		retryAfter := resp.Header.Get("Retry-After")
		if retryAfter != "" {
			waitTime, _ := strconv.Atoi(retryAfter)
			return fmt.Errorf("rate limited - retry after %d seconds", waitTime+2)
		}
		return fmt.Errorf("rate limited")
	case 400:
		return fmt.Errorf("username may be taken or blacklisted: %s", string(body))
	case 401:
		return fmt.Errorf("username is available but feature not rolled out for user")
	default:
		return fmt.Errorf("claim failed with status %d: %s", resp.StatusCode, string(body))
	}
}

// claimUsername attempts to change the authenticated user's username
func claimUsername(username, token, password string) error {
	claimURL := "https://discord.com/api/v9/users/@me"

	payload := map[string]string{
		"username": username,
		"password": password,
	}

	err := attemptClaim(claimURL, payload, token, "")
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "update your app") {
		fmt.Println("Discord requires app update detection - retrying with human-like delay...")
		time.Sleep(2 * time.Second)
		err = attemptClaim(claimURL, payload, token, "")
		if err == nil {
			return nil
		}
	}

	if strings.Contains(err.Error(), "captcha") {
		fmt.Printf("\n=== CAPTCHA REQUIRED FOR USERNAME CHANGE ===")
		fmt.Printf("Target: %s\n", username)
		fmt.Printf("Tip: Try waiting longer between claims or using different accounts\n")
		fmt.Print("Enter captcha solution (or 'skip' to skip this username): ")
		var captchaSolution string
		fmt.Scanln(&captchaSolution)

		if captchaSolution == "skip" {
			return fmt.Errorf("skipped by user")
		}

		return attemptClaim(claimURL, payload, token, captchaSolution)
	}

	return err
}

// attemptClaim attempts to claim username with optional captcha token
func attemptClaim(claimURL string, payload map[string]string, token, captchaToken string) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PATCH", claimURL, bytes.NewReader(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/channels/@me")
	req.Header.Set("Sec-Ch-Ua", "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"")
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", "\"Windows\"")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Gpc", "1")
	req.Header.Set("X-Discord-Timezone", "Australia/Sydney")
	req.Header.Set("X-Discord-Locale", "en-US")
	req.Header.Set("X-Debug-Options", "bugReporterEnabled")
	req.Header.Set("DNT", "1")

	superProperties := map[string]interface{}{
		"os":                      "Windows",
		"browser":                 "Chrome",
		"browser_user_agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"browser_version":         "120.0.0.0",
		"os_version":              "10",
		"referrer":                "",
		"referring_domain":        "",
		"referring_current_titan": "",
		"release_channel":         "stable",
		"client_build_number":     300000,
		"client_event_source":     "",
		"design_id":               0,
	}
	superPropsJSON, _ := json.Marshal(superProperties)
	req.Header.Set("X-Super-Properties", string(superPropsJSON))

	if captchaToken != "" {
		req.Header.Set("X-Captcha-Token", captchaToken)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: false,
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		if strings.Contains(string(body), "captcha") || strings.Contains(string(body), "captcha_key") {
			if strings.Contains(string(body), "update your app") {
				return fmt.Errorf("Discord requires official client - this endpoint may not work for automated claiming")
			}
			return fmt.Errorf("captcha required")
		}
		if strings.Contains(string(body), "Unknown Session") || resp.StatusCode == 10020 {
			return fmt.Errorf("token expired or invalid (Unknown Session) - refresh your tokens")
		}
		if strings.Contains(string(body), "Password does not match") {
			return fmt.Errorf("incorrect password - check passwords.txt")
		}
		return fmt.Errorf("claim failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// executeClaimAtExactTime executes a claim with millisecond precision
func executeClaimAtExactTime(username string, dropTime time.Time, token, password, webhookURL string) error {
	timeUntilDrop := time.Until(dropTime)

	if timeUntilDrop > 0 {
		if timeUntilDrop > 10*time.Millisecond {
			time.Sleep(timeUntilDrop - 10*time.Millisecond)
		}

		for time.Until(dropTime) > 0 {
		}
	}

	fmt.Printf("\n[CLAIM ATTEMPT] %s at %s\n", username, time.Now().Format(time.RFC3339Nano))
	err := claimUsername(username, token, password)

	if err != nil {
		fmt.Printf("[CLAIM FAILED] %s - %v\n", username, err)
		if webhookURL != "" {
			sendWebhookAlert(webhookURL, username, false, err.Error())
		}
	} else {
		fmt.Printf("[CLAIM SUCCESS] Successfully claimed: %s\n", username)
		if webhookURL != "" {
			sendWebhookAlert(webhookURL, username, true, "")
		}
	}

	return err
}
