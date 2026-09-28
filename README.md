# ScoutGo

A high-performance Discord username availability checker written in Go. Built for speed with concurrent execution, proxy rotation, and intelligent rate-limit handling.

---

## Features

- **High-Concurrency**: Worker pool pattern with goroutines for maximum throughput (2000 workers by default)
- **Proxy Rotation**: Supports HTTP/HTTPS/SOCKS5/SOCKS4 with automatic failover
- **Token Rotation**: Authorization token rotation for authenticated requests
- **Rate Limit Defense**: Automatic backoff on 429 responses with `Retry-After` header parsing
- **Smart Proxy Management**: Health tracking, cooldown tracking, and automatic proxy removal
- **Discord Validation**: Built-in username validation (2-32 chars, alphanumeric + underscores)
- **Clean Output**: Color-coded terminal output with real-time statistics
- **Username Generation**: Random username generation with Discord validation
- **Organized Structure**: Uses `data/` folder for config and inputs, `results/` for hits
- **Auto-Save Hits**: Available usernames automatically saved to `results/hits.txt`

---

## Installation

### Prerequisites
- Go 1.21 or higher

### Build from Source
```bash
git clone https://github.com/x7va/discord-ScoutGo-go-x7
cd discord-ScoutGo-go-x7
go build -o ScoutGo main.go
```

**By @x7va**

### Run Directly
```bash
go run main.go
```

---

## Quick Start

### Interactive Mode
```bash
./ScoutGo
# or
go run main.go
```

The program will prompt you for:
- Mode (Username Checker / Proxy Checker / Claim Mode)
- Proxy usage (y/n)
- Token usage (y/n)
- Username generation vs file loading
- Worker count
- Stop on first success

### Quick Setup Shortcut
Type `x` for instant setup:
- Proxies: yes
- Tokens: yes
- Generate usernames: yes
- Count: 100
- Length: 4
- Stop on success: yes

---

## Configuration

### Input Files

**data/proxies.txt** - One proxy per line:
```
http://proxy1.example.com:8080
https://proxy2.example.com:8443
socks5://proxy3.example.com:1080
socks4://proxy4.example.com:1080
```

**data/tokens.txt** - One token per line:
```
Bearer token1_here
Bearer token2_here
```

**data/passwords.txt** - One password per line (REQUIRED for username changes):
```
password_for_account1
password_for_account2
```

⚠️ **IMPORTANT**: Discord requires the account password for username changes. Each password in `passwords.txt` should correspond to the token at the same line number in `tokens.txt`. Without passwords, claim attempts will fail with "Password does not match" error.

**data/names_to_check.txt** - One username per line:
```
username1
username2
username3
```

**data/config.json** - Configuration file:
```json
{
  "debug": false,
  "workers": 2000,
  "timeout": 10,
  "auto_remove_dead_proxies": true,
  "reuse_proxies": true,
  "remove_proxies": true,
  "use_proxies": true,
  "proxy_file": "data/proxies.txt",
  "use_tokens": false,
  "token_file": "data/tokens.txt",
  "generate_usernames": true,
  "username_count": 100,
  "username_length": 4,
  "target_file": "data/names_to_check.txt",
  "base_url": "https://discord.com/api/v9/unique-username/username-attempt-unauthed",
  "include_numbers": true,
  "include_special": false,
  "letters_only": false,
  "avoid_repeated": false
}
```

### Output Files

**results/hits.txt** - Available usernames found during checking
**logs/** - Application logs

---

## Output Format

The tool displays results in real-time:

```
Available username, RPS : 18 / s, resp : {'taken': False}, proxy : proxy.example.com:8080
Taken username2, RPS : 18 / s, resp : {'taken': True}, proxy : proxy.example.com:8080
[RATELIMIT] Rate limited on @username3 - back in 5s (proxy: proxy.example.com:8080)
[ERROR] Proxy error: connection refused
```

**Color Coding:**
- 🟢 Green: Available usernames (saved to results/hits.txt)
- 🔴 Red: Taken usernames
- 🟡 Yellow: Rate limits
- 🔴 Red: Errors

---

## Architecture

```
├── main.go              # Entry point & interactive configuration
├── data/                # Input files
│   ├── config.json      # Configuration file
│   ├── proxies.txt       # Proxy list
│   ├── names_to_check.txt  # Target usernames
│   ├── customlist.txt  # Custom username list
│   ├── tokens.txt       # Authorization tokens
│   ├── passwords.txt    # Account passwords for claiming
│   ├── targets.txt      # Target usernames for drops
│   └── webhook.txt      # Discord webhook URL
├── generator/
│   └── generator.go     # Username generation
├── cmd/
│   └── generator/
│       └── main.go     # Generator CLI
├── httpclient/
│   ├── transport.go    # Optimized HTTP transport
│   ├── rotator.go      # Proxy/token rotation
│   ├── sniper.go       # Core execution engine
│   ├── middleware.go   # Response processing
│   └── ui.go           # Terminal UI
├── results/
│   ├── hits.txt        # Available usernames found
│   ├── working_proxies.txt  # Working proxies from proxy check
│   └── github_hits.txt # Available GitHub usernames
└── logs/               # Application logs
```

---

## Performance Tuning

### High Speed
```bash
# Uses 2000 workers with good proxy pool
./ScoutGo
# Select: proxies=y, tokens=y, workers=2000
```

### Conservative
```bash
# Use fewer workers for reliability
./ScoutGo
# Select: proxies=y, tokens=y, workers=5, timeout=60
```

### Without Proxies
```bash
# Direct connection with rate limiting
./ScoutGo
# Select: proxies=n, workers=10
```

---

## Rate Limit Handling

The system automatically handles rate limits by:
1. Detecting HTTP 429 responses
2. Parsing `Retry-After` headers
3. Applying backoff delays
4. Rotating to fresh proxy/token pairs
5. Tracking proxy cooldowns
6. Skipping rate-limited proxies during cooldown

---

## Proxy Management

### Proxy Health
- Proxies that fail 3+ times are automatically removed
- Rate-limited proxies are tracked in cooldown
- Cooldowned proxies are skipped until available

### Proxy Rotation
- Round-robin distribution
- Skips failed and cooldowned proxies
- HTTP clients cached per proxy for connection reuse

---

## Modes

### 1. Discord Username Checker
Check Discord usernames for availability using the unauthenticated endpoint.

### 2. Proxy Checker
Test proxy connectivity to Discord. Saves working proxies to `results/working_proxies.txt`.

### 3. Claim Mode
Claim usernames at scheduled drop times using authenticated requests.

### 4. GitHub Username Checker
Check GitHub username availability using GitHub API.

### 5. Proxy Scraper Integration
Automatically scrape working proxies from multiple sources and import them directly into ScoutGo. This mode integrates with the `proxy-scraper-checker` tool to fetch and verify HTTP/SOCKS4/SOCKS5 proxies from various sources, then automatically imports the working proxies into `data/proxies.txt`.

**Requirements:**
- Proxy scraper must be installed at `/Users/san0031/Desktop/home/proxy scraper`
- Rust toolchain must be available to run the scraper

**Features:**
- Scrapes proxies from multiple online sources
- Verifies each proxy actually works
- Includes response time, geolocation, and network ownership data
- Automatically imports working proxies to ScoutGo
- Optional: Run proxy checker on imported proxies

**Usage:**
1. Select mode 5 from the main menu
2. The proxy scraper will automatically run and collect working proxies
3. Verified proxies are automatically imported to `data/proxies.txt`
4. Optionally run the proxy checker to verify Discord connectivity

---

## Safety Features

- Thread-safe operations throughout
- Graceful shutdown on Ctrl+C
- Configurable error thresholds
- Automatic connection cleanup
- Secure token truncation in logs
- Discord username validation
- Auto-save of available usernames

---

## Troubleshooting

### "go: command not found"
Install Go from https://golang.org/dl/

### Connection errors
- Check proxy file format
- Verify network connectivity
- Ensure proxies are operational

### Rate limiting
- Increase timeout value
- Add more proxies
- Reduce worker count

### High memory usage
- Reduce worker count
- Check for connection leaks

---

## Contributing

Contributions welcome! Please:
- Follow Go best practices
- Maintain thread-safety
- Add tests for new features
- Update documentation

---

## License

This project is provided as-is for educational and research purposes.

---

## Disclaimer

This tool is designed for legitimate testing and research purposes only. Users are responsible for ensuring compliance with applicable laws, terms of service, and ethical guidelines.
