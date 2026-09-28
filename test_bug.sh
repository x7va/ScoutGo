#!/bin/bash

echo "=== BUG TEST ==="
echo "Testing scoutgo with various configurations..."

# Test 1: No proxies, no tokens, generate usernames
echo -e "\n--- Test 1: No proxies, no tokens, generate usernames ---"
(echo "1"; echo "n"; echo "n"; echo "y"; echo "5"; echo "n"; echo "") | ./scoutgo &
PID=$!
sleep 3
kill $PID 2>/dev/null
echo "Test 1 completed"

# Test 2: With proxies, no tokens, load from file
echo -e "\n--- Test 2: With proxies, no tokens, load from file ---"
(echo "1"; echo "y"; echo "n"; echo "n"; echo "5"; echo "n"; echo "") | ./scoutgo &
PID=$!
sleep 3
kill $PID 2>/dev/null
echo "Test 2 completed"

# Test 3: Proxy checker mode (limited to 10 proxies)
echo -e "\n--- Test 3: Proxy checker mode (limited) ---"
(echo "2"; echo "data/proxies.txt"; echo "n"; echo "") | ./scoutgo &
PID=$!
sleep 5
kill $PID 2>/dev/null
echo "Test 3 completed"

echo -e "\n=== BUG TEST COMPLETE ==="
