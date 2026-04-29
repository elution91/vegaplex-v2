#!/bin/bash

# EarningsEdgeDetection CLI Scanner Runner
# Usage: 
#   ./run.sh                     - Run scanner with current date
#   ./run.sh MM/DD/YYYY          - Run scanner with specified date
#   ./run.sh -l                  - Run with list format
#   ./run.sh -i                  - Run with iron fly calculations
#   ./run.sh -a TICKER           - Analyze a specific ticker
#   ./run.sh -a TICKER -i        - Analyze ticker with iron fly strategy

# Make sure dependencies are installed (pip install -r requirements.txt)

# Get number of available cores for parallel processing
NUM_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Use half the available cores (minimum 2, maximum 6 for stability)
WORKERS=$(( NUM_CORES / 2 ))
if (( WORKERS < 2 )); then
  WORKERS=2
elif (( WORKERS > 6 )); then
  WORKERS=6
fi

# Check for flags
LIST_FLAG=""
IRONFLY_FLAG=""
ANALYZE_FLAG=""
FINNHUB_FLAG=""
DOLTHUB_FLAG=""
COMBINED_FLAG=""
if [[ "$*" == *"-l"* || "$*" == *"--list"* ]]; then
  LIST_FLAG="--list"
fi
if [[ "$*" == *"-i"* || "$*" == *"--iron-fly"* ]]; then
  IRONFLY_FLAG="--iron-fly"
fi
if [[ "$*" == *"-f"* || "$*" == *"--use-finnhub"* ]]; then
  FINNHUB_FLAG="--use-finnhub"
fi
if [[ "$*" == *"-u"* || "$*" == *"--use-dolthub"* ]]; then
  DOLTHUB_FLAG="--use-dolthub"
fi
if [[ "$*" == *"-c"* || "$*" == *"--all-sources"* ]]; then
  COMBINED_FLAG="--all-sources"
fi

# Check for analyze flag and extract ticker if present
for i in "$@"; do
  if [[ "$i" == "-a" || "$i" == "--analyze" ]]; then
    ANALYZE_MODE=true
  elif [[ "$ANALYZE_MODE" == true && ! "$i" == -* ]]; then
    ANALYZE_TICKER="$i"
    ANALYZE_FLAG="--analyze $ANALYZE_TICKER"
    ANALYZE_MODE=false
  fi
done

# Handle analyze mode specifically
if [[ ! -z "$ANALYZE_TICKER" ]]; then
  echo "Analyzing ticker: $ANALYZE_TICKER"
  python3 scanner.py $ANALYZE_FLAG $IRONFLY_FLAG $FINNHUB_FLAG $DOLTHUB_FLAG $COMBINED_FLAG
  exit 0
fi

# Run the scanner with parallel processing
if [ -z "$1" ]; then
  # No arguments - Run with current date
  python3 scanner.py --parallel $WORKERS $LIST_FLAG $IRONFLY_FLAG $FINNHUB_FLAG $DOLTHUB_FLAG $COMBINED_FLAG
elif [[ "$1" == "-l" || "$1" == "--list" || "$1" == "-i" || "$1" == "--iron-fly" || "$1" == "-f" || "$1" == "--use-finnhub" || "$1" == "-u" || "$1" == "--use-dolthub" || "$1" == "-c" || "$1" == "--all-sources" ]]; then
  # Handle flags when they appear as the first argument
  if [[ "$1" == "-l" || "$1" == "--list" ]]; then
    LIST_FLAG="--list"
  elif [[ "$1" == "-i" || "$1" == "--iron-fly" ]]; then
    IRONFLY_FLAG="--iron-fly"
  elif [[ "$1" == "-f" || "$1" == "--use-finnhub" ]]; then
    FINNHUB_FLAG="--use-finnhub"
  elif [[ "$1" == "-u" || "$1" == "--use-dolthub" ]]; then
    DOLTHUB_FLAG="--use-dolthub"
  elif [[ "$1" == "-c" || "$1" == "--all-sources" ]]; then
    COMBINED_FLAG="--all-sources"
  fi
  
  # Check for additional flags
  for arg in "$@"; do
    if [[ "$arg" == "-l" || "$arg" == "--list" ]]; then
      LIST_FLAG="--list"
    elif [[ "$arg" == "-i" || "$arg" == "--iron-fly" ]]; then
      IRONFLY_FLAG="--iron-fly"
    elif [[ "$arg" == "-f" || "$arg" == "--use-finnhub" ]]; then
      FINNHUB_FLAG="--use-finnhub"
    elif [[ "$arg" == "-u" || "$arg" == "--use-dolthub" ]]; then
      DOLTHUB_FLAG="--use-dolthub"
    elif [[ "$arg" == "-c" || "$arg" == "--all-sources" ]]; then
      COMBINED_FLAG="--all-sources"
    fi
  done
  
  python3 scanner.py --parallel $WORKERS $LIST_FLAG $IRONFLY_FLAG $FINNHUB_FLAG $DOLTHUB_FLAG $COMBINED_FLAG
else
  # Date is provided as first argument
  python3 scanner.py --date "$1" --parallel $WORKERS $LIST_FLAG $IRONFLY_FLAG $FINNHUB_FLAG $DOLTHUB_FLAG $COMBINED_FLAG
fi
