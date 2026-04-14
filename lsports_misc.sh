#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

./parselsports.py leagues
sleep 5
./parselsports.py locations
sleep 5
./parselsports.py sports
date >> /tmp/lsports_misc.run
