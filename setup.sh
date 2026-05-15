#!/usr/bin/env bash
set -e

# Install system deps on Ubuntu/Debian (skipped on macOS)
if command -v apt-get &>/dev/null; then
    sudo apt-get update -qq
    sudo apt-get install -y python3 python3-pip python3-venv python3-dev \
        libssl-dev libffi-dev ca-certificates
fi

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip -q
pip install -r scraperone/requirements.txt

echo ""
echo "Done. Activate the venv with: source .venv/bin/activate"
