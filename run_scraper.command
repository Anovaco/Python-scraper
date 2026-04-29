#!/bin/bash
cd "$(dirname "$0")"
echo "Installing required packages..."
pip3 install requests beautifulsoup4 openpyxl lxml tqdm selenium webdriver-manager python-whois --quiet
echo ""
echo "Starting scraper v6..."
python3 scraper.py
read -p "Press Enter to close..."
