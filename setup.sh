#!/bin/bash
# ──────────────────────────────────────────────────
# ETF Lookup Tool — Setup
# ──────────────────────────────────────────────────
#
# QUICK START:
#   1. Install dependencies:
#        pip install requests beautifulsoup4
#
#   2. Make the script executable:
#        chmod +x etf_lookup.py
#
#   3. Add an alias so you can type 'etf KCE' from anywhere.
#      Run ONE of these depending on your shell:
#
#      For Bash (~/.bashrc):
#        echo 'alias etf="python3 ~/etf_lookup.py"' >> ~/.bashrc
#        source ~/.bashrc
#
#      For Zsh (~/.zshrc):
#        echo 'alias etf="python3 ~/etf_lookup.py"' >> ~/.zshrc
#        source ~/.zshrc
#
#   4. Usage:
#        etf KCE
#        etf SPY
#        etf QQQ
#
# ──────────────────────────────────────────────────
# Or just run it directly:
#   python3 etf_lookup.py KCE
# ──────────────────────────────────────────────────

# Auto-setup: run this script to install deps + create alias
echo "Installing dependencies..."
pip install requests beautifulsoup4 2>/dev/null || pip3 install requests beautifulsoup4

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/etf_lookup.py"
chmod +x "$SCRIPT_PATH"

# Detect shell config file
if [ -n "$ZSH_VERSION" ] || [ "$SHELL" = "/bin/zsh" ]; then
    RC_FILE="$HOME/.zshrc"
else
    RC_FILE="$HOME/.bashrc"
fi

# Add alias if not already present
if ! grep -q 'alias etf=' "$RC_FILE" 2>/dev/null; then
    echo "" >> "$RC_FILE"
    echo "# ETF Lookup Tool" >> "$RC_FILE"
    echo "alias etf=\"python3 $SCRIPT_PATH\"" >> "$RC_FILE"
    echo "✓ Added 'etf' alias to $RC_FILE"
    echo "  Run: source $RC_FILE   (or open a new terminal)"
else
    echo "✓ Alias 'etf' already exists in $RC_FILE"
fi

echo ""
echo "Done! Try it:  etf KCE"
