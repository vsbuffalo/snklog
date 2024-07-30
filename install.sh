#!/bin/bash

# Create ~/.local/bin if it doesn't exist
mkdir -p ~/.local/bin

# Download the script
curl -sSL https://raw.githubusercontent.com/vsbuffalo/snklog/main/snklog/main.py -o ~/.local/bin/snklog

# Make it executable
chmod +x ~/.local/bin/snklog

echo "snklog has been installed to ~/.local/bin/snklog"
echo "Make sure ~/.local/bin is in your PATH."
