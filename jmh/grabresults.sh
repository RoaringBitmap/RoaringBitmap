#!/bin/bash
echo "Parsing results from log file " $1
grep -A 10000 "# Run complete." $1 | awk '{print $2 " "  $1 " " $5}' | sort  |rev | column -t |rev
