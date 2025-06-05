#!/usr/bin/env python3
"""Format benchmark comparison output with visual indicators for GitHub markdown."""

import re
import sys


def format_benchmark_output(content):
    """Add visual formatting to benchmark comparison output."""
    lines = content.split("\n")
    formatted_lines = []

    for line in lines:
        # Skip empty lines and headers
        if not line.strip() or line.startswith("|") and "---" in line:
            formatted_lines.append(line)
            continue

        # Process benchmark result lines
        if "|" in line and ("faster" in line or "slower" in line):
            # Extract the speed factor (e.g., "1.23x faster" or "1.10x slower")
            speed_match = re.search(r"(\d+\.\d+)x\s+(faster|slower)", line)
            if speed_match:
                factor = float(speed_match.group(1))
                direction = speed_match.group(2)

                # Add visual indicators based on performance
                if direction == "faster":
                    # Green indicator for faster
                    if factor >= 2.0:
                        indicator = "🟢🟢"  # Double green for 2x+ faster
                    elif factor >= 1.1:
                        indicator = "🟢"  # Single green for 1.1x+ faster
                    else:
                        indicator = "⚪"  # White for marginal improvement
                    formatted_text = f"{indicator} **{speed_match.group(0)}**"
                else:
                    # Red indicator for slower
                    if factor >= 2.0:
                        indicator = "🔴🔴"  # Double red for 2x+ slower
                    elif factor >= 1.1:
                        indicator = "🔴"  # Single red for 1.1x+ slower
                    else:
                        indicator = "⚪"  # White for marginal slowdown
                    formatted_text = f"{indicator} **{speed_match.group(0)}**"

                # Replace the original text with formatted version
                line = line.replace(speed_match.group(0), formatted_text)
            elif "not significant" in line:
                # Add neutral indicator for non-significant changes
                line = re.sub(r"not significant", "⚪ not significant", line)

        formatted_lines.append(line)

    return "\n".join(formatted_lines)


def main():
    if len(sys.argv) != 2:
        print("Usage: python format_benchmark.py <input_file>")
        sys.exit(1)

    input_file = sys.argv[1]

    try:
        with open(input_file, "r") as f:
            content = f.read()

        formatted = format_benchmark_output(content)
        print(formatted)

    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
