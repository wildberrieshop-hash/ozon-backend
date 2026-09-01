#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p png
for f in *.svg; do
  out="png/${f%.svg}.png"
  # Hi-res: 4x viewBox, cap long edge ~2400 for the board
  rsvg-convert -u -d 192 -p 192 "$f" -o "$out"
  echo "$out"
done
# Extra large board and avatars for review
rsvg-convert -u -w 1024 -h 1024 ashu-avatar.svg -o png/ashu-avatar.png
rsvg-convert -u -w 1024 -h 1024 ashu-badge.svg -o png/ashu-badge.png
rsvg-convert -u -w 800 -h 800 ashu-mark-color.svg -o png/ashu-mark-color.png
rsvg-convert -u -w 800 -h 800 ashu-mark-cream.svg -o png/ashu-mark-cream.png
rsvg-convert -u -w 800 -h 800 ashu-mark-chip.svg -o png/ashu-mark-chip.png
rsvg-convert -u -w 1260 -h 480 ashu-lockup-color.svg -o png/ashu-lockup-color.png
rsvg-convert -u -w 800 -h 840 ashu-stacked.svg -o png/ashu-stacked.png
rsvg-convert -u -w 2400 -h 640 ashu-banner.svg -o png/ashu-banner.png
rsvg-convert -u -w 1200 -h 2060 ashu-board.svg -o png/ashu-board.png
echo done
