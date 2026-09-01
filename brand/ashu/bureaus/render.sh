#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 generate.py
mkdir -p png
for f in *.svg; do
  rsvg-convert -u -d 144 -p 144 "$f" -o "png/${f%.svg}.png"
done
# readable cards + catalog
for f in *-card.svg; do
  rsvg-convert -u -w 720 -h 920 "$f" -o "png/${f%.svg}.png"
done
rsvg-convert -u -w 1280 -h 3480 00-catalog.svg -o png/00-catalog.png
for f in *-avatar.svg; do
  rsvg-convert -u -w 512 -h 512 "$f" -o "png/${f%.svg}.png"
done
echo done
ls png | wc -l
