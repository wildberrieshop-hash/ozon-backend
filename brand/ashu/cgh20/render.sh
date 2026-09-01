#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 generate.py
mkdir -p png
for f in *.svg; do
  rsvg-convert -u -d 144 -p 144 "$f" -o "png/${f%.svg}.png"
done
rsvg-convert -u -w 1140 -h 1680 00-catalog.svg -o png/00-catalog.png
rsvg-convert -u -w 1640 -h 1480 00-size-test.svg -o png/00-size-test.png
for f in *-card.svg; do
  rsvg-convert -u -w 720 -h 900 "$f" -o "png/${f%.svg}.png"
done
for f in *-avatar.svg; do
  rsvg-convert -u -w 512 -h 512 "$f" -o "png/${f%.svg}.png"
done
echo done "$(ls png | wc -l)" files
