#!/usr/bin/env python3
"""20 ASHU marks in the language of Chermayeff & Geismar & Haviv.

One idea each. Fat geometry. Must hold at 32px marketplace icon.
"""

from __future__ import annotations

import math
from pathlib import Path

OUT = Path(__file__).resolve().parent
FONT = "Inter, DejaVu Sans, sans-serif"

BLUE = "#0050A0"
RED = "#E31C23"
INK = "#111111"
PAPER = "#F3F5F8"
WHITE = "#FFFFFF"
CREAM = "#F7F4EE"


def svg(w, h, body, bg=None):
    bg_rect = f'<rect width="100%" height="100%" fill="{bg}"/>' if bg else ""
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}">
{bg_rect}{body}
</svg>
'''


def xml(t: str) -> str:
    return t.replace("&", "&amp;").replace("<", "&lt;")


def hexagon(cx, cy, r):
    pts = []
    for i in range(6):
        a = math.radians(-90 + i * 60)
        pts.append(f"{cx + r * math.cos(a):.2f},{cy + r * math.sin(a):.2f}")
    return " ".join(pts)


def regular(n, cx, cy, r, rot=-90):
    pts = []
    for i in range(n):
        a = math.radians(rot + i * 360 / n)
        pts.append(f"{cx + r * math.cos(a):.2f},{cy + r * math.sin(a):.2f}")
    return " ".join(pts)


def square(cx, cy, size, rot=0):
    return regular(4, cx, cy, size * math.sqrt(2) / 2, rot - 45)


# =====================================================================
# Twenty marks. Each returns SVG for a 200×200 box. Fat. One silhouette.
# =====================================================================

def m01_delta(fill=BLUE):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 12 L 188 188 H 12 Z
           M 100 88 A 30 30 0 1 0 100 148 A 30 30 0 1 0 100 88 Z"/>
'''


def m02_octagon(fill=BLUE):
    # Regular octagon (Chase), not the star you get from union of two squares.
    return f'<polygon fill="{fill}" points="{regular(8, 100, 100, 86, -22.5)}"/>'


def m03_bars(fill=BLUE):
    return f'''
  <g fill="{fill}">
    <polygon points="46,176 70,176 114,28 90,28"/>
    <polygon points="154,176 130,176 86,28 110,28"/>
    <rect x="58" y="108" width="84" height="24"/>
  </g>
'''


def m04_hex(fill=BLUE):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 12 L 176 56 L 176 144 L 100 188 L 24 144 L 24 56 Z
           M 100 78 A 28 28 0 1 0 100 134 A 28 28 0 1 0 100 78 Z"/>
'''


def m05_ring_wedge(fill=RED):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 14 A 86 86 0 1 1 99.99 14 Z
           M 100 50 A 50 50 0 1 0 100 150 A 50 50 0 1 0 100 50 Z
           M 100 100 L 168 48 L 176 72 Z"/>
'''


def m06_house(fill=BLUE):
    return f'<polygon fill="{fill}" points="100,16 184,88 184,188 16,188 16,88"/>'


def m07_chevron(fill=BLUE):
    return f'<path fill="{fill}" d="M 100 20 L 184 132 H 148 L 100 72 L 52 132 H 16 Z"/>'


def m08_bolt(fill=RED):
    return f'<polygon fill="{fill}" points="118,16 52,108 92,108 70,188 160,84 112,84"/>'


def m09_screen(fill=INK):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 24 44 H 176 V 156 H 24 Z
           M 84 74 L 140 100 L 84 126 Z"/>
'''


def m10_socket(fill=BLUE):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 16 A 84 84 0 1 1 99.99 16 Z
           M 70 88 A 18 18 0 1 0 70 124 A 18 18 0 1 0 70 88 Z
           M 130 88 A 18 18 0 1 0 130 124 A 18 18 0 1 0 130 88 Z"/>
'''


def m11_overlap(fill=None):
    return f'''
  <rect x="24" y="24" width="112" height="112" fill="{BLUE}"/>
  <rect x="64" y="64" width="112" height="112" fill="{RED}"/>
'''


def m12_diamond(fill=BLUE):
    return f'''
  <g transform="translate(100 100) rotate(45)">
    <path fill="{fill}" fill-rule="evenodd"
          d="M -62 -62 H 62 V 62 H -62 Z M -40 -12 H 40 V 12 H -40 Z"/>
  </g>
'''


def m13_fan(fill=None):
    parts = []
    for i, col in enumerate((RED, "#F47B20", BLUE)):
        start = -90 - 42 + i * 42
        a0 = math.radians(start)
        a1 = math.radians(start + 36)
        x0, y0 = 100 + 86 * math.cos(a0), 100 + 86 * math.sin(a0)
        x1, y1 = 100 + 86 * math.cos(a1), 100 + 86 * math.sin(a1)
        parts.append(
            f'<path fill="{col}" d="M 100 108 L {x0:.1f} {y0:.1f} A 86 86 0 0 1 {x1:.1f} {y1:.1f} Z"/>'
        )
    return "\n".join(parts)


def m14_stencil(fill=BLUE):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 10 A 90 90 0 1 1 99.99 10 Z
           M 100 42 L 142 150 H 124 L 116 128 H 84 L 76 150 H 58 Z
           M 90 110 H 110 L 100 82 Z"/>
'''


def m15_pixel(fill=BLUE):
    cells = [
        (2, 0), (1, 1), (3, 1), (1, 2), (2, 2), (3, 2),
        (0, 3), (4, 3), (0, 4), (4, 4),
    ]
    s, g, x0, y0 = 28, 4, 30, 26
    return "\n".join(
        f'<rect x="{x0 + c * (s + g)}" y="{y0 + r * (s + g)}" width="{s}" height="{s}" fill="{fill}"/>'
        for c, r in cells
    )


def m16_switch(fill=INK):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 16
           A 52 52 0 0 1 152 68 V 132
           A 52 52 0 0 1 48 132 V 68
           A 52 52 0 0 1 100 16 Z
           M 86 40 H 114 V 92 H 86 Z"/>
'''


def m17_signal(fill=BLUE):
    return f'''
  <g fill="{fill}">
    <rect x="36" y="132" width="36" height="48" rx="4"/>
    <rect x="82" y="84" width="36" height="96" rx="4"/>
    <rect x="128" y="36" width="36" height="144" rx="4"/>
  </g>
'''


def m18_arrow(fill=RED):
    return f'<path fill="{fill}" d="M 100 16 L 184 104 H 140 V 188 H 60 V 104 H 16 Z"/>'


def m19_window(fill=BLUE):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 16 L 184 78 H 172 V 188 H 28 V 78 H 16 Z
           M 93 90 H 107 V 176 H 93 Z
           M 40 125 H 160 V 139 H 40 Z"/>
'''


def m20_letter(fill=RED):
    return f'''
  <path fill="{fill}" fill-rule="evenodd"
        d="M 100 8 L 192 192 H 148 L 132 152 H 68 L 52 192 H 8 Z
           M 84 120 H 116 L 100 76 Z"/>
'''


MARKS = [
    ("01-delta", "Delta", "Треугольник и круг. A + питание.", m01_delta, BLUE, PAPER),
    ("02-octagon", "Octagon", "Конструкция Chase: два квадрата.", m02_octagon, BLUE, PAPER),
    ("03-bars", "Bars", "Три одинаковых бруска = A.", m03_bars, BLUE, PAPER),
    ("04-hex", "Hex", "Шестигранник-чип и отверстие.", m04_hex, BLUE, PAPER),
    ("05-wedge", "Wedge", "Кольцо с клином. Громкость / ток.", m05_ring_wedge, RED, CREAM),
    ("06-house", "House", "Дом магазина. Силуэт A.", m06_house, BLUE, PAPER),
    ("07-chevron", "Chevron", "Одна галка. Стрелка и A.", m07_chevron, BLUE, PAPER),
    ("08-bolt", "Bolt", "Молния из четырёх плоскостей.", m08_bolt, RED, CREAM),
    ("09-screen", "Screen", "Экран. Play вырезан.", m09_screen, INK, PAPER),
    ("10-socket", "Socket", "Круг и два отверстия. Розетка.", m10_socket, BLUE, PAPER),
    ("11-overlap", "Overlap", "Два квадрата. Синий и красный.", m11_overlap, INK, PAPER),
    ("12-diamond", "Diamond", "Ромб и перекладина.", m12_diamond, BLUE, PAPER),
    ("13-fan", "Fan", "Три сегмента. Сигнал, школа NBC.", m13_fan, INK, CREAM),
    ("14-stencil", "Stencil", "Круг, A выбита. Иконка 16px.", m14_stencil, BLUE, PAPER),
    ("15-pixel", "Pixel", "Пиксельная A. Для самого мелкого.", m15_pixel, BLUE, PAPER),
    ("16-switch", "Switch", "Капсула и вырез. Выключатель.", m16_switch, INK, PAPER),
    ("17-signal", "Signal", "Три столба. Уровень сигнала = A.", m17_signal, BLUE, PAPER),
    ("18-arrow", "Arrow", "Жирная стрелка вверх.", m18_arrow, RED, CREAM),
    ("19-window", "Window", "Экран под крышей. Магазин + устройство.", m19_window, BLUE, PAPER),
    ("20-letter", "Letter", "Буква как знак. Школа Mobil.", m20_letter, RED, CREAM),
]


def wrap_mark(inner, fill_bg, size=200):
    return f'<rect width="{size}" height="{size}" fill="{fill_bg}"/>{inner}'


def avatar(inner, field, size=512):
    scale = size / 200
    return f'''
  <rect width="{size}" height="{size}" fill="{field}"/>
  <g transform="scale({scale})">{inner}</g>
'''


def rounded_avatar(inner, field, size=512, rx=96):
    scale = size / 200
    return f'''
  <rect width="{size}" height="{size}" rx="{rx}" fill="{field}"/>
  <g transform="scale({scale})">{inner}</g>
'''


def icon_draw(fn, slug):
    """White mark on brand field. Two-color marks stay as designed on black."""
    if slug in ("11-overlap", "13-fan"):
        return fn()
    return fn(WHITE)


files: dict[str, str] = {}

for slug, name, idea, fn, field, paper in MARKS:
    mark = fn()
    ic = icon_draw(fn, slug)
    files[f"{slug}-mark.svg"] = svg(200, 200, mark)
    files[f"{slug}-avatar.svg"] = svg(512, 512, rounded_avatar(ic, field))
    files[f"{slug}-icon64.svg"] = svg(64, 64, rounded_avatar(ic, field, 64, 12))
    files[f"{slug}-icon32.svg"] = svg(32, 32, rounded_avatar(ic, field, 32, 6))
    # Card
    files[f"{slug}-card.svg"] = svg(
        720,
        900,
        f'''
  <text x="40" y="52" font-family="{FONT}" font-size="13" font-weight="700"
        letter-spacing="3" fill="{field}">{xml(slug.upper().replace("-", "  "))}</text>
  <text x="40" y="84" font-family="{FONT}" font-size="22" font-weight="600"
        fill="{INK}">{xml(idea)}</text>
  <g transform="translate(160 140) scale(2.0)">
    <rect width="200" height="200" fill="{paper}"/>
    {mark}
  </g>
  <text x="40" y="620" font-family="{FONT}" font-size="13" font-weight="700"
        letter-spacing="3" fill="#7A8490">MARKETPLACE  ·  64  ·  32  ·  16</text>
  <g transform="translate(40 650) scale(0.32)">{rounded_avatar(ic, field, 200, 36)}</g>
  <g transform="translate(180 666) scale(0.16)">{rounded_avatar(ic, field, 200, 36)}</g>
  <g transform="translate(260 674) scale(0.08)">{rounded_avatar(ic, field, 200, 36)}</g>
  <text x="40" y="860" font-family="{FONT}" font-size="15" fill="#6A7280">ASHU  ·  Chermayeff &amp; Geismar &amp; Haviv</text>
''',
        paper,
    )

# Catalog 4×5 of avatars
cat = f'''
  <text x="48" y="56" font-family="{FONT}" font-size="14" font-weight="700"
        letter-spacing="4" fill="{BLUE}">ASHU  ·  CGH  ·  20 MARKS</text>
  <text x="48" y="100" font-family="{FONT}" font-size="32" font-weight="700"
        fill="{INK}">Двадцать знаков. Один язык. Иконка 32px.</text>
'''
cell, gap, ox, oy = 240, 24, 48, 140
for i, (slug, name, idea, fn, field, paper) in enumerate(MARKS):
    col, row = i % 4, i // 4
    x = ox + col * (cell + gap)
    y = oy + row * (cell + 56)
    cat += f'''
  <g transform="translate({x} {y}) scale({cell/512})">{rounded_avatar(icon_draw(fn, slug), field)}</g>
  <text x="{x}" y="{y + cell + 28}" font-family="{FONT}" font-size="13"
        font-weight="700" fill="{INK}">{i+1:02d}  {xml(name)}</text>
'''

files["00-catalog.svg"] = svg(1140, 1680, cat, PAPER)

# Icon sheet: all 20 at 64px and 32px — the real test
sheet = f'''
  <text x="40" y="48" font-family="{FONT}" font-size="14" font-weight="700"
        letter-spacing="3" fill="{BLUE}">SIZE TEST  ·  HOW A BUYER SEES IT</text>
  <text x="40" y="88" font-family="{FONT}" font-size="28" font-weight="700"
        fill="{INK}">64px — плитка маркетплейса</text>
'''
for i, (slug, name, idea, fn, field, paper) in enumerate(MARKS):
    x = 40 + (i % 10) * 76
    y = 120 + (i // 10) * 92
    sheet += f'<g transform="translate({x} {y})">{rounded_avatar(icon_draw(fn, slug), field, 64, 12)}</g>'

sheet += f'''
  <text x="40" y="360" font-family="{FONT}" font-size="28" font-weight="700"
        fill="{INK}">32px — фавикон / бейдж</text>
'''
for i, (slug, name, idea, fn, field, paper) in enumerate(MARKS):
    x = 40 + (i % 10) * 76
    y = 392 + (i // 10) * 60
    sheet += f'<g transform="translate({x} {y})">{rounded_avatar(icon_draw(fn, slug), field, 32, 6)}</g>'

sheet += f'''
  <text x="40" y="560" font-family="{FONT}" font-size="28" font-weight="700"
        fill="{INK}">16px — край</text>
'''
for i, (slug, name, idea, fn, field, paper) in enumerate(MARKS):
    x = 40 + (i % 10) * 76
    y = 592 + (i // 10) * 44
    sheet += f'<g transform="translate({x} {y})">{rounded_avatar(icon_draw(fn, slug), field, 16, 3)}</g>'

files["00-size-test.svg"] = svg(820, 740, sheet, WHITE)

readme = """# ASHU × Chermayeff & Geismar & Haviv — 20 знаков

Один язык: жирная геометрия, 1–2 цвета, силуэт без подписи.
Каждый знак проверяется на 64 / 32 / 16 px — как иконка магазина на маркетплейсе.

| # | Имя | Идея |
|---|---|---|
| 01 | Delta | Треугольник + круг = A и питание |
| 02 | Octagon | Два квадрата → восьмиугольник Chase |
| 03 | Bars | Три бруска = A |
| 04 | Hex | Чип и отверстие |
| 05 | Wedge | Кольцо с клином |
| 06 | House | Дом магазина |
| 07 | Chevron | Одна галка |
| 08 | Bolt | Молния плоскостями |
| 09 | Screen | Экран, play вырезан |
| 10 | Socket | Розетка |
| 11 | Overlap | Два квадрата, синий + красный |
| 12 | Diamond | Ромб и перекладина |
| 13 | Fan | Три сегмента, школа NBC |
| 14 | Stencil | Круг, A выбита |
| 15 | Pixel | Пиксельная A |
| 16 | Switch | Выключатель |
| 17 | Signal | Три столба сигнала |
| 18 | Arrow | Стрелка вверх |
| 19 | Window | Экран под крышей |
| 20 | Letter | Буква как знак, школа Mobil |

`*-avatar.svg` — иконка 512. `*-icon64.svg` / `*-icon32.svg` — мелкий масштаб.
Каталог: `00-catalog.svg`. Проверка размера: `00-size-test.svg`.

Пересборка: `python3 generate.py && ./render.sh`
"""

for name, content in files.items():
    (OUT / name).write_text(content)
    print("wrote", name)

(OUT / "README.md").write_text(readme)
print("count", len(files))
