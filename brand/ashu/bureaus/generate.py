#!/usr/bin/env python3
"""Ten ASHU identities — one concept per bureau. Not recolors of the same A."""

from __future__ import annotations

import math
from pathlib import Path

OUT = Path(__file__).resolve().parent
FONT = "Inter, DejaVu Sans, sans-serif"
SERIF = "Tinos, Liberation Serif, Times New Roman, serif"


def svg(w, h, body, bg=None):
    bg_rect = f'<rect width="100%" height="100%" fill="{bg}"/>' if bg else ""
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}">
{bg_rect}{body}
</svg>
'''


def card(bg, body, w=720, h=920):
    return svg(w, h, body, bg)


def xml(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def label(x, y, text, fill, size=13, weight=700, tracking=4, anchor="start"):
    return (
        f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
        f'font-weight="{weight}" letter-spacing="{tracking}" fill="{fill}" '
        f'text-anchor="{anchor}">{xml(text)}</text>'
    )


def caption(x, y, text, fill, size=16, weight=500, anchor="start"):
    return (
        f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{xml(text)}</text>'
    )


# =====================================================================
# 1. Chermayeff & Geismar & Haviv
# One shape. Triangle + circular counter = A + power. One color.
# =====================================================================
CGH = "#0050A0"
CGH_BG = "#F3F5F8"

CGH_MARK = f'''
<g fill="{CGH}" fill-rule="evenodd">
  <path d="M 120 14 L 228 198 L 12 198 Z M 120 92 A 32 32 0 1 0 120 156 A 32 32 0 1 0 120 92 Z"/>
</g>
'''


def cgh_word(x, y, fill=CGH, size=42):
    return (
        f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
        f'font-weight="800" letter-spacing="10" fill="{fill}">ASHU</text>'
    )


# =====================================================================
# 2. Clay — friendly product wordmark, lowercase, soft a
# =====================================================================
CLAY = "#FF5A36"
CLAY_INK = "#2B2624"
CLAY_BG = "#F7F1EA"


def clay_word(x, y, s=1.0, fill=CLAY_INK):
    # Stroke-built lowercase: product UI, not a broken puzzle.
    sw = 11
    return f'''<g transform="translate({x} {y}) scale({s})"
              fill="none" stroke="{fill}" stroke-width="{sw}"
              stroke-linecap="round" stroke-linejoin="round">
  <circle cx="28" cy="40" r="18"/>
  <path d="M 46 22 V 58"/>
  <circle cx="58" cy="16" r="3.2" fill="{CLAY}" stroke="none"/>
  <path d="M 108 26 C 100 18 86 20 84 32 C 82 42 96 44 104 48 C 114 52 116 62 104 66 C 92 70 82 64 80 58"/>
  <path d="M 140 8 V 60"/>
  <path d="M 140 36 C 148 28 168 28 168 48 V 60"/>
  <path d="M 196 22 V 48 C 196 62 220 62 220 48 V 22"/>
</g>'''


CLAY_ICON = f'''
<rect width="200" height="200" rx="48" fill="{CLAY}"/>
<g fill="none" stroke="#FFF8F3" stroke-width="14" stroke-linecap="round">
  <circle cx="92" cy="108" r="36"/>
  <path d="M 128 72 V 144"/>
</g>
<circle cx="150" cy="68" r="8" fill="#FFF8F3"/>
'''


# =====================================================================
# 3. Designstudio — type is the logo. Flat-top A. Digital black.
# =====================================================================
DS = "#0B0B0B"
DS_BG = "#FFFFFF"
DS_MUTED = "#6B6B6B"


def ds_letters(fill=DS, h=72):
    # Custom digital grotesque, all caps, medium-bold. Flat-top A.
    sc = h / 72
    return f'''<g fill="{fill}" transform="scale({sc})">
  <path fill-rule="evenodd" d="M 10 72 L 26 0 H 46 L 62 72 H 50 L 46 54 H 22 L 18 72 Z M 25 42 H 43 L 36 12 Z"/>
  <path d="M 110 8 C 90 8 76 18 76 32 C 76 44 88 48 102 52 C 112 55 114 58 114 62
           C 114 66 108 68 98 68 C 88 68 84 64 82 60 H 72 C 74 70 86 76 98 76
           C 120 76 128 66 128 56 C 128 44 116 40 102 36 C 92 33 90 30 90 26
           C 90 20 96 16 108 16 C 116 16 120 20 122 26 H 132 C 128 14 120 8 110 8 Z"/>
  <path d="M 156 0 H 168 V 30 H 204 V 0 H 216 V 72 H 204 V 42 H 168 V 72 H 156 Z"/>
  <path d="M 240 0 H 252 V 54 C 252 66 276 66 276 54 V 0 H 288 V 54
           C 288 74 240 74 240 54 Z"/>
</g>'''


# =====================================================================
# 4. DixonBaxi — media / play. Acid yellow on black.
# =====================================================================
DB_Y = "#F2E111"
DB_K = "#0A0A0A"


def db_mark():
    # Screen bezel + play triangle that is an A (bar as a cut)
    return f'''
  <rect x="16" y="36" width="208" height="148" rx="18" fill="{DB_Y}"/>
  <rect x="28" y="48" width="184" height="124" rx="8" fill="{DB_K}"/>
  <path fill="{DB_Y}" fill-rule="evenodd"
        d="M 86 70 L 168 110 L 86 150 Z M 104 104 L 140 110 L 104 116 Z"/>
'''


def db_word(x, y, fill=DB_Y, size=64):
    return (
        f'<g transform="translate({x} {y}) skewX(-14)">'
        f'<text x="0" y="0" font-family="{FONT}" font-size="{size}" '
        f'font-weight="900" letter-spacing="-2" fill="{fill}">ASHU</text></g>'
    )


# =====================================================================
# 5. Futura — premium craft. Serif wordmark, thin gold monogram.
# =====================================================================
FU_INK = "#1C1612"
FU_GOLD = "#C6A15B"
FU_CREAM = "#F3EDE2"


def fu_mono():
    # Three strokes only — jeweler's A, not a outlined blob.
    return f'''
  <g fill="none" stroke="{FU_GOLD}" stroke-width="3.4" stroke-linecap="square" stroke-linejoin="miter">
    <path d="M 100 24 L 40 188"/>
    <path d="M 100 24 L 160 188"/>
    <path d="M 72 128 H 128"/>
  </g>
'''


def fu_word(x, y, fill=FU_INK, size=48):
    return (
        f'<text x="{x}" y="{y}" font-family="{SERIF}" font-size="{size}" '
        f'font-weight="400" letter-spacing="16" fill="{fill}" '
        f'text-anchor="middle">ASHU</text>'
    )


# =====================================================================
# 6. KIND — the plug IS the A. One idea.
# =====================================================================
KIND_INK = "#141414"
KIND_BG = "#EFECE6"
KIND_SPOT = "#1F7A4D"


def kind_plug(fill=KIND_INK, hole=KIND_BG):
    # Rounded house/plug body (not a sharp CGH triangle) + two pins + two socket holes.
    return f'''
  <g fill="{fill}">
    <path d="M 100 22
             L 170 132
             C 186 154 176 186 148 186
             H 52
             C 24 186 14 154 30 132
             Z"/>
    <rect x="62" y="186" width="20" height="40" rx="6"/>
    <rect x="118" y="186" width="20" height="40" rx="6"/>
  </g>
  <circle cx="78" cy="128" r="13" fill="{hole}"/>
  <circle cx="122" cy="128" r="13" fill="{hole}"/>
'''


# =====================================================================
# 7. Landor — wordmark with a hidden current between S and H (FedEx school)
# =====================================================================
LA_NAVY = "#0C2D6B"
LA_RED = "#E10600"
LA_BG = "#F5F6F8"


def landor_word(x, y, h=88, fill=LA_NAVY):
    sc = h / 88
    # FedEx school: one clean bolt in the gutter. Letters stay readable.
    a = (
        "M 34 0 L 68 88 H 54 L 47 68 H 21 L 14 88 H 0 Z "
        "M 25 54 H 43 L 34 28 Z"
    )
    s = (
        "M 4 0 H 50 V 14 H 18 V 34 H 46 "
        "L 38 50 H 46 L 32 88 H 4 V 74 H 22 L 28 50 H 18 L 32 34 H 4 Z"
    )
    hh = (
        "M 18 0 H 32 V 36 H 62 V 0 H 76 V 88 H 62 V 50 H 32 V 88 H 18 "
        "L 6 72 H 14 L 4 50 H 12 L 18 36 Z"
    )
    u = "M 0 0 H 14 V 70 H 52 V 0 H 66 V 76 H 0 Z"
    return f'''<g transform="translate({x} {y}) scale({sc})" fill="{fill}">
  <path d="{a}"/>
  <g transform="translate(78 0)"><path d="{s}"/></g>
  <g transform="translate(134 0)"><path d="{hh}"/></g>
  <g transform="translate(222 0)"><path d="{u}"/></g>
</g>'''


# =====================================================================
# 8. Pentagram — 2×2 poster grid. The letters ARE the mark.
# =====================================================================
PE = "#111111"
PE_BG = "#F2F2F0"


def pentagram_grid(x, y, cell=110, fill=PE, bg=None):
    letters = [("A", 0, 0), ("S", 1, 0), ("H", 0, 1), ("U", 1, 1)]
    parts = []
    for ch, cx, cy in letters:
        px = x + cx * cell
        py = y + cy * cell
        if bg:
            parts.append(
                f'<rect x="{px}" y="{py}" width="{cell}" height="{cell}" fill="{bg}"/>'
            )
        parts.append(
            f'<text x="{px + cell/2}" y="{py + cell*0.72}" text-anchor="middle" '
            f'font-family="{FONT}" font-size="{cell*0.62}" font-weight="900" '
            f'fill="{fill}">{ch}</text>'
        )
    return "\n".join(parts)


# =====================================================================
# 9. Saffron — civic signal. A as beacon / radio tower.
# =====================================================================
SA_TEAL = "#0B6E7A"
SA_INK = "#12313A"
SA_BG = "#E8F1F2"


def saffron_mark(fill=SA_TEAL):
    # Two legs of A + crossbar. Three signal arcs from the apex.
    return f'''
  <g fill="none" stroke="{fill}" stroke-linecap="round" stroke-linejoin="round">
    <path stroke-width="16" d="M 40 188 L 100 28 L 160 188"/>
    <path stroke-width="14" d="M 62 124 H 138"/>
    <path stroke-width="8" d="M 128 40 A 36 36 0 0 1 154 78"/>
    <path stroke-width="8" d="M 136 28 A 56 56 0 0 1 176 86"/>
    <path stroke-width="8" d="M 144 16 A 76 76 0 0 1 198 94"/>
  </g>
'''


# =====================================================================
# 10. Wolff Olins — living circle-A. Simple, ownable, slightly wrong.
# =====================================================================
WO = "#FF3A1A"
WO_BG = "#FFF6F1"
WO_INK = "#1A1A1A"


def wo_mark(fill=WO, letter="#FFFFFF"):
    return f'''
  <circle cx="100" cy="100" r="96" fill="{fill}"/>
  <path fill="{letter}" d="M 100 38 L 148 162 H 128 L 118 134 H 82 L 72 162 H 52 Z
                           M 88 116 H 112 L 100 80 Z"/>
'''


def wo_word(x, y, fill=WO_INK, size=56):
    return (
        f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
        f'font-weight="900" letter-spacing="-1" fill="{fill}">ASHU</text>'
    )


# =====================================================================
# Assemble files
# =====================================================================
files: dict[str, str] = {}

# --- cards (what you swipe through) ---

files["01-cgh-card.svg"] = card(
    CGH_BG,
    f'''
  {label(48, 56, "01  CHERMAYEFF  &  GEISMAR  &  HAVIV", CGH, 12, 700, 2)}
  {caption(48, 88, "Одна фигура. Треугольник и круг — A и питание.", "#3A4A62", 18)}
  <g transform="translate(200 180) scale(1.15)">{CGH_MARK}</g>
  {cgh_word(248, 720)}
  {caption(48, 860, "Классика знака: NBC, Mobil, Chase. Один цвет, ноль декора.", "#5A6A80", 15)}
''',
)

files["01-cgh-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{CGH}"/>'
    f'<g transform="translate(116 80) scale(1.16)">'
    f'<path fill="#FFFFFF" fill-rule="evenodd" '
        f'd="M 120 14 L 228 198 L 12 198 Z M 120 92 A 32 32 0 1 0 120 156 A 32 32 0 1 0 120 92 Z"/>'
    f"</g>",
)

files["01-cgh-lockup.svg"] = svg(
    640,
    200,
    f'<g transform="translate(24 16) scale(0.72)">{CGH_MARK}</g>'
    f"{cgh_word(220, 128, CGH, 56)}",
    CGH_BG,
)

# Clay
files["02-clay-card.svg"] = card(
    CLAY_BG,
    f'''
  {label(48, 56, "02  CLAY", CLAY, 12, 700, 3)}
  {caption(48, 88, "Продуктовый wordmark. Как иконка приложения.", CLAY_INK, 18)}
  <g transform="translate(260 200) scale(1.0)">{CLAY_ICON}</g>
  <g transform="translate(88 560)">{clay_word(0, 0, 1.35)}</g>
  {caption(48, 860, "IT и стартапы: Slack, Airbnb, Dropbox. Мягко, тепло, lowercase.", "#7A7068", 15)}
''',
)

files["02-clay-avatar.svg"] = svg(512, 512, f'<g transform="scale(2.56)">{CLAY_ICON}</g>')

files["02-clay-lockup.svg"] = svg(
    640,
    180,
    f'<g transform="translate(28 28) scale(0.62)">{CLAY_ICON}</g>'
    f'<g transform="translate(180 52)">{clay_word(0, 0, 1.2)}</g>',
    CLAY_BG,
)

# Designstudio
files["03-designstudio-card.svg"] = card(
    DS_BG,
    f'''
  {label(48, 56, "03  DESIGNSTUDIO", DS, 12, 700, 3)}
  {caption(48, 88, "Логотип — это шрифт. Никакой иконки.", DS_MUTED, 18)}
  <g transform="translate(96 340)">{ds_letters(DS, 96)}</g>
  {caption(48, 860, "Минимализм Uber / Instagram / Robinhood. Цифра, не картинка.", DS_MUTED, 15)}
''',
)

files["03-designstudio-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{DS}"/>'
    f'<g transform="translate(78 220)">{ds_letters("#FFFFFF", 58)}</g>',
)

files["03-designstudio-lockup.svg"] = svg(
    640,
    180,
    f'<g transform="translate(80 54)">{ds_letters(DS, 72)}</g>',
    DS_BG,
)

# DixonBaxi
files["04-dixonbaxi-card.svg"] = card(
    DB_K,
    f'''
  {label(48, 56, "04  DIXONBAXI", DB_Y, 12, 700, 3)}
  {caption(48, 88, "Медиа. Play-кнопка, которая читается как A.", "#C8C070", 18)}
  <g transform="translate(200 200) scale(1.25)">{db_mark()}</g>
  {db_word(80, 720, DB_Y, 92)}
  {caption(48, 860, "ITV, UEFA, Snapchat. Плакат, не визитка.", "#8A8840", 15)}
''',
)

files["04-dixonbaxi-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{DB_K}"/>'
    f'<g transform="translate(56 70) scale(1.7)">{db_mark()}</g>',
)

files["04-dixonbaxi-lockup.svg"] = svg(
    720,
    200,
    f'<rect width="720" height="200" fill="{DB_K}"/>'
    f'<g transform="translate(24 16) scale(0.72)">{db_mark()}</g>'
    f'{db_word(250, 130, DB_Y, 72)}',
)

# Futura
files["05-futura-card.svg"] = card(
    FU_CREAM,
    f'''
  {label(48, 56, "05  FUTURA", FU_GOLD, 12, 700, 4)}
  {caption(48, 88, "Премиум. Не гаджеты — дом электроники.", FU_INK, 18)}
  <g transform="translate(220 180)">{fu_mono()}</g>
  {fu_word(360, 560)}
  <line x1="250" y1="590" x2="470" y2="590" stroke="{FU_GOLD}" stroke-width="0.8"/>
  <text x="360" y="624" text-anchor="middle" font-family="{SERIF}" font-size="13"
        letter-spacing="7" fill="{FU_GOLD}">ELECTRONICS</text>
  {caption(48, 860, "Стратегия для Reserve и Nike. Тишина, золото, засечки.", "#7A7060", 15)}
''',
)

files["05-futura-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{FU_INK}"/>'
    f'<g transform="translate(56 40) scale(2.0)">{fu_mono()}</g>',
)

files["05-futura-lockup.svg"] = svg(
    640,
    180,
    f'<g transform="translate(20 -10) scale(0.7)">{fu_mono()}</g>'
    f"{fu_word(400, 108, FU_INK, 40)}",
    FU_CREAM,
)

# KIND
files["06-kind-card.svg"] = card(
    KIND_BG,
    f'''
  {label(48, 56, "06  KIND   OSLO / BERGEN", KIND_SPOT, 12, 700, 2)}
  {caption(48, 88, "Вилка и есть буква. Одна смелая мысль.", KIND_INK, 18)}
  <g transform="translate(210 170) scale(1.2)">{kind_plug()}</g>
  <text x="360" y="720" text-anchor="middle" font-family="{FONT}" font-size="52"
        font-weight="800" letter-spacing="6" fill="{KIND_INK}">ASHU</text>
  {caption(48, 860, "Концепт важнее красоты. Награды за идею, не за тренд.", "#6A6860", 15)}
''',
)

files["06-kind-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{KIND_INK}"/>'
    f'<g transform="translate(56 28) scale(2)">'
    f'{kind_plug("#F4F1EA", KIND_INK)}'
    f"</g>",
)

files["06-kind-lockup.svg"] = svg(
    640,
    200,
    f'<g transform="translate(20 8) scale(0.72)">{kind_plug()}</g>'
    f'<text x="230" y="120" font-family="{FONT}" font-size="56" font-weight="800" '
    f'letter-spacing="6" fill="{KIND_INK}">ASHU</text>',
    KIND_BG,
)

# Landor
files["07-landor-card.svg"] = card(
    LA_BG,
    f'''
  {label(48, 56, "07  LANDOR", LA_RED, 12, 700, 4)}
  {caption(48, 88, "Скрытый ток между S и H. Школа FedEx.", LA_NAVY, 18)}
  <g transform="translate(90 360)">{landor_word(0, 0, 110)}</g>
  {caption(48, 860, "Глобальные системы: FedEx, Coca-Cola, Lego. Смысл в промежутке.", "#5A6A80", 15)}
''',
)

files["07-landor-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{LA_NAVY}"/>'
    f'<g transform="translate(70 200)">{landor_word(0, 0, 92, "#FFFFFF")}</g>',
)

files["07-landor-lockup.svg"] = svg(
    720,
    180,
    f'<g transform="translate(70 40)">{landor_word(0, 0, 96)}</g>',
    LA_BG,
)

# Pentagram
files["08-pentagram-card.svg"] = card(
    PE_BG,
    f'''
  {label(48, 56, "08  PENTAGRAM", PE, 12, 700, 3)}
  {caption(48, 88, "Четыре буквы — четыре клетки. Плакат.", PE, 18)}
  {pentagram_grid(150, 200, 210, PE)}
  {caption(48, 860, "Партнёрская сеть. Mastercard, Windows 11, The Guardian.", "#666", 15)}
''',
)

files["08-pentagram-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{PE}"/>'
    f"{pentagram_grid(16, 16, 240, '#FFFFFF')}",
)

files["08-pentagram-lockup.svg"] = svg(
    640,
    200,
    f"{pentagram_grid(40, 16, 84, PE)}"
    f'<text x="240" y="120" font-family="{FONT}" font-size="64" font-weight="900" '
    f'letter-spacing="-2" fill="{PE}">ASHU</text>',
    PE_BG,
)

# Saffron
files["09-saffron-card.svg"] = card(
    SA_BG,
    f'''
  {label(48, 56, "09  SAFFRON", SA_TEAL, 12, 700, 4)}
  {caption(48, 88, "Маяк. A как сигнал — авиация, города, сети.", SA_INK, 18)}
  <g transform="translate(200 180) scale(1.2)">{saffron_mark()}</g>
  <text x="360" y="720" text-anchor="middle" font-family="{FONT}" font-size="44"
        font-weight="600" letter-spacing="12" fill="{SA_INK}">ASHU</text>
  {caption(48, 860, "Barcelona, Vodafone. Институционально, не стартап.", "#5A7A80", 15)}
''',
)

files["09-saffron-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{SA_INK}"/>'
    f'<g transform="translate(56 70) scale(2.0)">{saffron_mark("#7FDBE6")}</g>',
)

files["09-saffron-lockup.svg"] = svg(
    680,
    200,
    f'<g transform="translate(16 8) scale(0.85)">{saffron_mark()}</g>'
    f'<text x="250" y="120" font-family="{FONT}" font-size="48" font-weight="600" '
    f'letter-spacing="10" fill="{SA_INK}">ASHU</text>',
    SA_BG,
)

# Wolff Olins
files["10-wolffolins-card.svg"] = card(
    WO_BG,
    f'''
  {label(48, 56, "10  WOLFF OLINS", WO, 12, 700, 3)}
  {caption(48, 88, "Живой круг. Простой, чужой, запоминается.", WO_INK, 18)}
  <g transform="translate(210 190) scale(1.5)">{wo_mark()}</g>
  {wo_word(248, 720)}
  {caption(48, 860, "Легенда: Unilever, МТС, Зенит. Бренд как характер, не как знак.", "#8A7068", 15)}
''',
)

files["10-wolffolins-avatar.svg"] = svg(
    512,
    512,
    f'<rect width="512" height="512" fill="{WO}"/>'
    f'<g transform="translate(56 56) scale(2.0)">'
    f'<path fill="#FFFFFF" d="M 100 38 L 148 162 H 128 L 118 134 H 82 L 72 162 H 52 Z M 88 116 H 112 L 100 80 Z"/>'
    f"</g>",
)

files["10-wolffolins-lockup.svg"] = svg(
    640,
    200,
    f'<g transform="translate(20 10) scale(0.9)">{wo_mark()}</g>'
    f"{wo_word(230, 122)}",
    WO_BG,
)

# =====================================================================
# Catalog — 2 × 5
# =====================================================================
# Mini cards drawn into a big board. Each cell 560×640.

def mini_cell(tx, ty, inner):
    return f'<g transform="translate({tx} {ty})">{inner}</g>'


catalog = f'''
  <text x="64" y="64" font-family="{FONT}" font-size="14" font-weight="700"
        letter-spacing="5" fill="#444">ASHU  ·  10 BUREAUS  ·  ONE MARK EACH</text>
  <text x="64" y="108" font-family="{FONT}" font-size="36" font-weight="700"
        fill="#111">Десять разных бюро. Десять разных идей.</text>
'''

# We'll compose the catalog by embedding scaled versions of the avatars + names
# rather than the full cards (more even).

entries = [
    (CGH_BG, CGH, "01  CGH", "Треугольник + круг",
     f'<g transform="translate(150 90) scale(0.85)">{CGH_MARK}</g>'),
    (CLAY_BG, CLAY_INK, "02  CLAY", "lowercase ashu",
     f'<g transform="translate(170 100) scale(0.85)">{CLAY_ICON}</g>'),
    ("#FFFFFF", DS, "03  DESIGNSTUDIO", "Только шрифт",
     f'<g transform="translate(70 200)">{ds_letters(DS, 42)}</g>'),
    (DB_K, DB_Y, "04  DIXONBAXI", "Play = A",
     f'<g transform="translate(130 80) scale(0.95)">{db_mark()}</g>'),
    (FU_CREAM, FU_INK, "05  FUTURA", "Премиум-монограмма",
     f'<g transform="translate(150 70) scale(0.9)">{fu_mono()}</g>'),
    (KIND_BG, KIND_INK, "06  KIND", "Вилка = A",
     f'<g transform="translate(150 50) scale(0.9)">{kind_plug()}</g>'),
    (LA_BG, LA_NAVY, "07  LANDOR", "Ток между S и H",
     f'<g transform="translate(70 180)">{landor_word(0, 0, 56)}</g>'),
    (PE_BG, PE, "08  PENTAGRAM", "Сетка 2×2",
     f"{pentagram_grid(140, 80, 120, PE)}"),
    (SA_BG, SA_INK, "09  SAFFRON", "A-маяк",
     f'<g transform="translate(150 70) scale(0.85)">{saffron_mark()}</g>'),
    (WO_BG, WO_INK, "10  WOLFF OLINS", "Живой круг",
     f'<g transform="translate(160 80) scale(1.15)">{wo_mark()}</g>'),
]

cell_w, cell_h = 560, 620
gap_x, gap_y = 32, 32
ox, oy = 64, 160
for i, (bg, fg, title, idea, art) in enumerate(entries):
    col, row = i % 2, i // 2
    x = ox + col * (cell_w + gap_x)
    y = oy + row * (cell_h + gap_y)
    catalog += f'''
  <rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" fill="{bg}"/>
  <text x="{x+28}" y="{y+40}" font-family="{FONT}" font-size="13" font-weight="700"
        letter-spacing="2" fill="{fg}">{title}</text>
  <text x="{x+28}" y="{y+64}" font-family="{FONT}" font-size="14" fill="{fg}" opacity="0.55">{idea}</text>
  <g transform="translate({x} {y+40})">{art}</g>
'''

files["00-catalog.svg"] = svg(1280, 3480, catalog, "#D8D6D0")

readme = """# ASHU — 10 бюро, 10 идей

Не перекраски одного знака. У каждого бюро своя логика.

| # | Бюро | Идея | Файлы |
|---|---|---|---|
| 01 | Chermayeff & Geismar & Haviv | Треугольник + круг = A и кнопка питания | `01-cgh-*` |
| 02 | Clay | Lowercase ashu, мягкая иконка, LED-точка | `02-clay-*` |
| 03 | Designstudio | Логотип — только кастомный шрифт | `03-designstudio-*` |
| 04 | DixonBaxi | Play-кнопка в экране = A, жёлтый/чёрный | `04-dixonbaxi-*` |
| 05 | Futura | Премиум-монограмма, антиква, золото | `05-futura-*` |
| 06 | KIND | Вилка и есть буква A | `06-kind-*` |
| 07 | Landor | Скрытый ток в промежутке S–H | `07-landor-*` |
| 08 | Pentagram | Плакатная сетка 2×2 | `08-pentagram-*` |
| 09 | Saffron | A как радиомаяк | `09-saffron-*` |
| 10 | Wolff Olins | Живой красный круг | `10-wolffolins-*` |

`*-card.svg` — презентационный лист. `*-avatar.svg` — аватар маркетплейса. `*-lockup.svg` — горизонтальная сборка.

Каталог всех десяти: `00-catalog.svg`.

Пересборка: `python3 generate.py && ./render.sh`
"""

for name, content in files.items():
    (OUT / name).write_text(content)
    print("wrote", name)

(OUT / "README.md").write_text(readme)
print("wrote README.md")
