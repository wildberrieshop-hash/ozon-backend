#!/usr/bin/env python3
"""ASHU identity — geometric construction in the spirit of Allan Peters.

Nouns combined in the mark: A (the name) + lightning (power) + chip (electronics).
Black-and-white first. Color is a system, not the idea.
"""

from __future__ import annotations

import math
from pathlib import Path

OUT = Path(__file__).resolve().parent

INK = "#111111"
PAPER = "#F4F1EA"
NAVY = "#15233B"
BOLT = "#E85D04"
CREAM = "#F4F1EA"
GOLD = "#C4A35A"
WHITE = "#FFFFFF"

FONT = "Inter, DejaVu Sans, sans-serif"


def svg(w, h, body, bg=None):
    bg_rect = f'<rect width="100%" height="100%" fill="{bg}"/>' if bg else ""
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}">
{bg_rect}{body}
</svg>
'''


# ---------------------------------------------------------------------------
# Primary mark — silhouette A, lightning counter
# Wide stance (shop / stability). Crotch low enough that the bolt never leaks.
# ---------------------------------------------------------------------------

A_OUTER = " ".join(
    [
        "M 100 18",
        "L 184 180",
        "L 148 180",
        "L 128 154",
        "L 72 154",
        "L 52 180",
        "L 16 180",
        "Z",
    ]
)

# Six-point bolt, fully inside the solid body (y < 154).
A_BOLT = " ".join(
    [
        "M 108 52",
        "L 82 100",
        "L 96 100",
        "L 78 140",
        "L 124 88",
        "L 108 88",
        "Z",
    ]
)

MARK_PATH = f"{A_OUTER} {A_BOLT}"


def mark_evenodd(fill: str) -> str:
    return f'<path fill="{fill}" fill-rule="evenodd" d="{MARK_PATH}"/>'


def mark_color(body: str, bolt: str) -> str:
    return (
        f'<path fill="{body}" d="{A_OUTER}"/>'
        f'<path fill="{bolt}" d="{A_BOLT}"/>'
    )


def hexagon(cx, cy, r) -> str:
    pts = []
    for i in range(6):
        a = math.radians(-90 + i * 60)
        pts.append(f"{cx + r * math.cos(a):.2f},{cy + r * math.sin(a):.2f}")
    return " ".join(pts)


# ---------------------------------------------------------------------------
# Geometric wordmark — same weight language as the mark
# A is a reduced cousin of the mark. S/H/U are squared chip-letters.
# Designed on cap-height 100.
# ---------------------------------------------------------------------------

def _shu_letters(fill: str) -> str:
    ess = (
        f'<g fill="{fill}">'
        '<rect x="0" y="0" width="70" height="18"/>'
        '<rect x="0" y="0" width="18" height="52"/>'
        '<rect x="0" y="41" width="70" height="18"/>'
        '<rect x="52" y="48" width="18" height="52"/>'
        '<rect x="0" y="82" width="70" height="18"/>'
        "</g>"
    )
    aitch = (
        f'<g fill="{fill}">'
        '<rect x="0" y="0" width="18" height="100"/>'
        '<rect x="54" y="0" width="18" height="100"/>'
        '<rect x="0" y="41" width="72" height="18"/>'
        "</g>"
    )
    you = (
        f'<path fill="{fill}" fill-rule="evenodd" d="'
        "M 0 0 H 18 V 82 H 54 V 0 H 72 V 100 H 0 Z"
        '"/>'
    )
    return (
        f'<g transform="translate(0 0)">{ess}</g>'
        f'<g transform="translate(86 0)">{aitch}</g>'
        f'<g transform="translate(174 0)">{you}</g>'
    )


def letter_a(fill: str, bolt_fill: str | None = None) -> str:
    """Cap-height 100 cousin of the mark. bolt_fill=None punches a hole."""
    a_scale = 100 / 162
    inner = (
        mark_color(fill, bolt_fill)
        if bolt_fill
        else f'<path fill="{fill}" fill-rule="evenodd" d="{MARK_PATH}"/>'
    )
    return (
        f'<g transform="translate({-16 * a_scale:.3f} {-18 * a_scale:.3f}) '
        f'scale({a_scale:.4f})">{inner}</g>'
    )


def wordmark(fill: str, x=0, y=0, h=100, *, with_a: bool = True, bolt_fill: str | None = None) -> str:
    s = h / 100.0
    a = letter_a(fill, bolt_fill) if with_a else ""
    shu_x = 118 if with_a else 0
    return f'''<g transform="translate({x} {y}) scale({s})">
  {a}
  <g transform="translate({shu_x} 0)">{_shu_letters(fill)}</g>
</g>'''


def lockup_word(a_fill: str, shu_fill: str, bolt_fill: str | None, x=0, y=0, h=100) -> str:
    """Reads as ASHU: the mark is the A."""
    s = h / 100.0
    a = letter_a(a_fill, bolt_fill)
    return f'''<g transform="translate({x} {y}) scale({s})">
  {a}
  <g transform="translate(118 0)">{_shu_letters(shu_fill)}</g>
</g>'''


WORDMARK_UNITS = 364  # 292+72


def wordmark_width(h=100) -> float:
    return WORDMARK_UNITS * (h / 100.0)


# ---------------------------------------------------------------------------
# Arc lettering for the badge (no textPath — librsvg is unreliable there)
# Screen angles: 0° east, 90° south, -90° north.
# ---------------------------------------------------------------------------

def arc_letters(
    text: str,
    cx: float,
    cy: float,
    radius: float,
    start_deg: float,
    end_deg: float,
    font_size: float,
    fill: str,
    *,
    bottom: bool = False,
    weight: int = 700,
    tracking: float = 0,
) -> str:
    n = len(text)
    parts = []
    span = end_deg - start_deg
    extra = tracking * (n - 1) if n > 1 else 0
    # tracking is extra degrees distributed across gaps
    for i, ch in enumerate(text):
        t = i / (n - 1) if n > 1 else 0.5
        ang = math.radians(start_deg + t * span)
        x = cx + radius * math.cos(ang)
        y = cy + radius * math.sin(ang)
        rot = math.degrees(ang) + (90 if not bottom else -90)
        esc = {"&": "&amp;", "<": "&lt;", ">": "&gt;"}.get(ch, ch)
        parts.append(
            f'<text x="0" y="0" dy="0.35em" transform="translate({x:.2f} {y:.2f}) '
            f'rotate({rot:.2f})" text-anchor="middle" font-family="{FONT}" '
            f'font-size="{font_size}" font-weight="{weight}" fill="{fill}">{esc}</text>'
        )
    return "\n  ".join(parts)


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

files: dict[str, str] = {}

files["ashu-mark.svg"] = svg(200, 200, mark_evenodd(INK))
files["ashu-mark-inverse.svg"] = svg(200, 200, mark_evenodd(WHITE), bg=INK)
files["ashu-mark-cream.svg"] = svg(200, 200, mark_evenodd(INK), bg=CREAM)
files["ashu-mark-color.svg"] = svg(200, 200, mark_color(NAVY, BOLT), bg=CREAM)

files["ashu-avatar.svg"] = svg(
    512,
    512,
    f'''
  <rect width="512" height="512" rx="96" fill="{NAVY}"/>
  <g transform="translate(56 48) scale(2)">
    {mark_color(CREAM, BOLT)}
  </g>
''',
)

files["ashu-avatar-circle.svg"] = svg(
    512,
    512,
    f'''
  <circle cx="256" cy="256" r="256" fill="{NAVY}"/>
  <g transform="translate(56 48) scale(2)">
    {mark_color(CREAM, BOLT)}
  </g>
''',
)

files["ashu-mark-chip.svg"] = svg(
    220,
    220,
    f'''
  <polygon fill="{NAVY}" points="{hexagon(110, 110, 100)}"/>
  <g transform="translate(22 18) scale(0.88)">
    {mark_color(CREAM, BOLT)}
  </g>
''',
    bg=CREAM,
)

# Logotype: the mark IS the A of ASHU
files["ashu-lockup.svg"] = svg(
    420,
    160,
    lockup_word(INK, INK, None, x=28, y=30, h=100),
    bg=CREAM,
)

files["ashu-lockup-inverse.svg"] = svg(
    420,
    160,
    lockup_word(WHITE, WHITE, None, x=28, y=30, h=100),
    bg=INK,
)

files["ashu-lockup-color.svg"] = svg(
    420,
    160,
    lockup_word(NAVY, NAVY, BOLT, x=28, y=30, h=100),
    bg=CREAM,
)

files["ashu-wordmark.svg"] = svg(
    400,
    140,
    wordmark(INK, x=18, y=20, h=100),
    bg=CREAM,
)

files["ashu-stacked.svg"] = svg(
    420,
    430,
    f'''
  <g transform="translate(110 16) scale(1)">{mark_color(NAVY, BOLT)}</g>
  {wordmark(NAVY, x=28, y=250, h=88)}
''',
    bg=CREAM,
)

files["ashu-favicon.svg"] = svg(
    64,
    64,
    f'''
  <rect width="64" height="64" rx="12" fill="{NAVY}"/>
  <g transform="translate(6 5) scale(0.26)">
    {mark_color(CREAM, BOLT)}
  </g>
''',
)

# Tiny expression: at 16px the A-bolt collapses — a bolt in a square is the system seed.
files["ashu-mark-16.svg"] = svg(
    16,
    16,
    f'''
  <rect width="16" height="16" rx="3" fill="{NAVY}"/>
  <path fill="{BOLT}" d="M 9.2 3 L 5.6 8.2 L 7.4 8.2 L 5.2 13 L 11.4 6.8 L 9.2 6.8 Z"/>
''',
)

# Badge — geometric seal. Navy field, gold rings, cream type, volt bolt.
badge_body = f'''
  <circle cx="250" cy="250" r="244" fill="{NAVY}"/>
  <circle cx="250" cy="250" r="230" fill="none" stroke="{GOLD}" stroke-width="4"/>
  <circle cx="250" cy="250" r="218" fill="none" stroke="{CREAM}" stroke-width="1.25" opacity="0.45"/>
  <circle cx="250" cy="250" r="152" fill="none" stroke="{GOLD}" stroke-width="2.5"/>

  {arc_letters("ASHU", 250, 250, 184, -148, -32, 38, CREAM, weight=800)}
  {arc_letters("ЭЛЕКТРОНИКА", 250, 250, 184, 152, 28, 17, GOLD, bottom=True, weight=700)}

  <circle cx="78" cy="250" r="5" fill="{BOLT}"/>
  <circle cx="422" cy="250" r="5" fill="{BOLT}"/>

  <g transform="translate(150 132) scale(1.0)">
    {mark_color(CREAM, BOLT)}
  </g>
'''
files["ashu-badge.svg"] = svg(500, 500, badge_body)

# Marketplace store header: icon + spelled name
files["ashu-banner.svg"] = svg(
    1200,
    320,
    f'''
  <rect width="1200" height="320" fill="{NAVY}"/>
  <g transform="translate(64 48) scale(1.12)">{mark_color(CREAM, BOLT)}</g>
  {wordmark(CREAM, x=320, y=72, h=112)}
  <rect x="324" y="214" width="44" height="4" fill="{BOLT}"/>
  <text x="384" y="226" font-family="{FONT}" font-size="22" font-weight="600"
        letter-spacing="6" fill="{GOLD}">ЭЛЕКТРОНИКА</text>
''',
)

# Presentation board
board = f'''
  <text x="72" y="78" font-family="{FONT}" font-size="15" font-weight="700"
        letter-spacing="7" fill="{NAVY}">ASHU  ·  IDENTITY</text>
  <text x="72" y="128" font-family="{FONT}" font-size="40" font-weight="700"
        fill="{INK}">Электроника. Один знак.</text>
  <text x="72" y="168" font-family="{FONT}" font-size="18"
        fill="{NAVY}" opacity="0.7">A + молния + чип. Геометрия в духе Аллана Питерса.</text>

  <text x="72" y="230" font-family="{FONT}" font-size="12" font-weight="700"
        letter-spacing="4" fill="{NAVY}" opacity="0.5">01  MARK  —  BLACK AND WHITE</text>
  <rect x="72" y="248" width="520" height="440" fill="{WHITE}"/>
  <g transform="translate(182 308) scale(2.0)">{mark_evenodd(INK)}</g>
  <rect x="608" y="248" width="520" height="440" fill="{INK}"/>
  <g transform="translate(718 308) scale(2.0)">{mark_evenodd(WHITE)}</g>

  <text x="72" y="738" font-family="{FONT}" font-size="12" font-weight="700"
        letter-spacing="4" fill="{NAVY}" opacity="0.5">02  COLOR  —  NAVY / VOLT / CREAM</text>
  <rect x="72" y="756" width="340" height="300" fill="{WHITE}"/>
  <g transform="translate(142 796) scale(1.1)">{mark_color(NAVY, BOLT)}</g>
  <rect x="428" y="756" width="340" height="300" fill="{NAVY}"/>
  <g transform="translate(498 796) scale(1.1)">{mark_color(CREAM, BOLT)}</g>
  <g transform="translate(784 756)">
    <rect width="340" height="300" fill="{CREAM}"/>
    <polygon fill="{NAVY}" points="{hexagon(170, 150, 128)}"/>
    <g transform="translate(82 62) scale(0.88)">{mark_color(CREAM, BOLT)}</g>
  </g>

  <text x="72" y="1110" font-family="{FONT}" font-size="12" font-weight="700"
        letter-spacing="4" fill="{NAVY}" opacity="0.5">03  LOCKUP  —  THE MARK IS THE A</text>
  <rect x="72" y="1128" width="1056" height="200" fill="{WHITE}"/>
  {lockup_word(INK, INK, None, x=120, y=1176, h=100)}
  {lockup_word(NAVY, NAVY, BOLT, x=620, y=1176, h=100)}

  <text x="72" y="1388" font-family="{FONT}" font-size="12" font-weight="700"
        letter-spacing="4" fill="{NAVY}" opacity="0.5">04  BADGE  ·  AVATAR  ·  SIZE</text>

  <g transform="translate(72 1408) scale(0.56)">
    <circle cx="250" cy="250" r="244" fill="{NAVY}"/>
    <circle cx="250" cy="250" r="230" fill="none" stroke="{GOLD}" stroke-width="4"/>
    <circle cx="250" cy="250" r="152" fill="none" stroke="{GOLD}" stroke-width="2.5"/>
    {arc_letters("ASHU", 250, 250, 184, -148, -32, 38, CREAM, weight=800)}
    {arc_letters("ЭЛЕКТРОНИКА", 250, 250, 184, 152, 28, 17, GOLD, bottom=True, weight=700)}
    <circle cx="78" cy="250" r="5" fill="{BOLT}"/>
    <circle cx="422" cy="250" r="5" fill="{BOLT}"/>
    <g transform="translate(150 132)">{mark_color(CREAM, BOLT)}</g>
  </g>

  <g transform="translate(380 1436) scale(0.48)">
    <rect width="512" height="512" rx="96" fill="{NAVY}"/>
    <g transform="translate(56 48) scale(2)">{mark_color(CREAM, BOLT)}</g>
  </g>
  <g transform="translate(656 1436) scale(0.48)">
    <circle cx="256" cy="256" r="256" fill="{NAVY}"/>
    <g transform="translate(56 48) scale(2)">{mark_color(CREAM, BOLT)}</g>
  </g>

  <g transform="translate(940 1460)">{mark_evenodd(INK)}</g>
  <g transform="translate(940 1680) scale(0.32)">{mark_evenodd(INK)}</g>
  <g transform="translate(1020 1696) scale(0.16)">{mark_evenodd(INK)}</g>
  <g transform="translate(1070 1704) scale(0.08)">{mark_evenodd(INK)}</g>
  <text x="940" y="1888" font-family="{FONT}" font-size="12" fill="{NAVY}">200</text>
  <text x="940" y="1768" font-family="{FONT}" font-size="11" fill="{NAVY}">64</text>
  <text x="1020" y="1768" font-family="{FONT}" font-size="11" fill="{NAVY}">32</text>
  <text x="1070" y="1768" font-family="{FONT}" font-size="11" fill="{NAVY}">16</text>

  <rect x="72" y="1936" width="28" height="28" fill="{NAVY}"/>
  <rect x="108" y="1936" width="28" height="28" fill="{BOLT}"/>
  <rect x="144" y="1936" width="28" height="28" fill="{GOLD}"/>
  <rect x="180" y="1936" width="28" height="28" fill="{INK}"/>
  <text x="224" y="1956" font-family="{FONT}" font-size="13" fill="{NAVY}">
    Navy  #15233B    Volt  #E85D04    Gold  #C4A35A    Ink  #111111
  </text>
  <text x="72" y="2010" font-family="{FONT}" font-size="13" fill="{NAVY}" opacity="0.55">
    ASHU  ·  магазин электроники  ·  знак читается как «A» и как энергия
  </text>
'''

files["ashu-board.svg"] = svg(1200, 2060, board, bg=CREAM)

readme = """# ASHU — логотип

Магазин электроники. Знак собран методом Аллана Питерса:
существительные → одно геометрическое совмещение → сначала чёрно-белый знак.

**Существительные:** A (имя ASHU) · молния (ток) · чип (шестигранник).

**Идея.** Силуэт буквы A. Внутри — вырез молнии на месте перекладины.
С расстояния это A, вблизи — электричество.

## Файлы

| Файл | Назначение |
|---|---|
| `ashu-mark.svg` | Основной знак, чёрный, прозрачный фон |
| `ashu-mark-inverse.svg` | Белый на чёрном |
| `ashu-mark-color.svg` | Navy + volt |
| `ashu-mark-chip.svg` | Знак в шестиграннике-чипе |
| `ashu-wordmark.svg` | Надпись ASHU |
| `ashu-lockup.svg` | Логотип: знак = буква A в ASHU |
| `ashu-lockup-color.svg` | Цветной логотип |
| `ashu-badge.svg` | Печать для упаковки / карточки |
| `ashu-avatar.svg` | Квадратный аватар маркетплейса |
| `ashu-avatar-circle.svg` | Круглый аватар |
| `ashu-banner.svg` | Шапка магазина |
| `ashu-favicon.svg` | Иконка 64 |
| `ashu-mark-16.svg` | 16px — только молния |
| `ashu-stacked.svg` | Вертикальная сборка |
| `ashu-board.svg` | Презентационный лист |

## Цвета

| Имя | Hex |
|---|---|
| Ink | `#111111` |
| Navy | `#15233B` |
| Volt | `#E85D04` |
| Cream | `#F4F1EA` |
| Gold | `#C4A35A` |

Пересборка: `python3 generate.py && ./render.sh`
"""

for name, content in files.items():
    (OUT / name).write_text(content)
    print("wrote", name)

(OUT / "README.md").write_text(readme)
print("wrote README.md")
