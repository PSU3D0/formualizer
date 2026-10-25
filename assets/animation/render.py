# /// script
# requires-python = ">=3.11"
# dependencies = ["pillow>=10.4", "numpy>=1.26", "fonttools>=4.50", "brotli>=1.1"]
# ///
"""Formualizer README GIF — scripted, deterministic frame renderer.

Usage:  uv run render.py [--frames-only] [--stills-only] [--jobs N]
Output: out/formualizer-calc.gif, out/formualizer-calc.mp4, stills/*.png

Every frame is a pure function of its time t (seconds), so the render is
reproducible and parallelisable. Drawing happens at 2x and is downsampled.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
from functools import lru_cache
from multiprocessing import Pool
from pathlib import Path

import numpy as np
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont

HERE = Path(__file__).resolve().parent
BUILD = HERE / "build"
FRAMES = BUILD / "frames"
OUT = HERE / "out"
STILLS = HERE / "stills"
BENCHMARK = json.loads((HERE.parents[1] / "benchmarks" / "results" / "2026-10-25-native-financial-model.json").read_text())
FIRST_CALC_LABEL = f"{BENCHMARK['summary']['first_eval_ms']['median'] / 1000:.2f} s"
EDIT_LABEL = f"edit one input + recalc: {BENCHMARK['summary']['value_total_p50_ms']['median']:.0f} ms"

W, H = 960, 480
S = 2  # supersample factor
FPS = 25
DUR = 11.6
NFRAMES = round(DUR * FPS)

# ---------------------------------------------------------------- palette
BG = (11, 12, 14)
PANEL = (19, 21, 25)
PANEL_HI = (26, 29, 34)
LINE = (38, 42, 49)
DIM = (96, 103, 114)
TEXT = (196, 202, 211)
WHITE = (238, 241, 245)
AMBER = (247, 166, 36)
ORANGE = (255, 128, 32)
BLUE = (66, 145, 255)
TEAL = (45, 212, 180)

# ---------------------------------------------------------------- timeline (s)
T_TYPE0, T_TYPE1 = 0.35, 1.25  # typing "=B2*C2" into D2
T_COMMIT = 1.35
T_WAVE0, T_WAVE1 = 1.45, 1.85  # fill handle wave over visible rows
T_SCROLL0, T_SCROLL1 = 1.85, 3.95  # scroll to the bottom of the column
T_FUSE0, T_FUSE1 = 4.2, 4.95
T_SWEEP0, T_SWEEP1 = 5.05, 5.6
T_SPLIT0, T_SPLIT1 = 5.6, 5.9
T_FLOW0, T_FLOW1 = 5.9, 6.7  # results flow into Total -> Tax -> Net
T_EDIT0 = 7.05  # dim everything except the input
T_RATE0, T_RATE1 = 7.4, 7.8  # 5% -> 7%
T_RIP0, T_RIP1 = 7.9, 8.75  # dependency ripple
T_CARD0, T_CARD1 = 9.0, 9.4  # end card in
T_LOOP0 = 10.95  # end card out, back to frame-0 state

N_ROWS = 1_000_000  # formulas filled down (D2:D1000001)
FORMULA = "=B2*C2"

# ---------------------------------------------------------------- layout (1x units)
GX, GY = 24, 70  # grid panel origin
COLS = [("", 80), ("A", 90), ("B", 66), ("C", 82), ("D", 212)]  # gutter + A..D
GW = sum(w for _, w in COLS)
HDR_H = 22
ROW_H = 26
VIS = 13  # visible sheet rows (incl. label row 1 when at top)
GH = HDR_H + VIS * ROW_H
FBAR_Y = 28
RX0, RX1 = 612, 904  # right column
CARD_H = 50
CARD_Y = {"Total": 196, "Rate": 256, "Tax": 316, "Net": 376}
LANE_X = 922


def col_x(i: int) -> int:
    return GX + sum(w for _, w in COLS[:i])


# ---------------------------------------------------------------- helpers
def clamp(x, a=0.0, b=1.0):
    return a if x < a else b if x > b else x


def prog(t, a, b):
    return clamp((t - a) / (b - a)) if b > a else float(t >= a)


def praw(t, a, b):
    """Unclamped progress, used for pulses that must travel off and fade."""
    return (t - a) / (b - a)


def ease(x):  # smoothstep-ish, cubic in-out
    x = clamp(x)
    return 4 * x * x * x if x < 0.5 else 1 - (-2 * x + 2) ** 3 / 2


def ease_out(x):
    x = clamp(x)
    return 1 - (1 - x) ** 3


def mix(c1, c2, a):
    """a=0 -> c1, a=1 -> c2."""
    a = clamp(a)
    return tuple(int(round(c1[i] + (c2[i] - c1[i]) * a)) for i in range(3))


def fade(c, a, base=PANEL):
    """Colour c at visibility a over background base."""
    return mix(base, c, a)


@lru_cache(maxsize=None)
def font(kind: str, size: float, wght: int = 400):
    path = BUILD / ("JetBrainsMono.ttf" if kind == "mono" else "Satoshi-Variable.ttf")
    f = ImageFont.truetype(str(path), int(round(size * S)))
    try:
        f.set_variation_by_axes([wght])
    except Exception:
        pass
    return f


def ensure_fonts():
    from fontTools.ttLib import TTFont

    BUILD.mkdir(parents=True, exist_ok=True)
    font_dir = Path(os.environ.get("FORMUALIZER_ANIMATION_FONTS", str(HERE / "fonts"))).expanduser()
    for name in ("JetBrainsMono", "Satoshi-Variable"):
        source = font_dir / f"{name}.woff2"
        if not source.is_file():
            raise SystemExit(f"Missing {source}; supply --font-dir (see assets/animation/README.md for font sources/licenses)")
        # Reconvert these small files so changing the supplied font cannot leave
        # a stale cached TTF. Font binaries are intentionally not redistributed.
        f = TTFont(str(source))
        f.flavor = None
        f.save(str(BUILD / f"{name}.ttf"))


class Canvas:
    def __init__(self):
        self.im = Image.new("RGB", (W * S, H * S), BG)
        self.d = ImageDraw.Draw(self.im)
        self.glow = Image.new("RGB", (W * S, H * S), (0, 0, 0))
        self.g = ImageDraw.Draw(self.glow)

    # all coordinates in 1x units
    def rect(self, x0, y0, x1, y1, fill=None, outline=None, r=0, width=1, glow=None):
        box = [x0 * S, y0 * S, max(x0 * S, x1 * S - 1), max(y0 * S, y1 * S - 1)]
        if r:
            r = min(r, (box[2] - box[0]) / S / 2, (box[3] - box[1]) / S / 2)
        if r:
            self.d.rounded_rectangle(box, r * S, fill=fill, outline=outline, width=int(width * S))
        else:
            self.d.rectangle(box, fill=fill, outline=outline, width=int(width * S))
        if glow is not None:
            gcol, gw = glow
            if r:
                self.g.rounded_rectangle(box, r * S, outline=gcol, width=int(gw * S))
            else:
                self.g.rectangle(box, outline=gcol, width=int(gw * S))

    def glow_rect(self, x0, y0, x1, y1, col, r=0):
        box = [x0 * S, y0 * S, x1 * S - 1, y1 * S - 1]
        self.g.rounded_rectangle(box, r * S, fill=col) if r else self.g.rectangle(box, fill=col)

    def line(self, pts, fill, width=1, glow=None):
        p = [(x * S, y * S) for x, y in pts]
        self.d.line(p, fill=fill, width=max(1, int(width * S)), joint="curve")
        if glow is not None:
            self.g.line(p, fill=glow[0], width=int(glow[1] * S), joint="curve")

    def text(self, x, y, s, f, fill, anchor="la", glow=None):
        self.d.text((x * S, y * S), s, font=f, fill=fill, anchor=anchor)
        if glow is not None:
            self.g.text((x * S, y * S), s, font=f, fill=glow, anchor=anchor)

    def textw(self, s, f):
        return self.d.textlength(s, font=f) / S

    def ellipse(self, cx, cy, r, outline=None, width=1, fill=None, glow=None):
        box = [(cx - r) * S, (cy - r) * S, (cx + r) * S, (cy + r) * S]
        self.d.ellipse(box, outline=outline, width=int(width * S), fill=fill)
        if glow is not None:
            self.g.ellipse(box, outline=glow, width=int(width * 2 * S))

    def finish(self):
        blurred = self.glow.filter(ImageFilter.GaussianBlur(9 * S))
        tight = self.glow.filter(ImageFilter.GaussianBlur(2.5 * S))
        im = ImageChops.add(self.im, blurred)
        im = ImageChops.add(im, tight.point(lambda v: v * 0.55))
        return im.resize((W, H), Image.LANCZOS)


# ---------------------------------------------------------------- static background
@lru_cache(maxsize=1)
def background() -> Image.Image:
    rng = np.random.default_rng(7)
    yy, xx = np.mgrid[0 : H * S, 0 : W * S].astype(np.float32)
    cx, cy = W * S * 0.42, H * S * 0.45
    d = np.sqrt(((xx - cx) / (W * S)) ** 2 + ((yy - cy) / (H * S)) ** 2)
    vign = np.clip(1.0 - d * 0.9, 0.55, 1.0)
    base = np.array(BG, np.float32)[None, None, :] * vign[..., None] * 1.25
    # coarse, low-amplitude grain: blurred so it survives palette quantisation
    noise = rng.normal(0, 1, (H * S // 2, W * S // 2)).astype(np.float32)
    n = Image.fromarray(np.clip(128 + noise * 40, 0, 255).astype(np.uint8)).resize((W * S, H * S), Image.BILINEAR)
    n = (np.asarray(n, np.float32) - 128) / 128.0
    base += n[..., None] * 2.2
    # faint diagonal circuit-ish hairlines like the banner
    img = Image.fromarray(np.clip(base, 0, 255).astype(np.uint8))
    dr = ImageDraw.Draw(img)
    for y in (452, 458):
        dr.line([(0, y * S), (W * S, y * S)], fill=(22, 24, 28), width=S)
    return img


# ---------------------------------------------------------------- sheet data
def cell_vals(row: int):
    h = (row * 2654435761) & 0xFFFFFFFF
    units = 1 + (h % 40)
    price = 1 + ((h >> 8) % 5900) / 100
    return units, price


def fmt_money(v):
    return f"{v:,.2f}"


TOTAL = 600_418_227.35
TAX5 = TOTAL * 0.05
TAX7 = TOTAL * 0.07


# ---------------------------------------------------------------- scene state
def scroll_top(t):
    """First data row shown (sheet row number of the top visible body row)."""
    p = prog(t, T_SCROLL0, T_SCROLL1)
    if p <= 0:
        return 1.0
    last_top = N_ROWS + 2 - VIS  # so the last visible row is 1,000,001
    # ease in log space: races through magnitudes, lands softly
    if p >= 1:
        return float(last_top)
    e = ease(p)
    return 10 ** (e * math.log10(last_top))


def filled_rows(t):
    """How many rows of D contain the formula (for the counter)."""
    if t < T_COMMIT:
        return 0
    if t < T_WAVE1:
        return 1 + int((VIS - 2) * prog(t, T_WAVE0, T_WAVE1))
    if t < T_SCROLL1:
        top = scroll_top(t)
        return min(N_ROWS, int(top + VIS - 2))
    return N_ROWS


def seg_fade(t, a0, a1, b0, b1):
    return min(prog(t, a0, a1), 1 - prog(t, b0, b1))


# ---------------------------------------------------------------- drawing
def draw_formula_bar(c: Canvas, t, vis):
    y0, y1 = FBAR_Y, FBAR_Y + 30
    c.rect(GX, y0, GX + GW, y1, fill=fade(PANEL, vis, BG), outline=fade(LINE, vis, BG), r=6)
    mono = font("mono", 12.5, 500)
    c.text(GX + 14, (y0 + y1) / 2, "D2", mono, fade(TEXT, vis, BG), anchor="lm")
    c.line([(GX + 50, y0 + 7), (GX + 50, y1 - 7)], fade(LINE, vis, BG))
    c.text(GX + 62, (y0 + y1) / 2, "fx", font("mono", 12, 400), fade(DIM, vis, BG), anchor="lm")
    n = int(len(FORMULA) * prog(t, T_TYPE0, T_TYPE1) + 1e-6)
    s = FORMULA[:n] if t < T_EDIT0 - 0.3 else FORMULA
    typed_col = fade(AMBER, vis, BG)
    c.text(GX + 88, (y0 + y1) / 2, s, font("mono", 13, 600), typed_col, anchor="lm")
    if T_TYPE0 - 0.2 < t < T_COMMIT and int(t * 4) % 2 == 0:
        x = GX + 88 + c.textw(s, font("mono", 13, 600)) + 1
        c.rect(x, y0 + 8, x + 1.5, y1 - 8, fill=typed_col)
    # right side of the bar: brand tag
    c.text(GX + GW - 12, (y0 + y1) / 2, "formualizer", font("mono", 11, 500), fade(DIM, vis * 0.9, BG), anchor="rm")


def draw_grid(c: Canvas, t, vis):
    """The sheet. vis scales overall brightness (dimming during edit)."""
    top = scroll_top(t)
    first = int(math.floor(top))
    frac = top - first
    speed = scroll_top(t + 1 / FPS) - top  # rows per frame

    fuse = ease(prog(t, T_FUSE0, T_FUSE1))
    split = ease(prog(t, T_SPLIT0, T_SPLIT1))
    fused = fuse * (1 - split)  # 1 while one capsule
    valued = t >= T_SPLIT0 + 0.08

    pc = lambda col, a=1.0: fade(col, vis * a)  # noqa: E731
    # panel
    c.rect(GX, GY, GX + GW, GY + GH, fill=PANEL, outline=LINE, r=6)
    # column header
    c.rect(GX + 1, GY + 1, GX + GW - 1, GY + HDR_H, fill=PANEL_HI)
    hf = font("mono", 11, 500)
    for i, (name, w) in enumerate(COLS):
        x = col_x(i)
        if name:
            active = name == "D"
            col = AMBER if active and t >= T_TYPE0 - 0.2 else DIM
            c.text(x + w / 2, GY + HDR_H / 2 + 1, name, hf, pc(col, 0.95), anchor="mm")
        if i:
            c.line([(x, GY + 1), (x, GY + GH - 1)], LINE)
    c.line([(GX + 1, GY + HDR_H), (GX + GW - 1, GY + HDR_H)], LINE)

    body_y0 = GY + HDR_H
    cf = font("mono", 11.5, 400)
    lf = font("sans", 12, 600)
    wave_p = prog(t, T_WAVE0, T_WAVE1)
    late = []  # D values, drawn after the capsule so they emerge from it
    for k in range(-1, VIS + 1):
        row = first + k
        y = body_y0 + (k - frac) * ROW_H
        if y < body_y0 - ROW_H or y > GY + GH or row < 1:
            continue
        yc = y + ROW_H / 2
        # horizontal line
        if body_y0 < y + ROW_H < GY + GH:
            c.line([(GX + 1, y + ROW_H), (GX + GW - 1, y + ROW_H)], LINE)
        if not (body_y0 - 4 < yc < GY + GH + 4):
            continue
        # gutter row number
        c.text(col_x(1) - 8, yc, f"{row:,}", cf, pc(DIM, 0.9), anchor="rm")
        if row == 1:
            for i, lab in ((1, "Item"), (2, "Units"), (3, "Price"), (4, "Revenue")):
                c.text(col_x(i) + 10, yc, lab, lf, pc(TEXT, 0.85), anchor="lm")
            continue
        units, price = cell_vals(row)
        c.text(col_x(1) + 10, yc, f"P-{row:07d}"[: 10], cf, pc(TEXT, 0.55), anchor="lm")
        c.text(col_x(3) - 10, yc, f"{units}", cf, pc(TEXT, 0.7), anchor="rm")
        c.text(col_x(4) - 10, yc, f"{price:.2f}", cf, pc(TEXT, 0.7), anchor="rm")

        # ---- column D
        dx0, dx1 = col_x(4), col_x(5)
        has_formula = (row == 2 and t >= T_COMMIT) or (
            t >= T_WAVE0 and (row - 2) < (VIS - 1) * wave_p + 0.999
        ) or t >= T_SCROLL0
        if row == 2 and t < T_COMMIT:
            # typing directly in the cell
            n = int(len(FORMULA) * prog(t, T_TYPE0, T_TYPE1) + 1e-6)
            if t >= T_TYPE0 - 0.2:
                c.text(dx0 + 10, yc, FORMULA[:n], font("mono", 11.5, 600), pc(AMBER), anchor="lm")
            continue
        if not has_formula:
            continue
        if t >= T_FUSE0 and not valued:
            continue  # drawn by the capsule
        if valued:
            late.append((yc, units * price))
            continue
        else:
            # freshly filled formula: relative references per row
            age = t - (T_WAVE0 + (row - 2) / max(1, VIS - 1) * (T_WAVE1 - T_WAVE0)) if t < T_SCROLL0 else 1
            flash = clamp(1 - age / 0.35) if age >= 0 else 0
            s = f"=B{row}*C{row}"
            c.text(dx0 + 10, yc, s, font("mono", 11.5, 500), pc(mix(AMBER, WHITE, flash * 0.5), 0.8), anchor="lm",
                   glow=mix((0, 0, 0), ORANGE, flash * 0.6) if flash > 0 else None)

    # selection outline on D2 while typing / filling
    if T_TYPE0 - 0.2 <= t < T_SCROLL0 + 0.1:
        a = 1 - prog(t, T_SCROLL0 - 0.1, T_SCROLL0 + 0.1)
        y_top = body_y0 + (2 - top) * ROW_H
        nrows = 1 + (VIS - 2) * wave_p if t >= T_WAVE0 else 1
        c.rect(col_x(4) + 1, y_top + 1, col_x(5) - 1, y_top + nrows * ROW_H, outline=fade(AMBER, a * vis), width=1.5)
        # fill handle
        hy = y_top + nrows * ROW_H
        c.rect(col_x(5) - 5, hy - 4, col_x(5) + 1, hy + 2, fill=fade(AMBER, a * vis, BG))

    # motion smear while racing
    if speed > 1.2:
        # gentle smear while accelerating; at full speed the rows collapse into
        # vertical streaks (also keeps the GIF small: near-identical frames)
        k = min(28, 1 + speed ** 0.6 * 0.9)
        x0, y0, x1, y1 = (GX + 1) * S, int((body_y0 + 1) * S), (GX + GW - 1) * S, int((GY + GH - 2) * S)
        region = c.im.crop((x0, y0, x1, y1))
        sm = region.resize((region.width, max(4, int(region.height / k))), Image.BILINEAR).resize(region.size, Image.BILINEAR)
        region = Image.blend(region, sm, 0.85 * clamp((speed - 1.2) / 6) + 0.15 * clamp((speed - 40) / 200))
        c.im.paste(region, (x0, y0))
        # crisp, fixed row rules over the streaks so it still reads as a sheet
        la = clamp((speed - 4) / 20)
        if la > 0:
            for k2 in range(1, VIS):
                yy = body_y0 + k2 * ROW_H
                c.line([(GX + 1, yy), (GX + GW - 1, yy)], mix(PANEL, LINE, 0.5 + 0.5 * la))

    # ---- the fused capsule (one formula family) + sweep
    if t >= T_FUSE0 and t < T_SPLIT1:
        dx0, dx1 = col_x(4), col_x(5)
        yt, yb = body_y0 + 1, GY + GH - 1
        gap = (1 - fuse) * 3
        a = fuse * (1 - split)
        inset = 4 + 2 * fuse
        # per-row chips converging into one body
        if fuse < 0.999:
            for k in range(VIS):
                row = int(top) + k
                y0 = yt + k * ROW_H + gap
                y1 = yt + (k + 1) * ROW_H - gap
                cc = mix(PANEL, (60, 38, 12), 0.5 + 0.5 * fuse)
                c.rect(dx0 + inset, y0, dx1 - inset, y1, fill=cc, r=3)
                c.text(dx0 + 10 + inset, (y0 + y1) / 2, f"=B{row}*C{row}", font("mono", 11.5, 500),
                       mix(cc, AMBER, 0.8 * clamp(1 - fuse * 1.7)), anchor="lm")
        body = mix((60, 38, 12), (70, 44, 12), fuse)
        if split > 0:
            body = mix(body, PANEL, split)
        c.rect(dx0 + inset, yt + 3, dx1 - inset, yb - 3, fill=body if fuse >= 0.999 else None,
               outline=mix(PANEL, AMBER, a), r=8, width=1.5, glow=(mix((0, 0, 0), ORANGE, a * 0.9), 3))
        sp = prog(t, T_SWEEP0, T_SWEEP1)
        band_y = yt + (yb - yt + 40) * ease(sp) - 20 if sp > 0 else -1e9
        # results already written above the sweep: tint the body teal
        if sp > 0 and split < 1:
            fy = min(yb - 4, max(yt + 4, band_y - 14))
            if fy > yt + 6:
                tint = mix(body, (14, 58, 56), 0.9 * (1 - split))
                c.rect(dx0 + inset + 2, yt + 5, dx1 - inset - 2, fy, fill=tint, r=6)
        # label: amber/white until the band passes it, then teal (computed)
        if fuse > 0.5:
            la = clamp((fuse - 0.72) / 0.28) * clamp(1 - split * 1.6)
            midy = (yt + yb) / 2
            cx = (dx0 + dx1) / 2
            for yy, txt, fnt, col_pre, col_post, g in (
                (yt + 18, "ONE FORMULA FAMILY", font("mono", 9.5, 600), AMBER, (120, 230, 210), False),
                (midy - 12, FORMULA, font("mono", 20, 700), WHITE, (190, 255, 240), True),
                (midy + 14, "× 1,000,000 rows", font("mono", 11.5, 500), AMBER, (120, 230, 210), False),
            ):
                done = band_y - 16 > yy
                bgc = mix(body, (14, 58, 56), 0.9) if done else body
                col = col_post if done else col_pre
                c.text(cx, yy, txt, fnt, mix(bgc, col, la), anchor="mm",
                       glow=mix((0, 0, 0), TEAL if done else ORANGE, la * 0.55) if g else None)
        # sweep band
        if 0 < sp < 1:
            for i in range(14):
                yy = band_y - i * 2.2
                if yt + 4 < yy < yb - 4:
                    colr = mix(WHITE, BLUE, i / 14)
                    colr = mix(body, colr, (1 - i / 14) ** 1.3)
                    c.line([(dx0 + inset + 3, yy), (dx1 - inset - 3, yy)], colr, width=2.2)
            if yt < band_y < yb:
                c.glow_rect(dx0 + inset, band_y - 10, dx1 - inset, band_y + 3, mix((0, 0, 0), BLUE, 0.9), r=4)

    # D results, appearing all at once as the capsule releases them
    if late:
        dx1 = col_x(5)
        appear = ease(prog(t, T_SPLIT0, T_SPLIT1))
        fl = clamp(1 - (t - T_SPLIT1) / 0.5)
        for yc, v in late:
            col = mix(TEAL, WHITE, fl * 0.55)
            c.text(dx1 - 12, yc, fmt_money(v), font("mono", 11.5, 500), fade(col, vis * 0.92 * appear), anchor="rm",
                   glow=mix((0, 0, 0), TEAL, fl * 0.45 * vis * appear) if fl > 0 else None)
    return body_y0


def card(c: Canvas, name, formula, value, y, vis, accent=None, accent_a=0.0, value_col=TEXT, flash=0.0,
         value_a=1.0):
    x0, x1 = RX0, RX1
    edge = mix(LINE, accent, accent_a) if accent else LINE
    c.rect(x0, y, x1, y + CARD_H, fill=fade(PANEL, 1, BG), outline=fade(edge, vis, BG) if accent_a <= 0 else edge,
           r=7, width=1.2 + 0.6 * accent_a, glow=(mix((0, 0, 0), accent, accent_a * 0.8), 3) if accent and accent_a > 0 else None)
    c.text(x0 + 14, y + 17, name, font("sans", 14, 700), fade(TEXT, vis))
    c.text(x0 + 14, y + 36, formula, font("mono", 10.5, 400), fade(DIM, vis))
    if value:
        col = mix(value_col, WHITE, flash * 0.7)
        c.text(x1 - 14, y + CARD_H / 2 + 1, value, font("mono", 14, 600), fade(col, vis * value_a), anchor="rm",
               glow=mix((0, 0, 0), value_col, flash * 0.7) if flash > 0 else None)


def pulse_line(c: Canvas, pts, p, col, vis_base, width=1.4):
    """Polyline with a travelling light pulse at progress p (0..1)."""
    c.line(pts, fade(mix(LINE, col, 0.25), vis_base, BG), width=width)
    if not (0 < p < 1.15):
        return
    segs = list(zip(pts[:-1], pts[1:]))
    lens = [math.dist(a, b) for a, b in segs]
    total = sum(lens)
    head = min(1.0, p) * total
    tail = max(0.0, head - 0.55 * total)
    acc = 0.0
    lit = []
    for (a, b), L in zip(segs, lens):
        s0, s1 = max(tail, acc), min(head, acc + L)
        if s1 > s0:
            ta, tb = (s0 - acc) / L, (s1 - acc) / L
            lit.append(((a[0] + (b[0] - a[0]) * ta, a[1] + (b[1] - a[1]) * ta),
                        (a[0] + (b[0] - a[0]) * tb, a[1] + (b[1] - a[1]) * tb)))
        acc += L
    fadeout = 1 - clamp((p - 1) / 0.15)
    for a, b in lit:
        c.line([a, b], mix(BG, col, fadeout), width=width + 0.6, glow=(mix((0, 0, 0), col, fadeout), 3))


def draw_right(c: Canvas, t, dim_other):
    """Counter + dependency cards. dim_other: 0..1 dim for non-dependents."""
    vis_o = 1 - 0.62 * dim_other
    # ---- counter
    rows = filled_rows(t)
    racing = T_WAVE0 <= t < T_SCROLL1 + 0.2
    c.text(RX0, 80, "FORMULAS FILLED", font("mono", 10, 600), fade(DIM, vis_o, BG))
    nf = font("mono", 34, 700)
    col = AMBER if rows > 0 else DIM
    done = prog(t, T_SCROLL1 - 0.02, T_SCROLL1 + 0.3)
    c.text(RX0 - 2, 116, f"{rows:,}", nf, fade(mix(col, WHITE, 0.15 if racing else 0), vis_o, BG), anchor="lm",
           glow=mix((0, 0, 0), ORANGE, (0.35 if racing else 0) + 0.5 * (1 - done) * (done > 0)) if rows else None)
    c.text(RX0, 146, f"D2:D{1 + max(rows, 1):,}".replace(",", "") if rows else "D2", font("mono", 11, 400),
           fade(DIM, vis_o, BG))
    # thin progress rule
    frac = rows / N_ROWS
    c.rect(RX0, 160, RX1, 162, fill=fade(LINE, vis_o, BG))
    if frac > 0:
        c.rect(RX0, 160, RX0 + (RX1 - RX0) * frac, 162, fill=fade(AMBER, vis_o, BG),
               glow=(mix((0, 0, 0), ORANGE, 0.5 * vis_o), 1) if racing else None)

    # ---- values
    calc = t >= T_FLOW0
    p_total = prog(t, T_FLOW0 + 0.1, T_FLOW0 + 0.35)
    p_tax = prog(t, T_FLOW0 + 0.35, T_FLOW0 + 0.55)
    p_net = prog(t, T_FLOW0 + 0.55, T_FLOW0 + 0.75)
    new_rate = t >= T_RATE0 + 0.2
    tax = TAX7 if t >= T_RIP0 + 0.3 else TAX5
    net = TOTAL - tax
    fl_total = clamp(1 - (t - (T_FLOW0 + 0.35)) / 0.4) if p_total >= 1 else 0
    fl_tax = clamp(1 - (t - (T_FLOW0 + 0.55)) / 0.4) if p_tax >= 1 else 0
    fl_net = clamp(1 - (t - (T_FLOW0 + 0.75)) / 0.4) if p_net >= 1 else 0
    if t >= T_RIP0:
        fl_tax = clamp(1 - abs(t - (T_RIP0 + 0.3)) / 0.35)
        fl_net = clamp(1 - abs(t - (T_RIP0 + 0.55)) / 0.35)

    # ---- dependency edges (lane on the right)
    ly = lambda n: CARD_Y[n] + CARD_H / 2  # noqa: E731
    edge_total_tax = [(RX1, ly("Total")), (LANE_X, ly("Total")), (LANE_X, ly("Tax") - 6), (RX1, ly("Tax") - 6)]
    edge_rate_tax = [(RX1, ly("Rate")), (LANE_X - 8, ly("Rate")), (LANE_X - 8, ly("Tax") + 2), (RX1, ly("Tax") + 2)]
    edge_tax_net = [(RX1, ly("Tax") + 10), (LANE_X, ly("Tax") + 10), (LANE_X, ly("Net")), (RX1, ly("Net"))]
    pf = praw(t, T_FLOW0 + 0.3, T_FLOW0 + 0.6)
    pf2 = praw(t, T_FLOW0 + 0.5, T_FLOW0 + 0.8)
    pr = praw(t, T_RIP0 + 0.0, T_RIP0 + 0.35)
    pr2 = praw(t, T_RIP0 + 0.25, T_RIP0 + 0.6)
    pulse_line(c, edge_total_tax, pf if t < T_EDIT0 else 0, BLUE, vis_o)
    pulse_line(c, edge_rate_tax, pf if t < T_EDIT0 else pr, BLUE if t < T_EDIT0 else TEAL, 1)
    pulse_line(c, edge_tax_net, pf2 if t < T_EDIT0 else pr2, BLUE if t < T_EDIT0 else TEAL, 1)

    # ---- cards
    card(c, "Total", "=SUM(D2:D1000001)", fmt_money(TOTAL) if p_total > 0 else "", CARD_Y["Total"], vis_o,
         value_col=TEAL, flash=fl_total, value_a=p_total)
    rate_edit = prog(t, T_EDIT0, T_EDIT0 + 0.3) * (1 - prog(t, T_CARD0, T_CARD1))
    rtxt = "5%"
    if T_RATE0 <= t < T_RATE0 + 0.2:
        rtxt = ""
    elif new_rate:
        rtxt = "7%"
    card(c, "Rate", "input", rtxt, CARD_Y["Rate"], 1, accent=AMBER, accent_a=rate_edit, value_col=AMBER,
         flash=clamp(1 - abs(t - (T_RATE0 + 0.25)) / 0.3) if t >= T_RATE0 else 0)
    if T_RATE0 - 0.1 <= t < T_RATE0 + 0.5 and int(t * 6) % 2 == 0:
        x = RX1 - 12
        c.rect(x, CARD_Y["Rate"] + 15, x + 1.5, CARD_Y["Rate"] + CARD_H - 14, fill=AMBER)
    dep_col = TEAL if t >= T_EDIT0 else TEAL
    card(c, "Tax", "=Total*Rate", fmt_money(tax) if p_tax > 0 else "", CARD_Y["Tax"], 1, value_col=dep_col,
         flash=fl_tax, value_a=p_tax, accent=TEAL, accent_a=fl_tax * 0.8 if t >= T_RIP0 else 0)
    card(c, "Net", "=Total-Tax", fmt_money(net) if p_net > 0 else "", CARD_Y["Net"], 1, value_col=dep_col,
         flash=fl_net, value_a=p_net, accent=TEAL, accent_a=fl_net * 0.8 if t >= T_RIP0 else 0)

    # ---- ripple rings from the rate cell
    if T_RIP0 <= t < T_RIP1:
        cx, cy = RX1 - 30, ly("Rate")
        for i in range(3):
            q = prog(t, T_RIP0 + i * 0.12, T_RIP0 + i * 0.12 + 0.6)
            if 0 < q < 1:
                r = 8 + 44 * ease_out(q)
                c.ellipse(cx, cy, r, outline=mix(PANEL, TEAL, (1 - q) * 0.6), width=1.2,
                          glow=mix((0, 0, 0), TEAL, (1 - q) * 0.35))
    return calc


def draw_flow_from_column(c: Canvas, t, vis):
    """Pulse from the D column into the Total card."""
    y = CARD_Y["Total"] + CARD_H / 2
    pts = [(GX + GW, y), (RX0, y)]
    pulse_line(c, pts, praw(t, T_FLOW0, T_FLOW0 + 0.3), BLUE, vis)


def draw_caption(c: Canvas, t, dim_other):
    caps = [
        (0.0, T_FUSE0 - 0.15, "01", "fill =B2*C2 down a million rows"),
        (T_FUSE0 - 0.15, T_EDIT0 - 0.1, "02", "one formula family · one pass · every result at once"),
        (T_EDIT0 - 0.1, T_CARD0 + 0.2, "03", "edit one input → only its dependents recalculate"),
    ]
    y = 452
    for a, b, num, s in caps:
        v = min(prog(t, a, a + 0.25) if a > 0 else 1, 1 - prog(t, b - 0.2, b))
        if a == 0.0:  # caption 01 is visible at t=0 (loop seam)
            v = 1 - prog(t, b - 0.2, b)
        if v <= 0:
            continue
        c.text(GX, y, num, font("mono", 12, 700), fade(AMBER, v, BG), anchor="lm")
        c.text(GX + 28, y, s, font("sans", 14, 500), fade(TEXT, v, BG), anchor="lm")


def draw_untouched_tag(c: Canvas, t):
    a = prog(t, T_RIP0 + 0.2, T_RIP0 + 0.5) * (1 - prog(t, T_CARD0, T_CARD1))
    if a <= 0:
        return
    f = font("mono", 10.5, 600)
    s = "not recomputed"
    w = c.textw(s, f) + 22
    x0 = col_x(4) + 8
    y = GY + HDR_H + ROW_H * 6.5
    c.rect(x0, y - 12, x0 + w, y + 12, fill=fade((30, 33, 39), a, PANEL), outline=fade((58, 63, 72), a, PANEL), r=12)
    c.text(x0 + w / 2, y + 1, s, f, fade(TEXT, a * 0.75, PANEL), anchor="mm")


def render_scene(t: float) -> Image.Image:
    c = Canvas()
    c.im.paste(background())
    dim_other = ease(prog(t, T_EDIT0, T_EDIT0 + 0.35))
    vis_grid = 1 - 0.68 * dim_other
    draw_formula_bar(c, t, 1 - 0.55 * dim_other)
    draw_grid(c, t, vis_grid)
    draw_flow_from_column(c, t, 1 - 0.62 * dim_other)
    draw_right(c, t, dim_other)
    draw_untouched_tag(c, t)
    draw_caption(c, t, dim_other)
    return c.finish()


# ---------------------------------------------------------------- end card
def end_card(base: Image.Image, a: float, text_a: float | None = None) -> Image.Image:
    """Overlay the end card on base. a: backdrop dim (0..1); text_a: card text.

    By default the backdrop darkens first and the text follows, so the card
    never overlaps legible scene text in transitional frames."""
    if text_a is None:
        text_a = clamp((a - 0.45) / 0.55)
        a = clamp(a / 0.5)
    if a <= 0:
        return base
    blurred = base.filter(ImageFilter.GaussianBlur(6 * a))
    dark = Image.blend(blurred, Image.new("RGB", base.size, BG), 0.84 * a)
    c = Canvas()
    c.im = dark.resize((W * S, H * S), Image.BILINEAR)
    c.d = ImageDraw.Draw(c.im)
    if text_a <= 0:
        return c.finish()
    a = text_a
    k = ease_out(a)
    dy = (1 - k) * 10
    cx, cy = W / 2, H / 2 - 18 + dy

    f_big = font("sans", 50, 800)
    part1, sep, part2 = "1,000,000 formulas", "  ·  ", FIRST_CALC_LABEL
    w1, ws, w2 = c.textw(part1, f_big), c.textw(sep, f_big), c.textw(part2, f_big)
    x = cx - (w1 + ws + w2) / 2
    col_main = mix(BG, WHITE, k)
    c.text(x, cy, part1, f_big, col_main, anchor="lm")
    c.text(x + w1, cy, sep, f_big, mix(BG, DIM, k), anchor="lm")
    c.text(x + w1 + ws, cy, part2, f_big, mix(BG, AMBER, k), anchor="lm", glow=mix((0, 0, 0), ORANGE, 0.45 * k))
    # label line
    c.text(cx, cy - 48, "FIRST FULL CALCULATION · NATIVE", font("mono", 11, 600), mix(BG, DIM, k), anchor="mm")
    # gradient rule
    rw = 360 * k
    for i in range(int(rw)):
        u = i / max(1, rw)
        colr = mix(ORANGE, BLUE, u * 2) if u < 0.5 else mix(BLUE, TEAL, (u - 0.5) * 2)
        xx = cx - rw / 2 + i
        c.d.line([(xx * S, (cy + 38) * S), ((xx + 1) * S, (cy + 38) * S)], fill=mix(BG, colr, k), width=int(1.5 * S))
    c.text(cx, cy + 66, EDIT_LABEL, font("mono", 17, 600), mix(BG, TEAL, k), anchor="mm",
           glow=mix((0, 0, 0), TEAL, 0.25 * k))
    c.text(cx, cy + 112, "Formualizer — the fastest open-source spreadsheet engine", font("sans", 16, 500),
           mix(BG, TEXT, k), anchor="mm")
    c.text(cx, cy + 134, "native financial model · 2026-10-25 · load excluded", font("mono", 10, 400),
           mix(BG, DIM, k * 0.9), anchor="mm")
    return c.finish()


def render(t: float) -> Image.Image:
    card_in = ease(prog(t, T_CARD0, T_CARD1))
    loop_out = ease(prog(t, T_LOOP0, DUR))
    if t >= T_LOOP0:
        # text out -> hard cut of the dimmed backdrop -> frame-0 scene rises.
        # The cut (rather than a slow crossfade between two different scenes)
        # matters for gifski: its temporal diffing leaves faint ghosts of the
        # old scene if every frame changes only slightly.
        q = loop_out
        if q < 0.35:
            return end_card(render_scene(T_CARD1), 1.0, 1 - q / 0.35)
        if q < 0.45:
            return end_card(render_scene(T_CARD1), 1.0, 0.0)
        return end_card(render_scene(0.0), 1 - (q - 0.45) / 0.55, 0.0)
    base = render_scene(t)
    return end_card(base, card_in)


# ---------------------------------------------------------------- pipeline
def _render_frame(i: int):
    t = i / FPS
    render(t).save(FRAMES / f"f_{i:04d}.png", optimize=False, compress_level=1)
    return i


def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stderr[-2000:], file=sys.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd[:3])} ...")


HALF_RATE_WINDOW = (T_SCROLL0 + 0.55, T_SCROLL1 - 0.2)
HALF_RATE_STEP = 2
GIF_DITHER = "bayer:bayer_scale=5"
GIFSKI_LOCAL = BUILD / "tools" / "bin" / "gifski"  # cargo install gifski --root build/tools
GIFSKI_QUALITY = 90

STILL_TIMES = {"01-fill-race": 3.0, "02-fused-sweep": 5.35, "03-edit-ripple": 8.25, "04-end-card": 10.2}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 4) - 2))
    ap.add_argument("--font-dir", type=Path, help="directory containing JetBrainsMono.woff2 and Satoshi-Variable.woff2 (not bundled)")
    ap.add_argument("--stills-only", action="store_true")
    ap.add_argument("--frames-only", action="store_true")
    ap.add_argument("--gifski", action="store_true", help="encode the GIF with gifski instead of ffmpeg")
    ap.add_argument("--at", type=float, nargs="*", help="render ad-hoc stills at these times into build/")
    args = ap.parse_args()
    if args.jobs < 1:
        ap.error("--jobs must be positive")
    if args.font_dir is not None:
        os.environ["FORMUALIZER_ANIMATION_FONTS"] = str(args.font_dir.resolve())
    ensure_fonts()
    STILLS.mkdir(exist_ok=True)
    OUT.mkdir(exist_ok=True)

    if args.at:
        for tt in args.at:
            render(tt).save(BUILD / f"at_{tt:05.2f}.png")
        return
    for name, tt in STILL_TIMES.items():
        render(tt).save(STILLS / f"{name}.png")
    if args.stills_only:
        return

    if FRAMES.exists():
        shutil.rmtree(FRAMES)
    FRAMES.mkdir(parents=True)
    with Pool(args.jobs) as pool:
        for n, _ in enumerate(pool.imap_unordered(_render_frame, range(NFRAMES), chunksize=4), 1):
            if n % 50 == 0 or n == NFRAMES:
                print(f"frames {n}/{NFRAMES}", flush=True)
    if args.frames_only:
        return

    pattern = str(FRAMES / "f_%04d.png")
    palette = str(BUILD / "palette.png")
    gif = str(OUT / "formualizer-calc.gif")
    mp4 = str(OUT / "formualizer-calc.mp4")
    gifski = shutil.which("gifski") or (str(GIFSKI_LOCAL) if GIFSKI_LOCAL.exists() else None)
    if args.gifski:
        if not gifski:
            raise SystemExit("--gifski requested but gifski not found")
        # Smaller (~3.2 MB) but its inter-frame diffing leaves visible ghosts of
        # the previous scene in slow dark fades (end card -> loop). Opt-in only.
        run([gifski, "-q", "--fps", str(FPS), "--quality", str(GIFSKI_QUALITY), "--repeat", "0",
             "-o", gif, *sorted(str(p) for p in FRAMES.glob("f_*.png"))])
    else:
        # Variable frame timing: during the fastest part of the race the rows
        # are streaks anyway, so every other frame is kept at double duration.
        concat = BUILD / "gif-frames.txt"
        lines, i = ["ffconcat version 1.0"], 0
        while i < NFRAMES:
            step = HALF_RATE_STEP if HALF_RATE_WINDOW[0] <= i / FPS < HALF_RATE_WINDOW[1] else 1
            step = min(step, NFRAMES - i)
            lines += [f"file 'frames/f_{i:04d}.png'", f"duration {step / FPS:.4f}"]
            i += step
        lines.append(f"file 'frames/f_{NFRAMES - 1:04d}.png'")
        concat.write_text("\n".join(lines) + "\n")
        gif_in = ["-f", "concat", "-safe", "0", "-i", str(concat)]
        run(["ffmpeg", "-y", "-v", "error", *gif_in,
             "-vf", "palettegen=max_colors=256:stats_mode=full:reserve_transparent=0", palette])
        run(["ffmpeg", "-y", "-v", "error", *gif_in, "-i", palette,
             "-lavfi", f"[0:v][1:v]paletteuse=dither={GIF_DITHER}:diff_mode=rectangle",
             "-fps_mode", "vfr", "-loop", "0", gif])
    run(["ffmpeg", "-y", "-v", "error", "-framerate", str(FPS), "-i", pattern,
         "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18", "-preset", "slow",
         "-movflags", "+faststart", mp4])
    for p in (gif, mp4):
        print(f"{p}  {os.path.getsize(p) / 1e6:.2f} MB")


if __name__ == "__main__":
    main()
