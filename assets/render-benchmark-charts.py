# /// script
# dependencies = ["matplotlib==3.10.8"]
# ///
"""Rebuild README charts: uv run assets/render-benchmark-charts.py.

Source: README.md / benchmarks/0.10-vs-0.9.3.md, LibreOffice comparison.
Times are the displayed rounded medians; speedups come from unrounded measurements.
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import NullLocator

ROWS = [
    ("Product lookups", 53.3, .35, "53.3 s", "0.35 s", "154×"),
    ("Sales report joins", 19.7, .50, "19.7 s", "0.50 s", "40×"),
    ("Enron cash-flow forecast", 1.57, .07, "1.57 s", "0.07 s", "23×"),
    ("Multi-criteria summary report", 6.11, .28, "6.11 s", "0.28 s", "22×"),
    ("Revenue rollup", 4.46, .58, "4.46 s", "0.58 s", "7.7×"),
    ("Service operations model", .212, .080, "212 ms", "80 ms", "2.6×"),
    ("Enron trading workbook", 1.31, .50, "1.31 s", "0.50 s", "2.6×"),
    ("100,000-row running balance", .525, .291, "525 ms", "291 ms", "1.8×"),
    ("100,000 copied formulas", .550, .347, "550 ms", "347 ms", "1.6×"),
]

for theme in ("dark", "light"):
    dark = theme == "dark"
    bg, fg, muted = ("#111315", "#F3F4F5", "#ABB2BC") if dark else ("#FFFFFF", "#17191D", "#566170")
    amber, gray = ("#F5A623", "#747E8E") if dark else ("#C67500", "#BAC1CB")
    fig = plt.figure(figsize=(19.2, 12), dpi=100, facecolor=bg)
    ax = fig.add_axes((.255, .12, .635, .70), facecolor=bg)
    ax.set_xscale("log")
    ax.set_xlim(.01, 100)
    ax.set_ylim(-.6, 8.6)
    ax.invert_yaxis()
    for i, (name, lo, fz, lo_text, fz_text, speed) in enumerate(ROWS):
        for value, y, color, label in ((lo, i-.17, gray, lo_text), (fz, i+.17, amber, fz_text)):
            ax.barh(y, value-.01, left=.01, height=.27, color=color, zorder=3)
            ax.annotate(label, (value, y), xytext=(7, 0), textcoords="offset points", va="center", fontsize=15, color=fg)
        ax.text(-.018, i, name, transform=ax.get_yaxis_transform(), ha="right", va="center", color=fg, fontsize=15)
        ax.text(1.07, i, speed, transform=ax.get_yaxis_transform(), ha="center", va="center", fontsize=19, weight="bold", color=amber, bbox=dict(boxstyle="round,pad=.35", edgecolor=amber, facecolor=bg, linewidth=1.5))
    ax.set_yticks([])
    ax.set_xticks([.01, .1, 1, 10, 100], ["10 ms", "100 ms", "1 s", "10 s", "100 s"])
    ax.xaxis.set_minor_locator(NullLocator())
    ax.tick_params(axis="x", colors=muted, labelsize=15, pad=12)
    ax.grid(axis="x", color=muted, alpha=.22, linestyle=(0, (2, 4)))
    for spine in ax.spines.values():
        spine.set_visible(False)
    fig.text(.035, .94, "Load + calculate, from a cold start", fontsize=30, weight="bold", color=fg)
    fig.text(.035, .898, "Lower is better. Median of 3 runs on the same shared 24-core Linux machine.", fontsize=17, color=muted)
    fig.text(.255, .85, "━  LibreOffice Calc 24.2", fontsize=17, color=muted)
    fig.text(.51, .85, "━  Formualizer 0.10", fontsize=17, color=amber)
    fig.text(.035, .025, "Time axis: logarithmic · Selected workloads; full results and methodology linked in the README.", fontsize=13, color=muted)
    fig.savefig(Path(__file__).with_name(f"benchmark-load-calculate-{theme}.png"), facecolor=bg)
    plt.close(fig)
