"""Render calibrated Radiacode spectra as headless PNG camera frames."""
from datetime import datetime, timezone
from io import BytesIO

import numpy as np
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure


def spectrum_axis(spec):
    channels = np.arange(len(spec.counts), dtype=float)
    try:
        energies = float(spec.a0) + float(spec.a1) * channels + float(spec.a2) * channels ** 2
        if len(energies) > 1 and np.isfinite(energies).all() and np.all(np.diff(energies) > 0):
            return energies, "Energy (keV)"
    except (TypeError, ValueError, OverflowError):
        pass
    return channels, "Channel (calibration unavailable)"


def render_spectrum(spec, timestamp, *, scale="log"):
    """Return a 1200×600 PNG. Zeros remain zero; log display masks them."""
    if scale not in {"linear", "log"}:
        raise ValueError("Spectrum image scale must be linear or log")
    counts = np.asarray(spec.counts, dtype=float)
    if counts.ndim != 1 or not np.isfinite(counts).all() or np.any(counts < 0):
        raise ValueError("Spectrum counts must be finite nonnegative values")
    x, label = spectrum_axis(spec)
    fig = Figure(figsize=(12, 6), dpi=100, facecolor="#101827", layout="constrained")
    FigureCanvasAgg(fig)
    ax = fig.subplots()
    ax.set_facecolor("#101827")
    ax.tick_params(colors="#cbd5e1", labelsize=10)
    for spine in ax.spines.values():
        spine.set_color("#475569")
    ax.grid(True, color="#334155", alpha=0.6, linewidth=0.6)
    ax.set_xlabel(label, color="#cbd5e1", labelpad=10)
    ax.set_ylabel("Counts per channel" + (" (log scale)" if scale == "log" else ""), color="#cbd5e1")
    stamp = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    duration = spec.duration.total_seconds()
    ax.set_title(f"Radiacode spectrum\n{stamp}   |   Acquisition: {duration:,.0f} s   |   {len(counts)} channels",
                 color="#f8fafc", loc="left", fontsize=14, pad=18)
    if len(counts) and np.any(counts > 0):
        y = np.ma.masked_less_equal(counts, 0) if scale == "log" else counts
        ax.step(x, y, where="mid", color="#38bdf8", linewidth=1.2)
        if scale == "log":
            ax.set_yscale("log")
            ax.set_ylim(0.8, max(2, float(counts.max()) * 1.3))
        else:
            ax.set_ylim(0, float(counts.max()) * 1.1)
    else:
        ax.text(0.5, 0.5, "No counts accumulated yet", transform=ax.transAxes,
                ha="center", color="#cbd5e1", fontsize=16)
        ax.set_ylim(0, 1)
    if len(x) > 1:
        ax.set_xlim(float(x[0]), float(x[-1]))
    buffer = BytesIO()
    fig.savefig(buffer, format="png", facecolor=fig.get_facecolor())
    fig.clear()
    return buffer.getvalue()
