"""
OHLC Extractor — UI
====================
Fetches historical OHLC data for Indian stocks using yfinance.
Stocks are loaded from a CSV with columns:
    ISIN, Company_Name, BSE_Symbol, NSE_Symbol,
    Security_Code, Face_Value, ON_BSE, ON_NSE

Requirements:
    pip install yfinance pandas
    tkinter ships with Python on Windows.
"""

import csv
import os
import re
import queue
import threading
import time
from datetime import datetime, date

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

import pandas as pd
import yfinance as yf


# ===========================================================================
# THEME — Slate / Indigo
# ===========================================================================

C = {
    "bg":      "#090B13",
    "panel":   "#0E1120",
    "surface": "#151929",
    "border":  "#1F2540",
    "accent":  "#6366F1",
    "a_hover": "#818CF8",
    "a_dim":   "#1E1F5E",
    "ok":      "#34D399",
    "warn":    "#FBBF24",
    "err":     "#F87171",
    "text":    "#E2E8F0",
    "dim":     "#64748B",
    "faint":   "#2A3050",
    "hi":      "#1A1F3A",
    "gold":    "#F59E0B",
}

F_MONO = ("Consolas", 9)
F_BODY = ("Calibri", 10)
F_SM   = ("Calibri", 9)
F_BOLD = ("Calibri", 10, "bold")
F_BTN  = ("Calibri", 10, "bold")
F_HDR  = ("Consolas", 9, "bold")


# ===========================================================================
# YFINANCE FIXED VALUES
# ===========================================================================

YF_PERIODS   = ["1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","ytd","max"]
YF_INTERVALS = ["1d","5d","1wk","1mo","3mo"]   # daily+ only (intraday needs short period)

FETCH_MODES  = ["Date Range", "Past Years", "Period (yfinance)"]


# ===========================================================================
# TICKER BUILDER
# ===========================================================================

def build_ticker(row):
    """
    Prefer NSE (.NS). Fall back to BSE (.BO).
    Returns (ticker_string, exchange) or (None, None) if unavailable.
    """
    on_nse = str(row.get("ON_NSE", "")).strip().upper() in ("1", "Y", "YES", "TRUE", "X")
    on_bse = str(row.get("ON_BSE", "")).strip().upper() in ("1", "Y", "YES", "TRUE", "X")

    nse_sym = str(row.get("NSE_Symbol", "")).strip()
    bse_sym = str(row.get("BSE_Symbol", "")).strip()

    if on_nse and nse_sym:
        return f"{nse_sym}.NS", "NSE"
    if on_bse and bse_sym:
        return f"{bse_sym}.BO", "BSE"
    return None, None


def load_stocks_from_csv(path):
    """
    Returns list of dicts: {company, ticker, exchange}
    """
    stocks = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker, exchange = build_ticker(row)
                if ticker:
                    stocks.append({
                        "company": str(row.get("Company_Name", ticker)).strip(),
                        "ticker":  ticker,
                        "exchange": exchange,
                    })
    except Exception as e:
        raise RuntimeError(f"Could not read CSV: {e}")
    return stocks


# ===========================================================================
# SCRAPER ENGINE
# ===========================================================================

OHLC_COLS    = ["Open", "High", "Low", "Close", "Volume"]
OUTPUT_COLS  = ["Date", "Year", "Company", "Ticker", "Exchange",
                "Open", "High", "Low", "Close", "Volume"]
ENRICH_COLS  = ["Sector", "Industry", "MarketCap", "PE_Ratio",
                "52W_High", "52W_Low", "DividendYield"]
ALL_COLS     = OUTPUT_COLS + ENRICH_COLS


def fetch_info(ticker_str):
    """Fetch enrichment fields from yf.Ticker.info."""
    try:
        info = yf.Ticker(ticker_str).info
        return {
            "Sector":        info.get("sector", ""),
            "Industry":      info.get("industry", ""),
            "MarketCap":     info.get("marketCap", ""),
            "PE_Ratio":      info.get("trailingPE", ""),
            "52W_High":      info.get("fiftyTwoWeekHigh", ""),
            "52W_Low":       info.get("fiftyTwoWeekLow", ""),
            "DividendYield": info.get("dividendYield", ""),
        }
    except Exception:
        return {k: "" for k in ENRICH_COLS}


def fetch_history(ticker_str, cfg, log):
    """
    Fetch OHLCV history based on mode in cfg.
    Returns DataFrame or None.
    """
    stock = yf.Ticker(ticker_str)
    mode  = cfg["mode"]
    interval = cfg["interval"]

    try:
        if mode == "Date Range":
            data = stock.history(
                start=cfg["start_date"],
                end=cfg["end_date"],
                interval=interval,
                auto_adjust=True,
            )
        elif mode == "Past Years":
            start = datetime(datetime.now().year - cfg["past_years"], 1, 1)
            end   = datetime.now()
            data  = stock.history(
                start=start, end=end,
                interval=interval,
                auto_adjust=True,
            )
        else:  # Period
            data = stock.history(
                period=cfg["period"],
                interval=interval,
                auto_adjust=True,
            )
    except Exception as e:
        log(f"    Error fetching history: {e}", "err")
        return None

    if data is None or data.empty:
        return None

    missing = [c for c in OHLC_COLS if c not in data.columns]
    if missing:
        log(f"    Missing columns: {missing}", "warn")
        return None

    return data.reset_index()


def process_stock(stock_info, cfg, log, enrich_cache):
    """
    Fetch + process one stock. Returns list of row dicts.
    """
    company  = stock_info["company"]
    ticker   = stock_info["ticker"]
    exchange = stock_info["exchange"]

    data = fetch_history(ticker, cfg, log)
    if data is None:
        log(f"  No data — {company} ({ticker})", "dim")
        return []

    data["Company"]  = company
    data["Ticker"]   = ticker
    data["Exchange"] = exchange
    data["Year"]     = pd.to_datetime(data["Date"]).dt.year
    data["Date"]     = pd.to_datetime(data["Date"]).dt.date

    num_cols = ["Open", "High", "Low", "Close"]
    data[num_cols] = data[num_cols].round(2)

    rows = data.reindex(columns=OUTPUT_COLS).to_dict("records")

    if cfg["enrich"]:
        if ticker not in enrich_cache:
            enrich_cache[ticker] = fetch_info(ticker)
        info_row = enrich_cache[ticker]
        for r in rows:
            r.update(info_row)

    return rows


def perform_interval_analysis(full_df, iv_start, iv_end):
    """
    For each (Company, Year) compute pct change within MM-DD interval.
    Returns (pivot_df, agg_df) or (None, None).
    """
    results = []
    for (company, year), group in full_df.groupby(["Company", "Year"]):
        start_str = f"{year}-{iv_start}"
        end_str   = f"{year}-{iv_end}"
        sub = group[
            (group["Date"].astype(str) >= start_str) &
            (group["Date"].astype(str) <= end_str)
        ]
        if len(sub) < 2:
            continue
        o = sub.iloc[0]["Open"]
        c = sub.iloc[-1]["Close"]
        if pd.isna(o) or o == 0:
            continue
        results.append({
            "Company": company,
            "Year":    year,
            "Pct_Change": ((c - o) / o) * 100,
        })

    if not results:
        return None, None

    res_df  = pd.DataFrame(results)
    pivot   = res_df.pivot(index="Year", columns="Company", values="Pct_Change")
    agg     = (res_df.groupby("Company")["Pct_Change"]
                     .agg(["mean", "min", "max", "std"])
                     .reset_index()
                     .rename(columns={"mean": "Avg_%", "min": "Worst_%",
                                      "max": "Best_%",  "std": "StdDev_%"}))
    agg["Trend"] = agg["Avg_%"].apply(lambda x: "Bull" if x > 0 else "Bear")
    return pivot, agg


# ===========================================================================
# SMALL WIDGET HELPERS
# ===========================================================================

def _lbl(parent, text, row, col=0):
    tk.Label(
        parent, text=text,
        font=F_HDR, fg=C["faint"], bg=C["panel"],
    ).grid(row=row, column=col, padx=20, pady=(14, 2), sticky="w")


def _hint(parent, text, row):
    tk.Label(
        parent, text=text,
        font=("Calibri", 8),
        fg=C["faint"], bg=C["panel"],
    ).grid(row=row, column=0, padx=20, pady=(0, 0), sticky="w")


def _gap(parent, row, h=8):
    tk.Frame(parent, bg=C["panel"], height=h).grid(
        row=row, column=0, columnspan=2, sticky="ew"
    )


def _ghost_btn(parent, text, cmd):
    return tk.Button(
        parent, text=text,
        font=("Calibri", 9),
        fg=C["dim"], bg=C["surface"],
        activebackground=C["hi"],
        activeforeground=C["text"],
        relief="flat", bd=0,
        padx=10, pady=5,
        cursor="hand2",
        command=cmd,
    )


def _entry(parent, var, row, col=0, colspan=1, width=None, ipady=7):
    kw = dict(
        textvariable=var,
        font=F_BODY,
        bg=C["surface"], fg=C["text"],
        insertbackground=C["text"],
        relief="flat", bd=0,
        highlightthickness=1,
        highlightbackground=C["border"],
        highlightcolor=C["accent"],
    )
    if width:
        kw["width"] = width
    e = tk.Entry(parent, **kw)
    e.grid(row=row, column=col, columnspan=colspan,
           padx=(20 if col == 0 else 4, 20), pady=(2, 0),
           sticky="ew", ipady=ipady)
    return e


def _combo(parent, var, values, row, col=0, colspan=1, width=12):
    cb = ttk.Combobox(
        parent, textvariable=var,
        values=values, state="readonly",
        font=F_BODY, width=width,
    )
    cb.grid(row=row, column=col, columnspan=colspan,
            padx=(20 if col == 0 else 4, 4), pady=(2, 0), sticky="w")
    return cb


def _stat(parent, label, value, col):
    f = tk.Frame(parent, bg=C["panel"])
    f.grid(row=0, column=col, sticky="ew", padx=1)
    tk.Label(f, text=label, font=("Consolas", 7),
             fg=C["faint"], bg=C["panel"]).pack(pady=(10, 0))
    v = tk.Label(f, text=value, font=("Consolas", 12, "bold"),
                 fg=C["text"], bg=C["panel"])
    v.pack(pady=(0, 10))
    return v


# ===========================================================================
# MAIN APPLICATION
# ===========================================================================

class App(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("OHLC Extractor — Indian Markets")
        self.configure(bg=C["bg"])
        self.minsize(860, 660)

        self._stop       = threading.Event()
        self._q          = queue.Queue()
        self._stats      = {"done": 0, "total": 0, "records": 0}
        self._start_time = None
        self._stocks     = []          # loaded from CSV

        # ---- Variables ----
        self.v_stocks_path  = tk.StringVar(value="")
        self.v_stocks_count = tk.StringVar(value="No file loaded")

        self.v_mode      = tk.StringVar(value=FETCH_MODES[0])
        self.v_start     = tk.StringVar(value=date.today().replace(year=date.today().year - 1).strftime("%Y-%m-%d"))
        self.v_end       = tk.StringVar(value=date.today().strftime("%Y-%m-%d"))
        self.v_past_yrs  = tk.IntVar(value=5)
        self.v_period    = tk.StringVar(value="5y")
        self.v_interval  = tk.StringVar(value="1d")

        self.v_do_analysis = tk.BooleanVar(value=False)
        self.v_iv_start    = tk.StringVar(value="10-01")
        self.v_iv_end      = tk.StringVar(value="10-31")

        self.v_enrich    = tk.BooleanVar(value=False)
        self.v_separate  = tk.BooleanVar(value=False)
        self.v_filename  = tk.StringVar(value="ohlc_output")
        self.v_output    = tk.StringVar(value=os.path.join(os.getcwd(), "ohlc_data"))

        # ttk style
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TCombobox",
            fieldbackground=C["surface"],
            background=C["surface"],
            foreground=C["text"],
            selectbackground=C["accent"],
            borderwidth=0,
        )
        style.configure("OHLC.Horizontal.TProgressbar",
            troughcolor=C["surface"],
            background=C["accent"],
            thickness=3, borderwidth=0,
        )

        self._build()
        self._poll()
        self._on_mode_change()    # set initial widget visibility

        self.update_idletasks()
        w, h = 980, 720
        x = (self.winfo_screenwidth()  - w) // 2
        y = (self.winfo_screenheight() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    # -------------------------------------------------------------------------
    # BUILD
    # -------------------------------------------------------------------------

    def _build(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=0)
        self.rowconfigure(1, weight=1)
        self._build_header()
        self._build_body()

    def _build_header(self):
        bar = tk.Frame(self, bg=C["accent"], height=2)
        bar.grid(row=0, column=0, sticky="new")

        h = tk.Frame(self, bg=C["panel"], height=56)
        h.grid(row=0, column=0, sticky="ew")
        h.columnconfigure(1, weight=1)
        h.grid_propagate(False)

        tk.Label(h, text="OHLC  EXTRACTOR",
                 font=("Consolas", 13, "bold"),
                 fg=C["accent"], bg=C["panel"],
                 ).grid(row=0, column=0, padx=22, pady=16, sticky="w")

        tk.Label(h, text="Indian Markets  •  Yahoo Finance",
                 font=("Calibri", 9),
                 fg=C["dim"], bg=C["panel"],
                 ).grid(row=0, column=1, padx=4, sticky="w")

        self._status_lbl = tk.Label(h, text="  IDLE  ",
                                    font=("Consolas", 8, "bold"),
                                    fg=C["dim"], bg=C["surface"],
                                    padx=8, pady=4)
        self._status_lbl.grid(row=0, column=2, padx=22, sticky="e")

    def _build_body(self):
        body = tk.Frame(self, bg=C["bg"])
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=0, minsize=320)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)
        self._build_left(body)
        self._build_right(body)

    # ---- LEFT PANEL (scrollable) ----

    def _build_left(self, parent):
        container = tk.Frame(parent, bg=C["panel"])
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        canvas = tk.Canvas(container, bg=C["panel"], highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")

        vsb = tk.Scrollbar(container, orient="vertical", command=canvas.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        left = tk.Frame(canvas, bg=C["panel"])
        left.columnconfigure(0, weight=1)
        left.columnconfigure(1, weight=1)

        win_id = canvas.create_window((0, 0), window=left, anchor="nw")

        left.bind("<Configure>",
                  lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(win_id, width=e.width))

        def _mw(e): canvas.yview_scroll(int(-1*(e.delta/120)), "units")
        canvas.bind("<Enter>", lambda e: canvas.bind_all("<MouseWheel>", _mw))
        canvas.bind("<Leave>", lambda e: canvas.unbind_all("<MouseWheel>"))

        r = 0

        # ── Stock CSV ──────────────────────────────────────────
        _lbl(left, "STOCKS CSV", r); r += 1

        file_row = tk.Frame(left, bg=C["panel"])
        file_row.grid(row=r, column=0, columnspan=2,
                      padx=20, pady=(2, 0), sticky="ew"); r += 1
        file_row.columnconfigure(0, weight=1)

        tk.Entry(
            file_row, textvariable=self.v_stocks_path,
            font=F_BODY, bg=C["surface"], fg=C["text"],
            insertbackground=C["text"],
            relief="flat", bd=0,
            highlightthickness=1,
            highlightbackground=C["border"],
            highlightcolor=C["accent"],
            state="readonly",
        ).grid(row=0, column=0, sticky="ew", ipady=6)

        _ghost_btn(file_row, "Browse", self._browse_csv).grid(
            row=0, column=1, padx=(6, 0))

        self._stocks_lbl = tk.Label(
            left, textvariable=self.v_stocks_count,
            font=("Calibri", 8), fg=C["gold"], bg=C["panel"],
        )
        self._stocks_lbl.grid(row=r, column=0, columnspan=2,
                               padx=20, pady=(2, 0), sticky="w"); r += 1

        _hint(left, "CSV must have: Company_Name, NSE_Symbol / BSE_Symbol, ON_NSE / ON_BSE", r); r += 1

        _gap(left, r, 12); r += 1

        # ── Fetch Mode ────────────────────────────────────────
        _lbl(left, "FETCH MODE", r); r += 1

        mode_frame = tk.Frame(left, bg=C["panel"])
        mode_frame.grid(row=r, column=0, columnspan=2,
                        padx=20, pady=(4, 0), sticky="ew"); r += 1

        for m in FETCH_MODES:
            tk.Radiobutton(
                mode_frame, text=m, variable=self.v_mode, value=m,
                font=F_SM, fg=C["dim"], bg=C["panel"],
                selectcolor=C["surface"],
                activebackground=C["panel"],
                activeforeground=C["text"],
                relief="flat",
                command=self._on_mode_change,
            ).pack(side="left", padx=(0, 12))

        _gap(left, r, 6); r += 1

        # ── Date Range ────────────────────────────────────────
        self._date_frame = tk.Frame(left, bg=C["panel"])
        self._date_frame.grid(row=r, column=0, columnspan=2, sticky="ew"); r += 1
        self._date_frame.columnconfigure(0, weight=1)
        self._date_frame.columnconfigure(1, weight=1)

        _lbl(self._date_frame, "START DATE  (YYYY-MM-DD)", 0, col=0)
        _lbl(self._date_frame, "END DATE  (YYYY-MM-DD)", 0, col=1)

        _entry(self._date_frame, self.v_start, 1, col=0)
        _entry(self._date_frame, self.v_end,   1, col=1)

        # ── Past Years ────────────────────────────────────────
        self._yr_frame = tk.Frame(left, bg=C["panel"])
        self._yr_frame.grid(row=r, column=0, columnspan=2, sticky="ew"); r += 1
        self._yr_frame.columnconfigure(0, weight=1)

        _lbl(self._yr_frame, "YEARS TO LOOK BACK", 0)
        _hint(self._yr_frame,
              "Fetches all tickers and saves one combined CSV.", 1)

        yr_inner = tk.Frame(self._yr_frame, bg=C["panel"])
        yr_inner.grid(row=2, column=0, padx=20, pady=(4,0), sticky="w")

        tk.Spinbox(
            yr_inner, from_=1, to=50, textvariable=self.v_past_yrs,
            width=6, font=F_BODY,
            bg=C["surface"], fg=C["text"],
            insertbackground=C["text"],
            relief="flat", bd=0,
            highlightthickness=1,
            highlightbackground=C["border"],
            highlightcolor=C["accent"],
            buttonbackground=C["surface"],
        ).pack(side="left")
        tk.Label(yr_inner, text=" years", font=F_SM,
                 fg=C["dim"], bg=C["panel"]).pack(side="left")

        # ── Period ────────────────────────────────────────────
        self._period_frame = tk.Frame(left, bg=C["panel"])
        self._period_frame.grid(row=r, column=0, columnspan=2, sticky="ew"); r += 1
        self._period_frame.columnconfigure(0, weight=1)

        _lbl(self._period_frame, "PERIOD", 0)
        _combo(self._period_frame, self.v_period, YF_PERIODS, 1)

        _gap(left, r, 8); r += 1

        # ── Interval ──────────────────────────────────────────
        _lbl(left, "DATA INTERVAL", r); r += 1
        _combo(left, self.v_interval, YF_INTERVALS, r); r += 1

        _gap(left, r, 12); r += 1

        # ── Interval Analysis ─────────────────────────────────
        _lbl(left, "INTERVAL ANALYSIS", r); r += 1

        tk.Checkbutton(
            left,
            text="Enable performance analysis over a date window",
            variable=self.v_do_analysis,
            font=F_SM, fg=C["dim"], bg=C["panel"],
            selectcolor=C["surface"],
            activebackground=C["panel"],
            activeforeground=C["text"],
            relief="flat",
            command=self._on_analysis_toggle,
        ).grid(row=r, column=0, columnspan=2, padx=20, sticky="w"); r += 1

        self._analysis_frame = tk.Frame(left, bg=C["panel"])
        self._analysis_frame.grid(row=r, column=0, columnspan=2, sticky="ew"); r += 1
        self._analysis_frame.columnconfigure(0, weight=1)
        self._analysis_frame.columnconfigure(1, weight=1)

        _lbl(self._analysis_frame, "WINDOW START  (MM-DD)", 0, col=0)
        _lbl(self._analysis_frame, "WINDOW END  (MM-DD)", 0, col=1)
        _entry(self._analysis_frame, self.v_iv_start, 1, col=0)
        _entry(self._analysis_frame, self.v_iv_end,   1, col=1)

        _gap(left, r, 12); r += 1

        # ── Enrichment ────────────────────────────────────────
        _lbl(left, "DATA ENRICHMENT", r); r += 1

        tk.Checkbutton(
            left,
            text="Fetch sector, market cap, PE ratio, 52W high/low  (slower)",
            variable=self.v_enrich,
            font=F_SM, fg=C["dim"], bg=C["panel"],
            selectcolor=C["surface"],
            activebackground=C["panel"],
            activeforeground=C["text"],
            relief="flat",
        ).grid(row=r, column=0, columnspan=2, padx=20, sticky="w"); r += 1

        _gap(left, r, 12); r += 1

        # ── Output ────────────────────────────────────────────
        _lbl(left, "OUTPUT", r); r += 1

        tk.Checkbutton(
            left,
            text="Save separate CSV per stock  (default: one combined file)",
            variable=self.v_separate,
            font=F_SM, fg=C["dim"], bg=C["panel"],
            selectcolor=C["surface"],
            activebackground=C["panel"],
            activeforeground=C["text"],
            relief="flat",
        ).grid(row=r, column=0, columnspan=2, padx=20, sticky="w"); r += 1

        _lbl(left, "BASE FILENAME", r); r += 1
        _entry(left, self.v_filename, r); r += 1

        _lbl(left, "OUTPUT FOLDER", r); r += 1

        out_row = tk.Frame(left, bg=C["panel"])
        out_row.grid(row=r, column=0, columnspan=2,
                     padx=20, pady=(4,0), sticky="ew"); r += 1
        out_row.columnconfigure(0, weight=1)

        tk.Entry(
            out_row, textvariable=self.v_output,
            font=F_BODY, bg=C["surface"], fg=C["text"],
            insertbackground=C["text"],
            relief="flat", bd=0,
            highlightthickness=1,
            highlightbackground=C["border"],
            highlightcolor=C["accent"],
        ).grid(row=0, column=0, sticky="ew", ipady=7)

        _ghost_btn(out_row, "Browse", self._browse_out).grid(
            row=0, column=1, padx=(6, 0))

        # Push buttons to bottom
        left.rowconfigure(r, weight=1); r += 1

        btn_frame = tk.Frame(left, bg=C["panel"])
        btn_frame.grid(row=r, column=0, columnspan=2,
                       padx=20, pady=16, sticky="ew"); r += 1
        btn_frame.columnconfigure(0, weight=1)
        btn_frame.columnconfigure(1, weight=1)

        self._start_btn = tk.Button(
            btn_frame, text="START",
            font=F_BTN, fg=C["bg"], bg=C["accent"],
            activebackground=C["a_hover"], activeforeground=C["bg"],
            relief="flat", bd=0, pady=11,
            cursor="hand2", command=self._start,
        )
        self._start_btn.grid(row=0, column=0, sticky="ew")

        self._stop_btn = tk.Button(
            btn_frame, text="STOP",
            font=F_BTN, fg=C["dim"], bg=C["surface"],
            activebackground=C["err"], activeforeground=C["bg"],
            relief="flat", bd=0, pady=11,
            cursor="hand2", state="disabled", command=self._do_stop,
        )
        self._stop_btn.grid(row=0, column=1, sticky="ew", padx=(8, 0))

    # ---- RIGHT PANEL ----

    def _build_right(self, parent):
        right = tk.Frame(parent, bg=C["bg"])
        right.grid(row=0, column=1, sticky="nsew", padx=(1, 0))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(2, weight=1)

        stats = tk.Frame(right, bg=C["panel"])
        stats.grid(row=0, column=0, sticky="ew")
        stats.columnconfigure((0, 1, 2, 3), weight=1)

        self._s_stocks  = _stat(stats, "Stocks",   "0 / 0", 0)
        self._s_records = _stat(stats, "Rows",     "0",     1)
        self._s_cur     = _stat(stats, "Current",  "—",     2)
        self._s_time    = _stat(stats, "Elapsed",  "00:00", 3)

        self._pbar = ttk.Progressbar(
            right, style="OHLC.Horizontal.TProgressbar",
            orient="horizontal", mode="determinate",
        )
        self._pbar.grid(row=1, column=0, sticky="ew")

        log_wrap = tk.Frame(right, bg=C["bg"])
        log_wrap.grid(row=2, column=0, sticky="nsew")
        log_wrap.columnconfigure(0, weight=1)
        log_wrap.rowconfigure(1, weight=1)

        log_hdr = tk.Frame(log_wrap, bg=C["bg"])
        log_hdr.grid(row=0, column=0, sticky="ew", padx=16, pady=(10, 4))
        tk.Label(log_hdr, text="LIVE LOG",
                 font=("Consolas", 8), fg=C["faint"], bg=C["bg"],
                 ).pack(side="left")
        _ghost_btn(log_hdr, "Clear", self._clear_log).pack(side="right")

        self._log = scrolledtext.ScrolledText(
            log_wrap, font=F_MONO,
            bg=C["bg"], fg=C["text"],
            insertbackground=C["text"],
            relief="flat", bd=0,
            wrap="word", state="disabled",
            padx=16, pady=8,
        )
        self._log.grid(row=1, column=0, sticky="nsew")

        for tag, fg in [
            ("info", C["text"]), ("dim",  C["dim"]),
            ("ok",   C["ok"]),   ("warn", C["warn"]),
            ("err",  C["err"]),  ("head", C["accent"]),
            ("ts",   C["faint"]),("gold", C["gold"]),
        ]:
            self._log.tag_config(tag, foreground=fg)

        self._tick()

    # -------------------------------------------------------------------------
    # MODE / ANALYSIS TOGGLE
    # -------------------------------------------------------------------------

    def _on_mode_change(self, *_):
        mode = self.v_mode.get()
        self._date_frame.grid_remove()
        self._yr_frame.grid_remove()
        self._period_frame.grid_remove()

        if mode == "Date Range":
            self._date_frame.grid()
        elif mode == "Past Years":
            self._yr_frame.grid()
        else:
            self._period_frame.grid()

    def _on_analysis_toggle(self, *_):
        if self.v_do_analysis.get():
            self._analysis_frame.grid()
        else:
            self._analysis_frame.grid_remove()

    # -------------------------------------------------------------------------
    # FILE DIALOGS
    # -------------------------------------------------------------------------

    def _browse_csv(self):
        path = filedialog.askopenfilename(
            title="Select stocks CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            stocks = load_stocks_from_csv(path)
            self._stocks = stocks
            self.v_stocks_path.set(path)
            self.v_stocks_count.set(f"{len(stocks)} tickers loaded "
                                    f"({sum(1 for s in stocks if s['exchange']=='NSE')} NSE  "
                                    f"{sum(1 for s in stocks if s['exchange']=='BSE')} BSE)")
            self._log_write(f"Loaded {len(stocks)} tickers from {os.path.basename(path)}", "gold")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _browse_out(self):
        path = filedialog.askdirectory(title="Choose output folder")
        if path:
            self.v_output.set(path)

    def _clear_log(self):
        self._log.config(state="normal")
        self._log.delete("1.0", "end")
        self._log.config(state="disabled")

    # -------------------------------------------------------------------------
    # VALIDATION
    # -------------------------------------------------------------------------

    def _validate(self):
        if not self._stocks:
            messagebox.showerror("No stocks", "Load a stocks CSV first.")
            return None

        mode = self.v_mode.get()
        cfg  = {
            "mode":     mode,
            "interval": self.v_interval.get(),
            "enrich":   self.v_enrich.get(),
            "separate": self.v_separate.get(),
            "filename": self.v_filename.get().strip() or "ohlc_output",
            "output":   self.v_output.get().strip(),
        }

        if mode == "Date Range":
            try:
                s = datetime.strptime(self.v_start.get().strip(), "%Y-%m-%d")
                e = datetime.strptime(self.v_end.get().strip(), "%Y-%m-%d")
                if s >= e:
                    raise ValueError("Start must be before end.")
                cfg["start_date"] = s
                cfg["end_date"]   = e
            except ValueError as ex:
                messagebox.showerror("Invalid dates", str(ex))
                return None

        elif mode == "Past Years":
            yrs = self.v_past_yrs.get()
            if yrs < 1:
                messagebox.showerror("Invalid", "Years must be >= 1.")
                return None
            cfg["past_years"] = yrs

        else:
            cfg["period"] = self.v_period.get()

        if self.v_do_analysis.get():
            pat = re.compile(r"^\d{2}-\d{2}$")
            ivs = self.v_iv_start.get().strip()
            ive = self.v_iv_end.get().strip()
            if not pat.match(ivs) or not pat.match(ive):
                messagebox.showerror("Invalid window",
                    "Analysis window must be MM-DD format.")
                return None
            cfg["iv_start"] = ivs
            cfg["iv_end"]   = ive
            cfg["analysis"] = True
        else:
            cfg["analysis"] = False

        return cfg

    # -------------------------------------------------------------------------
    # START / STOP
    # -------------------------------------------------------------------------

    def _start(self):
        cfg = self._validate()
        if cfg is None:
            return

        os.makedirs(cfg["output"], exist_ok=True)

        self._stop.clear()
        total = len(self._stocks)
        self._stats      = {"done": 0, "total": total, "records": 0}
        self._start_time = datetime.now()

        self._pbar["maximum"] = total
        self._pbar["value"]   = 0
        self._s_stocks.config(text=f"0 / {total}")
        self._s_records.config(text="0")
        self._s_cur.config(text="—")
        self._status_lbl.config(text="  RUNNING  ", fg=C["ok"])

        self._start_btn.config(state="disabled")
        self._stop_btn.config(state="normal")

        self._log_write("=" * 54, "head")
        self._log_write(f"  {cfg['mode'].upper()}  —  {total} tickers", "head")
        self._log_write(f"  Interval : {cfg['interval']}", "dim")
        self._log_write(f"  Enrich   : {'Yes' if cfg['enrich'] else 'No'}", "dim")
        self._log_write(f"  Output   : {cfg['output']}", "dim")
        self._log_write("=" * 54, "head")

        threading.Thread(
            target=self._run,
            args=(list(self._stocks), cfg),
            daemon=True,
        ).start()

    def _do_stop(self):
        self._stop.set()
        self._log_write("Stop requested — finishing current ticker...", "warn")
        self._stop_btn.config(state="disabled")

    # -------------------------------------------------------------------------
    # SCRAPER THREAD
    # -------------------------------------------------------------------------

    def _run(self, stocks, cfg):
        all_rows      = []
        enrich_cache  = {}
        failed        = []

        try:
            for i, stock in enumerate(stocks):
                if self._stop.is_set():
                    self._qlog("Stopped by user.", "warn")
                    break

                self._q.put(("cur", stock["company"][:18]))
                self._qlog(
                    f"  [{i+1}/{len(stocks)}]  {stock['company'][:40]}"
                    f"  ({stock['ticker']})", "info"
                )

                rows = process_stock(stock, cfg, self._qlog, enrich_cache)

                if rows:
                    all_rows.extend(rows)
                    self._stats["records"] += len(rows)
                    self._qlog(
                        f"  OK  {len(rows)} rows  —  "
                        f"{stock['ticker']}", "ok"
                    )
                else:
                    failed.append(stock["ticker"])

                self._stats["done"] = i + 1
                self._q.put(("prog", i + 1, len(stocks)))
                time.sleep(0.3)

            # ── Save ──────────────────────────────────────────
            if all_rows:
                self._save(all_rows, cfg)
            else:
                self._qlog("No data fetched. Nothing saved.", "warn")

            if failed:
                self._qlog(f"\n  Failed tickers ({len(failed)}):", "warn")
                for t in failed:
                    self._qlog(f"    {t}", "dim")

            # ── Analysis ──────────────────────────────────────
            if cfg["analysis"] and all_rows:
                self._do_analysis(all_rows, cfg)

        except Exception as e:
            self._qlog(f"Unexpected error: {e}", "err")
        finally:
            self._qlog(
                f"\nFinished.  {self._stats['done']}/{self._stats['total']} tickers  |  "
                f"{self._stats['records']} rows saved.", "head"
            )
            self._q.put(("done",))

    def _save(self, all_rows, cfg):
        cols     = ALL_COLS if cfg["enrich"] else OUTPUT_COLS
        full_df  = pd.DataFrame(all_rows).reindex(columns=cols)
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        base     = cfg["filename"]
        out      = cfg["output"]

        if cfg["mode"] == "Past Years" or not cfg["separate"]:
            # One big combined file
            fname = os.path.join(out, f"{base}_{ts}.csv")
            full_df.to_csv(fname, index=False)
            self._qlog(f"\n  Saved combined CSV  ({len(full_df)} rows)  →  {os.path.basename(fname)}", "ok")
        else:
            self._qlog(f"\n  Saving separate files...", "dim")
            for company, grp in full_df.groupby("Company"):
                safe  = re.sub(r"\W+", "_", company.strip())
                fname = os.path.join(out, f"{base}_{safe}_{ts}.csv")
                grp.to_csv(fname, index=False)
                self._qlog(f"    {company[:40]}  →  {os.path.basename(fname)}", "ok")

    def _do_analysis(self, all_rows, cfg):
        self._qlog("\n" + "─" * 50, "dim")
        self._qlog("  INTERVAL ANALYSIS", "head")
        self._qlog("─" * 50, "dim")

        df = pd.DataFrame(all_rows).reindex(columns=OUTPUT_COLS)
        pivot, agg = perform_interval_analysis(
            df, cfg["iv_start"], cfg["iv_end"]
        )

        if pivot is None:
            self._qlog("  Not enough data for analysis.", "warn")
            return

        # Save analysis text
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = os.path.join(cfg["output"],
                             f"{cfg['filename']}_analysis_{ts}.txt")
        try:
            with open(fname, "w") as f:
                f.write(f"Interval Analysis  ({cfg['iv_start']} to {cfg['iv_end']})\n\n")
                f.write("Pct Change (%) per Year:\n")
                f.write(pivot.to_string(float_format="%.2f"))
                f.write("\n\nAggregate Results:\n")
                f.write(agg.to_string(index=False, float_format="%.2f"))
            self._qlog(f"  Analysis saved  →  {os.path.basename(fname)}", "ok")
        except Exception as e:
            self._qlog(f"  Could not save analysis: {e}", "err")

        # Log top performers
        if not agg.empty:
            top = agg.nlargest(3, "Avg_%")
            self._qlog("\n  Top performers (avg %):", "gold")
            for _, row in top.iterrows():
                self._qlog(
                    f"    {row['Company'][:35]:<35}  "
                    f"{row['Avg_%']:+.2f}%  ({row['Trend']})", "gold"
                )

    # -------------------------------------------------------------------------
    # QUEUE → UI
    # -------------------------------------------------------------------------

    def _qlog(self, msg, level="info"):
        self._q.put(("log", msg, level))

    def _poll(self):
        try:
            while True:
                item = self._q.get_nowait()
                kind = item[0]
                if kind == "log":
                    self._log_write(item[1], item[2])
                elif kind == "prog":
                    _, done, total = item
                    self._pbar["value"] = done
                    self._s_stocks.config(text=f"{done} / {total}")
                    self._s_records.config(text=str(self._stats["records"]))
                elif kind == "cur":
                    self._s_cur.config(text=item[1])
                elif kind == "done":
                    self._start_btn.config(state="normal")
                    self._stop_btn.config(state="disabled")
                    self._status_lbl.config(text="  IDLE  ", fg=C["dim"])
                    self._start_time = None
        except queue.Empty:
            pass
        self.after(80, self._poll)

    def _log_write(self, msg, level="info"):
        self._log.config(state="normal")
        ts = datetime.now().strftime("%H:%M:%S")
        self._log.insert("end", f"[{ts}]  ", "ts")
        self._log.insert("end", msg + "\n", level)
        self._log.see("end")
        self._log.config(state="disabled")

    def _tick(self):
        if self._start_time:
            elapsed    = datetime.now() - self._start_time
            mins, secs = divmod(int(elapsed.total_seconds()), 60)
            self._s_time.config(text=f"{mins:02d}:{secs:02d}")
        self.after(1000, self._tick)


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    app = App()
    app.mainloop()
