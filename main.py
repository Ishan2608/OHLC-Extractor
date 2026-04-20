"""
OHLC Extractor — UI
====================
Fetches historical OHLC data for Indian stocks using yfinance.
Stocks are auto-loaded from:  data/INDIA_LIST.csv
Expected columns: ISIN, Company_Name, BSE_Symbol, NSE_Symbol,
                  Security_Code, Face_Value, ON_BSE, ON_NSE

Requirements:
    pip install yfinance pandas
    tkinter ships with Python on Windows.
"""

import csv
import json
import os
import re
import queue
import shutil
import threading
import time
from datetime import datetime, date

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext

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


INDIA_LIST_PATH      = os.path.join("data", "INDIA_LIST.csv")
CHECKPOINT_DIR       = ".ohlc_checkpoint"         # Max History checkpoint folder
CHECKPOINT_DIR_REG   = ".ohlc_checkpoint_regular" # Regular START checkpoint folder
BATCH_SIZE           = 50    # tickers per batch before flushing to disk (low-RAM mode)


def load_stocks_both_tickers(path):
    """
    Load all stocks, keeping BOTH NSE and BSE tickers separately per row,
    so Max History can try one then fall back to the other.
    Returns list of dicts: {company, nse_ticker, bse_ticker}
    """
    stocks = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                company  = str(row.get("Company_Name", "")).strip()
                nse_sym  = str(row.get("NSE_Symbol",   "")).strip()
                bse_sym  = str(row.get("BSE_Symbol",   "")).strip()
                on_nse   = str(row.get("ON_NSE", "")).strip().upper() in ("1","Y","YES","TRUE","X")
                on_bse   = str(row.get("ON_BSE", "")).strip().upper() in ("1","Y","YES","TRUE","X")

                nse_t = f"{nse_sym}.NS" if (on_nse and nse_sym) else None
                bse_t = f"{bse_sym}.BO" if (on_bse and bse_sym) else None

                if nse_t or bse_t:
                    stocks.append({
                        "company":    company,
                        "nse_ticker": nse_t,
                        "bse_ticker": bse_t,
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
        self._stocks     = []          # auto-loaded from INDIA_LIST.csv
        self._selected_companies = []  # list of stock dicts chosen by user
        self._ac_matches         = []  # current autocomplete match list

        # ---- Variables ----
        self.v_mode      = tk.StringVar(value=FETCH_MODES[0])
        self.v_start     = tk.StringVar(value=date.today().replace(year=date.today().year - 1).strftime("%Y-%m-%d"))
        self.v_end       = tk.StringVar(value=date.today().strftime("%Y-%m-%d"))
        self.v_past_yrs  = tk.IntVar(value=5)
        self.v_period    = tk.StringVar(value="5y")
        self.v_interval  = tk.StringVar(value="1d")

        self.v_all_companies  = tk.BooleanVar(value=True)
        self.v_company_search = tk.StringVar()

        self.v_do_analysis = tk.BooleanVar(value=False)
        self.v_iv_start    = tk.StringVar(value="10-01")
        self.v_iv_end      = tk.StringVar(value="10-31")

        self.v_enrich    = tk.BooleanVar(value=False)
        self.v_separate  = tk.BooleanVar(value=False)

        # Closes-Only pivot controls
        self.v_closes_pivot   = tk.BooleanVar(value=False)
        self.v_pivot_vol_mode = tk.StringVar(value="none")   # "none" | "separate" | "multiindex"

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
        self._auto_load_stocks()  # load INDIA_LIST.csv on startup
        self.after(200, self._check_startup_checkpoint)  # deferred so UI is ready

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

        # ── Stock info banner ──────────────────────────────────
        _lbl(left, "STOCKS", r); r += 1

        self._stocks_lbl = tk.Label(
            left, text="Loading...",
            font=("Calibri", 8), fg=C["gold"], bg=C["panel"],
        )
        self._stocks_lbl.grid(row=r, column=0, columnspan=2,
                               padx=20, pady=(2, 0), sticky="w"); r += 1

        _hint(left, f"Source: {INDIA_LIST_PATH}", r); r += 1

        _gap(left, r, 8); r += 1

        # ── Company selection ──────────────────────────────────
        _lbl(left, "COMPANY SELECTION", r); r += 1

        # "All companies" checkbox
        self._all_chk = tk.Checkbutton(
            left,
            text="Fetch for ALL companies",
            variable=self.v_all_companies,
            font=F_SM, fg=C["dim"], bg=C["panel"],
            selectcolor=C["surface"],
            activebackground=C["panel"],
            activeforeground=C["text"],
            relief="flat",
            command=self._on_all_companies_toggle,
        )
        self._all_chk.grid(row=r, column=0, columnspan=2,
                           padx=20, sticky="w"); r += 1

        # Container for the search + tags widget (hidden when All is checked)
        self._company_sel_frame = tk.Frame(left, bg=C["panel"])
        self._company_sel_frame.grid(row=r, column=0, columnspan=2,
                                     sticky="ew"); r += 1
        self._company_sel_frame.columnconfigure(0, weight=1)

        # Search entry
        search_wrap = tk.Frame(
            self._company_sel_frame, bg=C["surface"],
            highlightthickness=1,
            highlightbackground=C["border"],
            highlightcolor=C["accent"],
        )
        search_wrap.grid(row=0, column=0, padx=20, pady=(4, 0), sticky="ew")
        search_wrap.columnconfigure(0, weight=1)

        self._company_search_entry = tk.Entry(
            search_wrap,
            textvariable=self.v_company_search,
            font=F_BODY,
            bg=C["surface"], fg=C["text"],
            insertbackground=C["text"],
            relief="flat", bd=0,
        )
        self._company_search_entry.grid(row=0, column=0, sticky="ew",
                                         padx=8, ipady=7)

        tk.Label(search_wrap, text="⌕", font=("Calibri", 11),
                 fg=C["dim"], bg=C["surface"]).grid(row=0, column=1, padx=(0, 8))

        # Autocomplete dropdown (Toplevel-less — a Frame shown/hidden)
        self._ac_frame = tk.Frame(
            self._company_sel_frame,
            bg=C["surface"],
            highlightthickness=1,
            highlightbackground=C["border"],
        )
        self._ac_frame.grid(row=1, column=0, padx=20, sticky="ew")
        self._ac_frame.columnconfigure(0, weight=1)
        self._ac_frame.grid_remove()   # hidden until user types

        self._ac_listbox = tk.Listbox(
            self._ac_frame,
            font=F_BODY,
            bg=C["surface"], fg=C["text"],
            selectbackground=C["accent"],
            selectforeground=C["bg"],
            relief="flat", bd=0,
            activestyle="none",
            height=6,
            highlightthickness=0,
        )
        self._ac_listbox.grid(row=0, column=0, sticky="ew")

        ac_sb = tk.Scrollbar(self._ac_frame, orient="vertical",
                             command=self._ac_listbox.yview)
        ac_sb.grid(row=0, column=1, sticky="ns")
        self._ac_listbox.configure(yscrollcommand=ac_sb.set)

        # Bind events
        self.v_company_search.trace_add("write", self._on_search_change)
        self._ac_listbox.bind("<<ListboxSelect>>", self._on_ac_select)
        self._company_search_entry.bind("<FocusOut>",
            lambda e: self.after(150, self._hide_ac))
        self._company_search_entry.bind("<Escape>",
            lambda e: self._hide_ac())

        # Selected tags area
        tk.Label(
            self._company_sel_frame,
            text="Selected:",
            font=("Consolas", 7), fg=C["faint"], bg=C["panel"],
        ).grid(row=2, column=0, padx=20, pady=(8, 2), sticky="w")

        self._tags_outer = tk.Frame(
            self._company_sel_frame,
            bg=C["hi"],
            highlightthickness=1,
            highlightbackground=C["border"],
        )
        self._tags_outer.grid(row=3, column=0, padx=20, pady=(0, 4), sticky="ew")

        self._tags_canvas = tk.Canvas(
            self._tags_outer, bg=C["hi"],
            highlightthickness=0, height=72,
        )
        self._tags_canvas.pack(side="left", fill="both", expand=True)

        tags_vsb = tk.Scrollbar(self._tags_outer, orient="vertical",
                                command=self._tags_canvas.yview)
        tags_vsb.pack(side="right", fill="y")
        self._tags_canvas.configure(yscrollcommand=tags_vsb.set)

        self._tags_inner = tk.Frame(self._tags_canvas, bg=C["hi"])
        self._tags_canvas_win = self._tags_canvas.create_window(
            (0, 0), window=self._tags_inner, anchor="nw"
        )
        self._tags_inner.bind(
            "<Configure>",
            lambda e: self._tags_canvas.configure(
                scrollregion=self._tags_canvas.bbox("all")
            )
        )
        self._tags_canvas.bind(
            "<Configure>",
            lambda e: self._tags_canvas.itemconfig(
                self._tags_canvas_win, width=e.width
            )
        )

        self._sel_count_lbl = tk.Label(
            self._company_sel_frame,
            text="0 selected",
            font=("Consolas", 7), fg=C["faint"], bg=C["panel"],
        )
        self._sel_count_lbl.grid(row=4, column=0, padx=20, pady=(0, 2), sticky="w")

        # Initially hide the picker (All is checked by default)
        self._company_sel_frame.grid_remove()

        _gap(left, r, 6); r += 1

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

        _gap(left, r, 12); r += 1

        # ── Closes-Only Pivot ─────────────────────────────────
        _lbl(left, "PIVOT / TIME-SERIES FORMAT", r); r += 1

        tk.Label(
            left,
            text="Wide matrix: rows = dates,  columns = tickers",
            font=("Calibri", 8), fg=C["faint"], bg=C["panel"],
        ).grid(row=r, column=0, columnspan=2, padx=20, pady=(0, 4), sticky="w"); r += 1

        self._closes_pivot_chk = tk.Checkbutton(
            left,
            text="Save Closes-Only pivot  (ideal for time-series / ML)",
            variable=self.v_closes_pivot,
            font=F_SM, fg=C["dim"], bg=C["panel"],
            selectcolor=C["surface"],
            activebackground=C["panel"],
            activeforeground=C["text"],
            relief="flat",
            command=self._on_closes_pivot_toggle,
        )
        self._closes_pivot_chk.grid(row=r, column=0, columnspan=2,
                                    padx=20, sticky="w"); r += 1

        # Sub-options (hidden until checkbox is ticked)
        self._pivot_opts_frame = tk.Frame(left, bg=C["panel"])
        self._pivot_opts_frame.grid(row=r, column=0, columnspan=2, sticky="ew"); r += 1
        self._pivot_opts_frame.columnconfigure(0, weight=1)
        self._pivot_opts_frame.grid_remove()

        # Decorative left border strip
        pip = tk.Frame(self._pivot_opts_frame, bg=C["accent"], width=2)
        pip.grid(row=0, column=0, rowspan=5, padx=(20, 0), sticky="ns")

        inner = tk.Frame(self._pivot_opts_frame, bg=C["panel"])
        inner.grid(row=0, column=1, padx=(8, 20), pady=(4, 8), sticky="ew")

        tk.Label(
            inner,
            text="Include Volume?",
            font=("Consolas", 8, "bold"), fg=C["faint"], bg=C["panel"],
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 4))

        _vol_opts = [
            ("none",        "Closes only\n"
                            "One file:  Date × Ticker  →  Close price"),
            ("separate",    "Closes + Volume  —  two separate CSVs\n"
                            "File 1: Date × Ticker → Close   |   File 2: Date × Ticker → Volume"),
            ("multiindex",  "Closes + Volume  —  single file, MultiIndex columns\n"
                            "Column header rows: (Close|Volume, Ticker)"),
        ]

        for i, (val, desc) in enumerate(_vol_opts):
            rb_frame = tk.Frame(inner, bg=C["panel"])
            rb_frame.grid(row=i + 1, column=0, sticky="w", pady=(2, 0))

            tk.Radiobutton(
                rb_frame,
                variable=self.v_pivot_vol_mode, value=val,
                font=F_SM, fg=C["text"], bg=C["panel"],
                selectcolor=C["surface"],
                activebackground=C["panel"],
                activeforeground=C["text"],
                relief="flat",
            ).pack(side="left")

            tk.Label(
                rb_frame, text=desc,
                font=("Calibri", 8), fg=C["dim"], bg=C["panel"],
                justify="left",
            ).pack(side="left", padx=(2, 0))

        _gap(left, r, 4); r += 1

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

        # Row 1: START + STOP
        btn_frame = tk.Frame(left, bg=C["panel"])
        btn_frame.grid(row=r, column=0, columnspan=2,
                       padx=20, pady=(16, 4), sticky="ew"); r += 1
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

        # Row 2: MAX HISTORY (full width, distinct amber colour)
        self._maxhist_btn = tk.Button(
            left, text="MAX HISTORY",
            font=F_BTN, fg=C["bg"], bg=C["gold"],
            activebackground="#FCD34D", activeforeground=C["bg"],
            relief="flat", bd=0, pady=9,
            cursor="hand2", command=self._ask_max_history,
        )
        self._maxhist_btn.grid(row=r, column=0, columnspan=2,
                               padx=20, pady=(0, 16), sticky="ew"); r += 1

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
    # COMPANY SELECTION HELPERS
    # -------------------------------------------------------------------------

    def _on_all_companies_toggle(self, *_):
        if self.v_all_companies.get():
            self._company_sel_frame.grid_remove()
        else:
            self._company_sel_frame.grid()

    def _on_search_change(self, *_):
        query = self.v_company_search.get().strip().lower()
        self._ac_listbox.delete(0, "end")

        if not query or not self._stocks:
            self._hide_ac()
            return

        selected_names = {s["company"] for s in self._selected_companies}
        matches = [
            s for s in self._stocks
            if query in s["company"].lower()
            and s["company"] not in selected_names
        ][:30]

        if matches:
            for s in matches:
                exch = s.get("exchange", "")
                self._ac_listbox.insert("end", f"  {s['company']}  [{exch}]")
            self._ac_matches = matches
            self._ac_frame.grid()
        else:
            self._hide_ac()

    def _on_ac_select(self, *_):
        sel = self._ac_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        stock = self._ac_matches[idx]
        self._add_company_tag(stock)
        self.v_company_search.set("")
        self._hide_ac()
        self._company_search_entry.focus()

    def _hide_ac(self):
        self._ac_frame.grid_remove()

    def _add_company_tag(self, stock):
        if any(s["ticker"] == stock["ticker"] for s in self._selected_companies):
            return
        self._selected_companies.append(stock)
        self._rebuild_tags()

    def _remove_company_tag(self, stock):
        self._selected_companies = [
            s for s in self._selected_companies if s["ticker"] != stock["ticker"]
        ]
        self._rebuild_tags()

    def _rebuild_tags(self):
        for w in self._tags_inner.winfo_children():
            w.destroy()

        row_f = None
        col   = 0
        max_cols = 2

        for i, stock in enumerate(self._selected_companies):
            if col == 0:
                row_f = tk.Frame(self._tags_inner, bg=C["hi"])
                row_f.pack(fill="x", padx=4, pady=2)

            tag = tk.Frame(row_f, bg=C["a_dim"],
                           highlightthickness=1,
                           highlightbackground=C["accent"])
            tag.pack(side="left", padx=(0, 4))

            short = stock["company"][:22] + ("…" if len(stock["company"]) > 22 else "")
            tk.Label(tag, text=short,
                     font=("Calibri", 8), fg=C["text"],
                     bg=C["a_dim"], padx=6, pady=3,
                     ).pack(side="left")

            _s = stock  # capture for lambda
            tk.Button(tag, text="×",
                      font=("Calibri", 9, "bold"),
                      fg=C["err"], bg=C["a_dim"],
                      activebackground=C["a_dim"],
                      activeforeground=C["text"],
                      relief="flat", bd=0, padx=4, pady=2,
                      cursor="hand2",
                      command=lambda s=_s: self._remove_company_tag(s),
                      ).pack(side="left")

            col += 1
            if col >= max_cols:
                col = 0

        n = len(self._selected_companies)
        self._sel_count_lbl.config(
            text=f"{n} selected" if n else "0 selected  —  type a name above to add"
        )

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

    def _on_closes_pivot_toggle(self, *_):
        if self.v_closes_pivot.get():
            self._pivot_opts_frame.grid()
        else:
            self._pivot_opts_frame.grid_remove()

    # -------------------------------------------------------------------------
    # FILE DIALOGS & AUTO-LOAD
    # -------------------------------------------------------------------------

    def _auto_load_stocks(self):
        """Load INDIA_LIST.csv automatically on startup."""
        try:
            stocks = load_stocks_from_csv(INDIA_LIST_PATH)
            self._stocks = stocks
            nse_c = sum(1 for s in stocks if s["exchange"] == "NSE")
            bse_c = sum(1 for s in stocks if s["exchange"] == "BSE")
            self._stocks_lbl.config(
                text=f"{len(stocks)} tickers  ({nse_c} NSE  {bse_c} BSE)"
            )
            self._log_write(
                f"Auto-loaded {len(stocks)} tickers from {INDIA_LIST_PATH}", "gold"
            )
        except Exception as e:
            self._stocks_lbl.config(text=f"Could not load: {e}", fg=C["err"])
            self._log_write(f"Could not load {INDIA_LIST_PATH}: {e}", "err")

    def _check_startup_checkpoint(self):
        """
        On startup, check whether a regular-run or max-history checkpoint
        exists in the current output folder and notify the user in the log.
        """
        output = self.v_output.get().strip()

        # Regular-run checkpoint
        reg_meta = self._load_reg_checkpoint_meta(output)
        if reg_meta:
            done_n  = len(reg_meta.get("done_tickers", []))
            total_n = reg_meta.get("total", "?")
            ts      = reg_meta.get("timestamp", "")[:19].replace("T", " ")
            mode    = reg_meta.get("cfg", {}).get("mode", "?")
            self._log_write(
                f"PREVIOUS RUN FOUND  —  {done_n}/{total_n} tickers done  "
                f"({mode},  saved {ts})  —  "
                "Click START to resume or start fresh.",
                "warn"
            )

        # Max History checkpoint
        mh_meta = self._load_checkpoint_meta(output)
        if mh_meta:
            done_n  = len(mh_meta.get("done_tickers", []))
            total_n = mh_meta.get("stats", {}).get("total", "?")
            ts      = mh_meta.get("timestamp", "")[:19].replace("T", " ")
            self._log_write(
                f"MAX HISTORY CHECKPOINT FOUND  —  {done_n}/{total_n} tickers done  "
                f"(saved {ts})  —  "
                "Click MAX HISTORY to resume or start fresh.",
                "warn"
            )

    def _browse_out(self):
        from tkinter import filedialog
        path = filedialog.askdirectory(title="Choose output folder")
        if path:
            self.v_output.set(path)

    def _clear_log(self):
        self._log.config(state="normal")
        self._log.delete("1.0", "end")
        self._log.config(state="disabled")

    # -------------------------------------------------------------------------
    # MAX HISTORY DIALOG  +  CHECKPOINT / RESUME
    # -------------------------------------------------------------------------

    # ── Checkpoint helpers ────────────────────────────────────────────────────

    def _ckpt_dir(self, output):
        return os.path.join(output, CHECKPOINT_DIR)

    def _ckpt_dir_reg(self, output):
        return os.path.join(output, CHECKPOINT_DIR_REG)

    # ── Regular-run checkpoint helpers ────────────────────────────────────────

    def _save_reg_checkpoint(self, closes_acc, volume_acc, all_rows,
                             done_tickers, cfg,
                             all_tickers_ordered, total, records):
        """
        Save progress for a regular START run so it can be resumed later.

        Layout inside {output}/.ohlc_checkpoint_regular/ :
          meta.json       — cfg, done ticker list, ordering, stats
          closes.csv      — Date × Ticker closes (closes_pivot mode)
          volume.csv      — Date × Ticker volume (closes_pivot + volume mode)
          longformat.csv  — long-format rows (long format OR wide-OHLC staging)
        """
        ckpt = self._ckpt_dir_reg(cfg["output"])
        os.makedirs(ckpt, exist_ok=True)

        use_pivot = cfg.get("closes_pivot", False)
        vol_mode  = cfg.get("pivot_vol_mode", "none")

        # Serialise cfg (strip non-JSON-safe objects)
        safe_cfg = {
            "mode":           cfg.get("mode"),
            "interval":       cfg.get("interval"),
            "enrich":         cfg.get("enrich"),
            "separate":       cfg.get("separate"),
            "closes_pivot":   cfg.get("closes_pivot"),
            "pivot_vol_mode": cfg.get("pivot_vol_mode"),
            "filename":       cfg.get("filename"),
            "output":         cfg.get("output"),
            "analysis":       cfg.get("analysis"),
            "all_companies":  cfg.get("all_companies", False),
        }
        for k in ("past_years", "period", "iv_start", "iv_end"):
            if k in cfg:
                safe_cfg[k] = cfg[k]
        for k in ("start_date", "end_date"):
            if k in cfg:
                v = cfg[k]
                safe_cfg[k] = v.isoformat() if isinstance(v, datetime) else str(v)

        meta = {
            "timestamp":     datetime.now().isoformat(),
            "done_tickers":  list(done_tickers),
            "all_tickers":   list(all_tickers_ordered),
            "total":         total,
            "records":       records,
            "cfg":           safe_cfg,
        }
        tmp_meta  = os.path.join(ckpt, "meta.json.tmp")
        real_meta = os.path.join(ckpt, "meta.json")
        with open(tmp_meta, "w") as f:
            json.dump(meta, f, indent=2)
        os.replace(tmp_meta, real_meta)

        # Closes pivot checkpoint
        if use_pivot and closes_acc:
            df  = pd.DataFrame(closes_acc).sort_index()
            df.index.name = "Date"
            tmp  = os.path.join(ckpt, "closes.csv.tmp")
            real = os.path.join(ckpt, "closes.csv")
            df.to_csv(tmp)
            os.replace(tmp, real)

        # Volume pivot checkpoint (only when volume is being collected)
        if use_pivot and volume_acc and vol_mode in ("separate", "multiindex"):
            df  = pd.DataFrame(volume_acc).sort_index()
            df.index.name = "Date"
            tmp  = os.path.join(ckpt, "volume.csv.tmp")
            real = os.path.join(ckpt, "volume.csv")
            df.to_csv(tmp)
            os.replace(tmp, real)

        # Long-format / wide-OHLC-staging checkpoint
        if not use_pivot and all_rows:
            tmp  = os.path.join(ckpt, "longformat.csv.tmp")
            real = os.path.join(ckpt, "longformat.csv")
            pd.DataFrame(all_rows).to_csv(tmp, index=False)
            os.replace(tmp, real)

    def _load_reg_checkpoint_meta(self, output):
        path = os.path.join(self._ckpt_dir_reg(output), "meta.json")
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return None

    def _load_reg_checkpoint_data(self, output, use_pivot):
        """
        Reload accumulators from checkpoint files.
        Returns (closes_acc, volume_acc, all_rows).
        """
        ckpt       = self._ckpt_dir_reg(output)
        closes_acc = {}
        volume_acc = {}
        all_rows   = []

        closes_path = os.path.join(ckpt, "closes.csv")
        volume_path = os.path.join(ckpt, "volume.csv")
        long_path   = os.path.join(ckpt, "longformat.csv")

        if use_pivot:
            if os.path.exists(closes_path):
                df = pd.read_csv(closes_path, index_col=0, parse_dates=True)
                for col in df.columns:
                    s = df[col].dropna()
                    s.index = pd.to_datetime(s.index).date
                    closes_acc[col] = s.rename(col)
            if os.path.exists(volume_path):
                df = pd.read_csv(volume_path, index_col=0, parse_dates=True)
                for col in df.columns:
                    s = df[col].dropna()
                    s.index = pd.to_datetime(s.index).date
                    volume_acc[col] = s.rename(col)
        else:
            if os.path.exists(long_path):
                all_rows = pd.read_csv(long_path).to_dict("records")

        return closes_acc, volume_acc, all_rows

    def _clear_reg_checkpoint(self, output):
        ckpt = self._ckpt_dir_reg(output)
        if os.path.exists(ckpt):
            shutil.rmtree(ckpt, ignore_errors=True)
            self._qlog("  Checkpoint cleared (run complete).", "dim")

    def _save_final_regular(self, closes_acc, volume_acc, all_rows,
                            cfg, use_pivot, wide_ohlc):
        """
        Write the final output CSV(s) for a completed regular run.

        Three modes
        ───────────
        A) closes_pivot=True  (use_pivot)
           Saves a Date × Ticker closes matrix.
           If pivot_vol_mode="separate", also saves a Date × Ticker volume matrix.
           If pivot_vol_mode="multiindex", saves a single (Close|Volume) × Ticker matrix.
           Ticker symbols are the column headers.

        B) all_companies=True, closes_pivot=False  (wide_ohlc)
           Saves a Date × (Ticker, Field) MultiIndex CSV.
           Column headers look like: RELIANCE.NS_Open, RELIANCE.NS_High, ...
           Rows come from all_rows (long format), pivoted here at save time.

        C) Specific companies, closes_pivot=False  (neither)
           Falls through to existing _save() — long format, unchanged.
        """
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = cfg["filename"]
        out  = cfg["output"]
        mode = cfg.get("pivot_vol_mode", "none")

        if use_pivot:
            # ── Mode A: Closes (+ Volume) pivot ───────────────────────────
            if not closes_acc:
                self._qlog("No data fetched. Nothing saved.", "warn")
                return

            self._qlog("\n  [Pivot]  Building close-price matrix...", "dim")
            try:
                closes_df = pd.DataFrame(closes_acc).sort_index()
                closes_df.index.name = "Date"

                if mode == "none":
                    fname = os.path.join(out, f"{base}_closes_{ts}.csv")
                    closes_df.to_csv(fname)
                    self._qlog(
                        f"  [Pivot]  Closes  "
                        f"({closes_df.shape[0]} dates × {closes_df.shape[1]} tickers)  →  "
                        f"{os.path.basename(fname)}", "ok"
                    )

                elif mode == "separate":
                    fname_c = os.path.join(out, f"{base}_closes_{ts}.csv")
                    closes_df.to_csv(fname_c)
                    self._qlog(
                        f"  [Pivot]  Closes  "
                        f"({closes_df.shape[0]} dates × {closes_df.shape[1]} tickers)  →  "
                        f"{os.path.basename(fname_c)}", "ok"
                    )
                    if volume_acc:
                        volume_df = pd.DataFrame(volume_acc).sort_index()
                        volume_df.index.name = "Date"
                        fname_v = os.path.join(out, f"{base}_volume_{ts}.csv")
                        volume_df.to_csv(fname_v)
                        self._qlog(
                            f"  [Pivot]  Volume  "
                            f"({volume_df.shape[0]} dates × {volume_df.shape[1]} tickers)  →  "
                            f"{os.path.basename(fname_v)}", "ok"
                        )

                elif mode == "multiindex":
                    if volume_acc:
                        volume_df = pd.DataFrame(volume_acc).sort_index()
                        volume_df.index.name = "Date"
                        closes_df.columns = pd.MultiIndex.from_product(
                            [["Close"],  closes_df.columns], names=["Metric", "Ticker"]
                        )
                        volume_df.columns = pd.MultiIndex.from_product(
                            [["Volume"], volume_df.columns], names=["Metric", "Ticker"]
                        )
                        multi = pd.concat([closes_df, volume_df], axis=1) \
                                  .sort_index(axis=1, level="Ticker")
                        fname = os.path.join(out, f"{base}_pivot_multi_{ts}.csv")
                        multi.to_csv(fname)
                        self._qlog(
                            f"  [Pivot]  MultiIndex  "
                            f"({multi.shape[0]} dates × {closes_df.columns.get_level_values('Ticker').nunique()} tickers × 2 metrics)  →  "
                            f"{os.path.basename(fname)}", "ok"
                        )
                    else:
                        # No volume collected (vol_mode wasn't separate/multiindex at fetch)
                        fname = os.path.join(out, f"{base}_closes_{ts}.csv")
                        closes_df.to_csv(fname)
                        self._qlog(
                            f"  [Pivot]  Closes (no volume available)  →  "
                            f"{os.path.basename(fname)}", "warn"
                        )

            except Exception as e:
                self._qlog(f"  [Pivot]  Error saving: {e}", "err")

        elif wide_ohlc:
            # ── Mode B: Wide OHLC — Date × (Ticker, Field) ────────────────
            if not all_rows:
                self._qlog("No data fetched. Nothing saved.", "warn")
                return

            self._qlog("\n  [Wide OHLC]  Building Date × Ticker×Field matrix...", "dim")
            try:
                df = pd.DataFrame(all_rows).reindex(columns=OUTPUT_COLS)
                df["Date"] = pd.to_datetime(df["Date"])

                # Pivot: rows=Date, columns=(Ticker, Field), values=numeric
                fields = ["Open", "High", "Low", "Close", "Volume"]
                wide = df.pivot_table(
                    index="Date",
                    columns="Ticker",
                    values=fields,
                    aggfunc="first",
                )
                # Reorder to (Ticker, Field) so each ticker's 4 cols are together
                wide = wide.swaplevel(axis=1).sort_index(axis=1, level=0)
                wide.index = wide.index.date
                wide.index.name = "Date"

                # Flatten the MultiIndex column names: "RELIANCE.NS_Open" etc.
                wide.columns = [f"{t}_{f}" for t, f in wide.columns]

                fname = os.path.join(out, f"{base}_ohlcv_{ts}.csv")
                wide.to_csv(fname)
                n_tickers = df["Ticker"].nunique()
                self._qlog(
                    f"  [Wide OHLC]  "
                    f"({wide.shape[0]} dates × {n_tickers} tickers × 5 fields)  →  "
                    f"{os.path.basename(fname)}", "ok"
                )

            except Exception as e:
                self._qlog(f"  [Wide OHLC]  Error saving: {e}", "err")

        else:
            # ── Mode C: Long format / separate per-company files ───────────
            if all_rows:
                self._save(all_rows, cfg)
            else:
                self._qlog("No data fetched. Nothing saved.", "warn")

    # ── Max History checkpoint helpers ────────────────────────────────────────

    def _save_checkpoint(self, closes_acc, volume_acc, all_rows,
                         done_tickers, skipped, cfg):
        """
        Persist progress to disk so the run can be resumed later.

        Layout inside  {output}/.ohlc_checkpoint/ :
          meta.json           — config, done ticker list, stats
          closes.csv          — partial closes pivot  (pivot mode only)
          volume.csv          — partial volume pivot  (pivot mode only)
          longformat.csv      — partial long-format rows (non-pivot mode only)
        """
        ckpt = self._ckpt_dir(cfg["output"])
        os.makedirs(ckpt, exist_ok=True)

        use_pivot = cfg.get("closes_pivot", False)

        # Serialise cfg: drop un-JSON-able objects (datetime, stock list)
        safe_cfg = {}
        for k, v in cfg.items():
            if k == "stocks":
                continue
            elif isinstance(v, datetime):
                safe_cfg[k] = v.isoformat()
            else:
                safe_cfg[k] = v

        meta = {
            "timestamp":    datetime.now().isoformat(),
            "done_tickers": done_tickers,
            "skipped":      skipped,
            "stats":        dict(self._stats),
            "cfg":          safe_cfg,
            "use_pivot":    use_pivot,
        }
        with open(os.path.join(ckpt, "meta.json"), "w") as f:
            json.dump(meta, f, indent=2)

        if use_pivot and closes_acc:
            pd.DataFrame(closes_acc).sort_index().to_csv(
                os.path.join(ckpt, "closes.csv")
            )
        if use_pivot and volume_acc:
            pd.DataFrame(volume_acc).sort_index().to_csv(
                os.path.join(ckpt, "volume.csv")
            )
        if not use_pivot and all_rows:
            pd.DataFrame(all_rows).to_csv(
                os.path.join(ckpt, "longformat.csv"), index=False
            )

        self._qlog(
            f"  Checkpoint saved → {ckpt}", "warn"
        )

    def _load_checkpoint_meta(self, output):
        """Return meta dict if a valid checkpoint exists, else None."""
        path = os.path.join(self._ckpt_dir(output), "meta.json")
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return None

    def _load_checkpoint_data(self, output):
        """
        Reload per-ticker Series dicts (pivot mode) or row list (long mode)
        from the checkpoint CSVs.  Returns (closes_acc, volume_acc, all_rows).
        """
        ckpt       = self._ckpt_dir(output)
        closes_acc = {}
        volume_acc = {}
        all_rows   = []

        closes_path = os.path.join(ckpt, "closes.csv")
        volume_path = os.path.join(ckpt, "volume.csv")
        long_path   = os.path.join(ckpt, "longformat.csv")

        if os.path.exists(closes_path):
            df = pd.read_csv(closes_path, index_col=0, parse_dates=True)
            for col in df.columns:
                s = df[col].dropna()
                s.index = pd.to_datetime(s.index).date
                closes_acc[col] = s.rename(col)

        if os.path.exists(volume_path):
            df = pd.read_csv(volume_path, index_col=0, parse_dates=True)
            for col in df.columns:
                s = df[col].dropna()
                s.index = pd.to_datetime(s.index).date
                volume_acc[col] = s.rename(col)

        if os.path.exists(long_path):
            all_rows = pd.read_csv(long_path).to_dict("records")

        return closes_acc, volume_acc, all_rows

    def _clear_checkpoint(self, output):
        """Delete the checkpoint directory after a successful full run."""
        ckpt = self._ckpt_dir(output)
        if os.path.exists(ckpt):
            shutil.rmtree(ckpt, ignore_errors=True)
            self._qlog("  Checkpoint cleared (run complete).", "dim")

    # ── Dialogs ───────────────────────────────────────────────────────────────

    def _ask_max_history(self):
        """
        Entry point for the MAX HISTORY button.
        Checks for an existing checkpoint first; shows a resume dialog if found,
        otherwise shows the normal fetch-settings dialog.
        """
        if not self._stocks:
            messagebox.showerror("No stocks", f"Could not read {INDIA_LIST_PATH}.")
            return

        output = self.v_output.get().strip()
        meta   = self._load_checkpoint_meta(output)

        if meta:
            self._ask_resume(meta, output)
        else:
            self._show_max_history_dialog()

    def _ask_resume(self, meta, output):
        """
        Show a dialog when a checkpoint is found.
        Lets the user resume, start fresh, or cancel.
        """
        done      = len(meta.get("done_tickers", []))
        total     = meta.get("stats", {}).get("total", "?")
        records   = meta.get("stats", {}).get("records", 0)
        skipped_n = len(meta.get("skipped", []))
        ts        = meta.get("timestamp", "")[:19].replace("T", " ")
        pivot     = meta.get("use_pivot", False)
        fmt       = "Pivot / time-series" if pivot else "Long format"

        dlg = tk.Toplevel(self)
        dlg.title("Resume previous run?")
        dlg.configure(bg=C["panel"])
        dlg.resizable(False, False)
        dlg.grab_set()

        self.update_idletasks()
        px = self.winfo_x() + self.winfo_width()  // 2
        py = self.winfo_y() + self.winfo_height() // 2
        dlg.geometry(f"440x310+{px-220}+{py-155}")

        tk.Label(dlg, text="CHECKPOINT FOUND",
                 font=("Consolas", 12, "bold"),
                 fg=C["warn"], bg=C["panel"],
                 ).pack(pady=(22, 6))

        info = (
            f"Saved:       {ts}\n"
            f"Progress:    {done} / {total} tickers done\n"
            f"Skipped:     {skipped_n}  (no data)\n"
            f"Rows so far: {records:,}\n"
            f"Format:      {fmt}"
        )
        tk.Label(dlg, text=info,
                 font=("Consolas", 9), fg=C["text"], bg=C["panel"],
                 justify="left",
                 ).pack(padx=30, pady=(0, 18), anchor="w")

        tk.Label(dlg,
                 text="Resume picks up exactly where it stopped.\n"
                      "Start Fresh discards saved progress permanently.",
                 font=("Calibri", 8), fg=C["faint"], bg=C["panel"],
                 justify="center",
                 ).pack(pady=(0, 14))

        btn_f = tk.Frame(dlg, bg=C["panel"])
        btn_f.pack()

        def _do_resume():
            dlg.destroy()
            self._launch_resume(meta, output)

        def _do_fresh():
            shutil.rmtree(self._ckpt_dir(output), ignore_errors=True)
            dlg.destroy()
            self._show_max_history_dialog()

        tk.Button(btn_f, text="RESUME",
                  font=F_BTN, fg=C["bg"], bg=C["ok"],
                  activebackground="#6EE7B7", activeforeground=C["bg"],
                  relief="flat", bd=0, padx=22, pady=9,
                  cursor="hand2", command=_do_resume,
                  ).pack(side="left", padx=4)

        tk.Button(btn_f, text="START FRESH",
                  font=F_BTN, fg=C["bg"], bg=C["warn"],
                  activebackground="#FCD34D", activeforeground=C["bg"],
                  relief="flat", bd=0, padx=22, pady=9,
                  cursor="hand2", command=_do_fresh,
                  ).pack(side="left", padx=4)

        tk.Button(btn_f, text="CANCEL",
                  font=F_BTN, fg=C["dim"], bg=C["surface"],
                  activebackground=C["hi"], activeforeground=C["text"],
                  relief="flat", bd=0, padx=22, pady=9,
                  cursor="hand2", command=dlg.destroy,
                  ).pack(side="left", padx=4)

    def _show_max_history_dialog(self):
        """Normal MAX HISTORY settings dialog (years / max)."""
        dlg = tk.Toplevel(self)
        dlg.title("Max History")
        dlg.configure(bg=C["panel"])
        dlg.resizable(False, False)
        dlg.grab_set()

        self.update_idletasks()
        px = self.winfo_x() + self.winfo_width()  // 2
        py = self.winfo_y() + self.winfo_height() // 2
        dlg.geometry(f"360x220+{px-180}+{py-110}")

        tk.Label(dlg, text="MAX HISTORY FETCH",
                 font=("Consolas", 11, "bold"),
                 fg=C["gold"], bg=C["panel"],
                 ).pack(pady=(20, 4))

        tk.Label(dlg,
                 text="Fetches the maximum available historical data\nfor every stock in INDIA_LIST.csv.",
                 font=("Calibri", 9), fg=C["dim"], bg=C["panel"],
                 justify="center",
                 ).pack(pady=(0, 14))

        row_f = tk.Frame(dlg, bg=C["panel"])
        row_f.pack(pady=(0, 6))

        tk.Label(row_f, text="Look back (years):",
                 font=("Calibri", 10), fg=C["text"], bg=C["panel"],
                 ).pack(side="left", padx=(0, 10))

        v_yrs = tk.StringVar(value="max")
        tk.Entry(
            row_f, textvariable=v_yrs, width=8,
            font=("Calibri", 10),
            bg=C["surface"], fg=C["text"],
            insertbackground=C["text"],
            relief="flat", bd=0,
            highlightthickness=1,
            highlightbackground=C["border"],
            highlightcolor=C["gold"],
        ).pack(side="left", ipady=5)

        tk.Label(dlg,
                 text='Type a number or leave "max" for all available data.',
                 font=("Calibri", 8), fg=C["faint"], bg=C["panel"],
                 ).pack()

        def _launch():
            raw = v_yrs.get().strip().lower()
            if raw in ("max", ""):
                past_years = None
            else:
                try:
                    past_years = int(raw)
                    if past_years < 1:
                        raise ValueError
                except ValueError:
                    messagebox.showerror("Invalid", "Enter a whole number or 'max'.",
                                         parent=dlg)
                    return
            dlg.destroy()
            self._launch_max_history(past_years)

        tk.Button(
            dlg, text="FETCH",
            font=F_BTN, fg=C["bg"], bg=C["gold"],
            activebackground="#FCD34D", activeforeground=C["bg"],
            relief="flat", bd=0, padx=24, pady=8,
            cursor="hand2", command=_launch,
        ).pack(pady=(10, 0))

    # ── Launchers ─────────────────────────────────────────────────────────────

    def _launch_max_history(self, past_years):
        """Start a fresh Max History run. past_years=None → period='max'."""
        output = self.v_output.get().strip()
        os.makedirs(output, exist_ok=True)

        if past_years is None:
            cfg = {
                "mode":           "Period (yfinance)",
                "period":         "max",
                "interval":       "1d",
                "enrich":         False,
                "separate":       False,
                "closes_pivot":   self.v_closes_pivot.get(),
                "pivot_vol_mode": self.v_pivot_vol_mode.get(),
                "filename":       "max_history",
                "output":         output,
                "analysis":       False,
                "max_history":    True,
            }
            label = "ALL AVAILABLE"
        else:
            cfg = {
                "mode":           "Past Years",
                "past_years":     past_years,
                "interval":       "1d",
                "enrich":         False,
                "separate":       False,
                "closes_pivot":   self.v_closes_pivot.get(),
                "pivot_vol_mode": self.v_pivot_vol_mode.get(),
                "filename":       f"max_history_{past_years}yr",
                "output":         output,
                "analysis":       False,
                "max_history":    True,
            }
            label = f"{past_years} YEARS"

        try:
            all_stocks = load_stocks_both_tickers(INDIA_LIST_PATH)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        self._start_max_history_thread(all_stocks, cfg, label,
                                       resume_closes={},
                                       resume_volume={},
                                       resume_rows=[],
                                       resume_done=[])

    def _launch_resume(self, meta, output):
        """Resume a stopped Max History run from checkpoint data."""
        self._log_write("=" * 54, "head")
        self._log_write("  RESUMING FROM CHECKPOINT", "warn")
        self._log_write(f"  Loading saved data...", "dim")

        try:
            closes_acc, volume_acc, all_rows = self._load_checkpoint_data(output)
        except Exception as e:
            self._log_write(f"  Could not load checkpoint data: {e}", "err")
            messagebox.showerror("Resume failed",
                                 f"Could not read checkpoint files:\n{e}")
            return

        done_tickers = set(meta.get("done_tickers", []))
        saved_cfg    = meta.get("cfg", {})

        # Re-hydrate cfg from saved metadata
        cfg = {
            "mode":           saved_cfg.get("mode", "Period (yfinance)"),
            "period":         saved_cfg.get("period", "max"),
            "interval":       saved_cfg.get("interval", "1d"),
            "enrich":         saved_cfg.get("enrich", False),
            "separate":       saved_cfg.get("separate", False),
            "closes_pivot":   saved_cfg.get("closes_pivot", False),
            "pivot_vol_mode": saved_cfg.get("pivot_vol_mode", "none"),
            "filename":       saved_cfg.get("filename", "max_history"),
            "output":         output,
            "analysis":       False,
            "max_history":    True,
        }
        if "past_years" in saved_cfg:
            cfg["past_years"] = saved_cfg["past_years"]

        # Load full stock list and remove already-done tickers
        try:
            all_stocks = load_stocks_both_tickers(INDIA_LIST_PATH)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        remaining = [
            s for s in all_stocks
            if s.get("nse_ticker") not in done_tickers
            and s.get("bse_ticker") not in done_tickers
        ]

        n_done = len(all_stocks) - len(remaining)
        label  = f"RESUME  ({n_done} already done, {len(remaining)} remaining)"

        self._log_write(
            f"  Loaded {len(closes_acc)} tickers from checkpoint  |  "
            f"{len(remaining)} remaining", "ok"
        )

        self._start_max_history_thread(remaining, cfg, label,
                                       resume_closes=closes_acc,
                                       resume_volume=volume_acc,
                                       resume_rows=all_rows,
                                       resume_done=list(done_tickers))

    def _start_max_history_thread(self, all_stocks, cfg, label,
                                  resume_closes, resume_volume,
                                  resume_rows, resume_done):
        """Wire up the UI and spin up the background thread."""
        total = len(all_stocks)
        self._stop.clear()
        self._stats      = {
            "done":    0,
            "total":   total,
            "records": self._stats.get("records", 0),   # carry over on resume
        }
        self._start_time = datetime.now()

        self._pbar["maximum"] = max(total, 1)
        self._pbar["value"]   = 0
        self._s_stocks.config(text=f"0 / {total}")
        self._s_records.config(text=str(self._stats["records"]))
        self._s_cur.config(text="—")
        self._status_lbl.config(text="  RUNNING  ", fg=C["ok"])

        self._start_btn.config(state="disabled")
        self._maxhist_btn.config(state="disabled")
        self._stop_btn.config(state="normal")

        self._log_write("=" * 54, "head")
        self._log_write(f"  MAX HISTORY  —  {label}  —  {total} tickers", "head")
        self._log_write(f"  Output : {cfg['output']}", "dim")
        self._log_write(f"  NSE preferred, BSE fallback per ticker", "dim")
        self._log_write("=" * 54, "head")

        threading.Thread(
            target=self._run_max_history,
            args=(all_stocks, cfg,
                  resume_closes, resume_volume,
                  resume_rows, resume_done),
            daemon=True,
        ).start()

    # -------------------------------------------------------------------------
    # VALIDATION
    # -------------------------------------------------------------------------

    def _validate(self):
        if not self._stocks:
            messagebox.showerror("No stocks", "Load a stocks CSV first.")
            return None

        # Determine which stocks to fetch
        if self.v_all_companies.get():
            active_stocks = self._stocks
        else:
            active_stocks = self._selected_companies
            if not active_stocks:
                messagebox.showerror(
                    "No companies selected",
                    "Select at least one company, or tick 'Fetch for ALL companies'."
                )
                return None

        mode = self.v_mode.get()
        cfg  = {
            "mode":           mode,
            "interval":       self.v_interval.get(),
            "enrich":         self.v_enrich.get(),
            "separate":       self.v_separate.get(),
            "closes_pivot":   self.v_closes_pivot.get(),
            "pivot_vol_mode": self.v_pivot_vol_mode.get(),
            "filename":       self.v_filename.get().strip() or "ohlc_output",
            "output":         self.v_output.get().strip(),
            "stocks":         active_stocks,
            "all_companies":  self.v_all_companies.get(),
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

        # ── Check for an existing regular-run checkpoint ───────────────────
        meta = self._load_reg_checkpoint_meta(cfg["output"])
        if meta:
            self._ask_resume_regular(meta, cfg)
            return

        self._launch_regular_run(cfg, resume_done=[], resume_closes={},
                                 resume_volume={}, resume_rows=[], resume_records=0)

    def _ask_resume_regular(self, meta, fresh_cfg):
        """
        Show a dialog when a regular-run checkpoint is found.
        """
        done_n    = len(meta.get("done_tickers", []))
        total_n   = meta.get("total", "?")
        records   = meta.get("records", 0)
        ts        = meta.get("timestamp", "")[:19].replace("T", " ")
        saved_cfg = meta.get("cfg", {})
        mode_lbl  = saved_cfg.get("mode", "?")

        dlg = tk.Toplevel(self)
        dlg.title("Resume previous run?")
        dlg.configure(bg=C["panel"])
        dlg.resizable(False, False)
        dlg.grab_set()

        self.update_idletasks()
        px = self.winfo_x() + self.winfo_width()  // 2
        py = self.winfo_y() + self.winfo_height() // 2
        dlg.geometry(f"460x320+{px-230}+{py-160}")

        tk.Label(dlg, text="PREVIOUS RUN FOUND",
                 font=("Consolas", 12, "bold"),
                 fg=C["warn"], bg=C["panel"],
                 ).pack(pady=(22, 6))

        info = (
            f"Saved:       {ts}\n"
            f"Mode:        {mode_lbl}\n"
            f"Progress:    {done_n} / {total_n} tickers done\n"
            f"Rows so far: {records:,}\n\n"
            "RESUME continues with the same settings.\n"
            "START FRESH discards saved progress."
        )
        tk.Label(dlg, text=info,
                 font=("Consolas", 9), fg=C["text"], bg=C["panel"],
                 justify="left",
                 ).pack(padx=30, pady=(0, 18), anchor="w")

        btn_f = tk.Frame(dlg, bg=C["panel"])
        btn_f.pack()

        def _do_resume():
            dlg.destroy()
            self._launch_resume_regular(meta)

        def _do_fresh():
            shutil.rmtree(self._ckpt_dir_reg(fresh_cfg["output"]),
                          ignore_errors=True)
            dlg.destroy()
            self._launch_regular_run(fresh_cfg, resume_done=[],
                                     resume_closes={}, resume_volume={},
                                     resume_rows=[], resume_records=0)

        tk.Button(btn_f, text="RESUME",
                  font=F_BTN, fg=C["bg"], bg=C["ok"],
                  activebackground="#6EE7B7", activeforeground=C["bg"],
                  relief="flat", bd=0, padx=22, pady=9,
                  cursor="hand2", command=_do_resume,
                  ).pack(side="left", padx=4)

        tk.Button(btn_f, text="START FRESH",
                  font=F_BTN, fg=C["bg"], bg=C["warn"],
                  activebackground="#FCD34D", activeforeground=C["bg"],
                  relief="flat", bd=0, padx=22, pady=9,
                  cursor="hand2", command=_do_fresh,
                  ).pack(side="left", padx=4)

        tk.Button(btn_f, text="CANCEL",
                  font=F_BTN, fg=C["dim"], bg=C["surface"],
                  activebackground=C["hi"], activeforeground=C["text"],
                  relief="flat", bd=0, padx=22, pady=9,
                  cursor="hand2", command=dlg.destroy,
                  ).pack(side="left", padx=4)

    def _launch_resume_regular(self, meta):
        """Reconstruct cfg from checkpoint meta and continue the run."""
        saved_cfg = meta.get("cfg", {})
        output    = saved_cfg.get("output", self.v_output.get().strip())

        # Re-hydrate cfg
        cfg = {
            "mode":           saved_cfg.get("mode", "Date Range"),
            "interval":       saved_cfg.get("interval", "1d"),
            "enrich":         saved_cfg.get("enrich", False),
            "separate":       saved_cfg.get("separate", False),
            "closes_pivot":   saved_cfg.get("closes_pivot", False),
            "pivot_vol_mode": saved_cfg.get("pivot_vol_mode", "none"),
            "filename":       saved_cfg.get("filename", "ohlc_output"),
            "output":         output,
            "analysis":       saved_cfg.get("analysis", False),
            "all_companies":  saved_cfg.get("all_companies", False),
        }
        for k in ("start_date", "end_date", "past_years", "period",
                  "iv_start", "iv_end"):
            if k in saved_cfg:
                cfg[k] = saved_cfg[k]

        # Reload full stock list from the checkpoint ticker ordering
        done_tickers = set(meta.get("done_tickers", []))
        all_tickers  = meta.get("all_tickers", [])

        try:
            full_stocks = load_stocks_from_csv(INDIA_LIST_PATH)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        ticker_map = {s["ticker"]: s for s in full_stocks}
        remaining  = [ticker_map[t] for t in all_tickers
                      if t not in done_tickers and t in ticker_map]
        cfg["stocks"] = remaining

        # Reload accumulated data from checkpoint files
        closes_acc, volume_acc, all_rows = self._load_reg_checkpoint_data(
            output, cfg.get("closes_pivot", False)
        )
        records = meta.get("records", 0)

        self._log_write("=" * 54, "head")
        self._log_write("  RESUMING PREVIOUS RUN", "warn")
        self._log_write(f"  {len(done_tickers)} done,  {len(remaining)} remaining", "dim")
        self._log_write("=" * 54, "head")

        self._launch_regular_run(cfg,
                                 resume_done=list(done_tickers),
                                 resume_closes=closes_acc,
                                 resume_volume=volume_acc,
                                 resume_rows=all_rows,
                                 resume_records=records)

    def _launch_regular_run(self, cfg, resume_done, resume_closes,
                            resume_volume, resume_rows, resume_records):
        """Wire up UI and start the regular _run thread."""
        stocks    = list(cfg["stocks"])
        total     = len(stocks) + len(resume_done)
        remaining = len(stocks)

        self._stop.clear()
        self._stats = {
            "done":    len(resume_done),
            "total":   total,
            "records": resume_records,
        }
        self._start_time = datetime.now()

        self._pbar["maximum"] = max(total, 1)
        self._pbar["value"]   = len(resume_done)
        self._s_stocks.config(text=f"{len(resume_done)} / {total}")
        self._s_records.config(text=str(resume_records))
        self._s_cur.config(text="—")
        self._status_lbl.config(text="  RUNNING  ", fg=C["ok"])

        self._start_btn.config(state="disabled")
        self._maxhist_btn.config(state="disabled")
        self._stop_btn.config(
            state="normal", text="STOP",
            bg=C["surface"], fg=C["dim"],
            activebackground=C["err"], activeforeground=C["bg"],
        )

        self._log_write("=" * 54, "head")
        self._log_write(
            f"  {cfg['mode'].upper()}  —  {remaining} remaining / {total} total",
            "head"
        )
        self._log_write(f"  Interval : {cfg['interval']}", "dim")
        self._log_write(f"  Enrich   : {'Yes' if cfg['enrich'] else 'No'}", "dim")
        self._log_write(f"  Output   : {cfg['output']}", "dim")
        self._log_write("=" * 54, "head")

        threading.Thread(
            target=self._run,
            args=(stocks, cfg, resume_done, resume_closes, resume_volume, resume_rows),
            daemon=True,
        ).start()

    def _do_stop(self):
        self._stop.set()
        self._log_write("Stop requested — finishing current ticker...", "warn")
        self._stop_btn.config(
            state="disabled", text="STOP",
            bg=C["surface"], fg=C["dim"],
        )

    # -------------------------------------------------------------------------
    # SCRAPER THREAD
    # -------------------------------------------------------------------------

    def _run(self, stocks, cfg,
             resume_done=None, resume_closes=None, resume_volume=None, resume_rows=None):
        """
        Regular START-button scraper thread.

        Output mode decision tree
        ─────────────────────────
        closes_pivot=True  →  Closes pivot: Date × Ticker (closes_acc + volume_acc)
                               pivot_vol_mode controls whether volume is also saved.
                               Applies regardless of whether all_companies is set.

        closes_pivot=False, all_companies=True
                           →  Wide OHLC: Date × (Ticker, Field) MultiIndex columns
                               Rows stored as long-format in all_rows; pivoted at save.

        closes_pivot=False, all_companies=False (specific companies)
                           →  Long format — unchanged existing behaviour.

        Column keys are always TICKER SYMBOLS (not company names).

        Checkpoint / resume
        ───────────────────
        - Checkpoint written atomically after every BATCH_SIZE tickers.
        - On stop or crash, all data so far is preserved.
        - Partial company data (fetch error mid-company) is never saved.
        - closes_acc / volume_acc saved as closes.csv / volume.csv.
        - all_rows (long and wide-OHLC modes) saved as longformat.csv.
        """
        enrich_cache  = {}
        failed        = []

        done_tickers  = list(resume_done  or [])

        use_pivot     = cfg.get("closes_pivot", False)
        all_companies = cfg.get("all_companies", False)
        vol_mode      = cfg.get("pivot_vol_mode", "none")  # "none"|"separate"|"multiindex"
        wide_ohlc     = all_companies and not use_pivot    # full OHLC wide matrix mode

        # Accumulators — only one pair is used per run
        closes_acc = dict(resume_closes or {})   # {ticker: pd.Series}  (closes pivot mode)
        volume_acc = dict(resume_volume or {})   # {ticker: pd.Series}  (closes pivot + volume)
        all_rows   = list(resume_rows   or [])   # row dicts (long format OR wide-OHLC staging)

        output        = cfg["output"]
        total_tickers = len(done_tickers) + len(stocks)
        all_tickers_ordered = done_tickers + [s["ticker"] for s in stocks]

        def _checkpoint():
            self._save_reg_checkpoint(
                closes_acc if use_pivot else {},
                volume_acc if use_pivot else {},
                all_rows,
                done_tickers, cfg,
                all_tickers_ordered,
                total=total_tickers,
                records=self._stats["records"],
            )

        try:
            for batch_start in range(0, len(stocks), BATCH_SIZE):
                batch = stocks[batch_start: batch_start + BATCH_SIZE]

                for stock in batch:
                    if self._stop.is_set():
                        self._qlog("Stopped by user.", "warn")
                        break

                    global_idx = len(done_tickers)
                    self._q.put(("cur", stock["company"][:18]))
                    self._qlog(
                        f"  [{global_idx + 1}/{total_tickers}]  "
                        f"{stock['company'][:40]}  ({stock['ticker']})", "info"
                    )

                    # ── Fetch — atomic: all rows or none ───────────────────
                    rows = process_stock(stock, cfg, self._qlog, enrich_cache)

                    if rows:
                        ticker = stock["ticker"]

                        if use_pivot:
                            # ── Closes (+ volume) pivot ─────────────────────
                            dates  = pd.to_datetime([r["Date"] for r in rows]).date
                            closes_acc[ticker] = pd.Series(
                                [r["Close"]  for r in rows],
                                index=dates, name=ticker,
                            )
                            if vol_mode in ("separate", "multiindex"):
                                volume_acc[ticker] = pd.Series(
                                    [r["Volume"] for r in rows],
                                    index=dates, name=ticker,
                                )

                        else:
                            # ── Long format (specific companies) OR
                            #    Wide-OHLC staging (all companies, no closes_pivot)
                            all_rows.extend(rows)

                        done_tickers.append(ticker)
                        self._stats["records"] += len(rows)
                        self._qlog(
                            f"  OK  {len(rows)} rows  —  {ticker}", "ok"
                        )
                    else:
                        failed.append(stock["ticker"])

                    self._stats["done"] = len(done_tickers)
                    self._q.put(("prog", len(done_tickers), total_tickers))
                    time.sleep(0.3)

                # ── End of batch: checkpoint ────────────────────────────────
                if self._stop.is_set():
                    break

                self._qlog(
                    f"  [Batch checkpoint]  {len(done_tickers)} / {total_tickers} done...",
                    "dim"
                )
                _checkpoint()

            # ── Post-loop ──────────────────────────────────────────────────
            if self._stop.is_set():
                self._qlog("\n  Saving checkpoint for resume...", "warn")
                _checkpoint()
                self._qlog(
                    f"  {len(done_tickers)} tickers saved.  "
                    "Click START next time to resume.", "warn"
                )
                self.after(0, lambda: self._stop_btn.config(
                    state="normal", text="RESUME",
                    bg=C["ok"], fg=C["bg"],
                    activebackground="#6EE7B7", activeforeground=C["bg"],
                    command=self._resume_from_stop,
                ))
            else:
                # Full completion — write final output file(s)
                self._save_final_regular(
                    closes_acc, volume_acc, all_rows,
                    cfg, use_pivot, wide_ohlc,
                )
                self._clear_reg_checkpoint(output)

                if cfg.get("analysis") and all_rows and not use_pivot:
                    self._do_analysis(all_rows, cfg)

            if failed:
                self._qlog(f"\n  Failed tickers ({len(failed)}):", "warn")
                for t in failed:
                    self._qlog(f"    {t}", "dim")

        except Exception as e:
            self._qlog(f"Unexpected error: {e}", "err")
            try:
                _checkpoint()
                self._qlog("  Emergency checkpoint saved.", "warn")
            except Exception as e2:
                self._qlog(f"  Could not save checkpoint: {e2}", "err")
        finally:
            self._qlog(
                f"\nFinished.  {self._stats['done']}/{self._stats['total']} tickers  |  "
                f"{self._stats['records']} rows saved.", "head"
            )
            self._q.put(("done",))

    def _resume_from_stop(self):
        """Called when RESUME button is clicked after a stop in the same session."""
        output = self.v_output.get().strip()
        meta   = self._load_reg_checkpoint_meta(output)
        if meta:
            self._launch_resume_regular(meta)
        else:
            self._log_write("No checkpoint found. Use START for a new run.", "warn")

    def _run_max_history(self, all_stocks, cfg,
                         resume_closes=None, resume_volume=None,
                         resume_rows=None,  resume_done=None):
        """
        Max History thread.

        On a fresh run  resume_* args are empty.
        On a resume run they carry the data loaded from the checkpoint,
        and all_stocks has already had the done tickers filtered out.

        Stop behaviour
        ──────────────
        When the user clicks STOP the loop exits cleanly.  Whatever has
        been accumulated (including pre-loaded resume data) is written to
        a checkpoint so the run can be continued next time.

        Completion behaviour
        ────────────────────
        On natural completion the final output is saved and the checkpoint
        directory is deleted.
        """
        use_pivot = cfg.get("closes_pivot", False)

        # Pre-populate accumulators with any resumed data
        closes_acc   = dict(resume_closes or {})
        volume_acc   = dict(resume_volume or {})
        all_rows     = list(resume_rows   or [])
        done_tickers = list(resume_done   or [])
        skipped      = []

        try:
            for i, stock in enumerate(all_stocks):
                if self._stop.is_set():
                    self._qlog("Stopped by user.", "warn")
                    break

                company = stock["company"]
                nse_t   = stock["nse_ticker"]
                bse_t   = stock["bse_ticker"]

                self._q.put(("cur", company[:18]))
                self._qlog(
                    f"  [{i+1}/{len(all_stocks)}]  {company[:44]}", "info"
                )

                data = None
                used = None

                # ── Try NSE first ──────────────────────────────────────────
                if nse_t:
                    self._qlog(f"    Trying NSE: {nse_t}", "dim")
                    data = fetch_history(nse_t, cfg, self._qlog)
                    if data is not None and not data.empty:
                        used = nse_t

                # ── Fall back to BSE ───────────────────────────────────────
                if data is None and bse_t:
                    self._qlog(f"    NSE empty — trying BSE: {bse_t}", "warn")
                    data = fetch_history(bse_t, cfg, self._qlog)
                    if data is not None and not data.empty:
                        used = bse_t

                if data is not None and not data.empty:
                    dates = pd.to_datetime(data["Date"]).dt.date

                    if use_pivot:
                        closes_acc[used] = pd.Series(
                            data["Close"].round(2).values,
                            index=dates, name=used,
                        )
                        volume_acc[used] = pd.Series(
                            data["Volume"].values,
                            index=dates, name=used,
                        )
                        n = len(dates)
                    else:
                        exch = "NSE" if used == nse_t else "BSE"
                        data["Company"]  = company
                        data["Ticker"]   = used
                        data["Exchange"] = exch
                        data["Year"]     = pd.to_datetime(data["Date"]).dt.year
                        data["Date"]     = dates
                        data[["Open","High","Low","Close"]] = (
                            data[["Open","High","Low","Close"]].round(2)
                        )
                        rows = data.reindex(columns=OUTPUT_COLS).to_dict("records")
                        all_rows.extend(rows)
                        n = len(rows)

                    done_tickers.append(used)
                    self._stats["records"] += n
                    self._qlog(f"    OK  {n} rows  via {used}", "ok")

                else:
                    skipped.append(company)
                    self._qlog("    SKIPPED — no data on NSE or BSE", "err")

                self._stats["done"] = i + 1
                self._q.put(("prog", i + 1, len(all_stocks)))
                time.sleep(0.25)

            # ── Post-loop: save checkpoint or final output ─────────────────
            if self._stop.is_set():
                # Partial run — persist everything for later resume
                self._qlog("\n  Saving checkpoint for resume...", "warn")
                self._save_checkpoint(
                    closes_acc, volume_acc, all_rows,
                    done_tickers, skipped, cfg,
                )
                self._qlog(
                    f"  {len(done_tickers)} tickers saved.  "
                    "Click MAX HISTORY next time to resume.", "warn"
                )
            else:
                # Full completion — write final output, wipe checkpoint
                if use_pivot and closes_acc:
                    self._save_pivot_from_acc(closes_acc, volume_acc, cfg)
                elif not use_pivot and all_rows:
                    self._save(all_rows, cfg)
                else:
                    self._qlog("No data fetched. Nothing saved.", "warn")
                self._clear_checkpoint(cfg["output"])

            # ── Skipped summary ────────────────────────────────────────────
            if skipped:
                self._qlog(
                    f"\n  Skipped ({len(skipped)} stocks — no data found):", "warn"
                )
                for s in skipped:
                    self._qlog(f"    {s}", "dim")

        except Exception as e:
            self._qlog(f"Unexpected error: {e}", "err")
        finally:
            self._qlog(
                f"\nMax History complete.  "
                f"{self._stats['done']}/{self._stats['total']} tickers  |  "
                f"{self._stats['records']} rows  |  "
                f"{len(skipped)} skipped.", "head"
            )
            self._q.put(("done",))

    def _save_pivot_from_acc(self, closes_acc, volume_acc, cfg):
        """
        Build and write pivot CSV(s) directly from the per-ticker Series dicts
        accumulated by _run_max_history (pivot mode).

        Memory profile:
          closes_acc / volume_acc  ≈ 250 MB each  (5 k tickers × 6 k floats)
          pd.DataFrame(closes_acc) ≈ 250 MB        (same data, contiguous array)
          Peak during concat       ≈ 600–700 MB    (old + new side by side briefly)
          No all_rows list, no full_df copy — saves ~10 GB vs the original path.
        """
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = cfg["filename"]
        out  = cfg["output"]
        mode = cfg.get("pivot_vol_mode", "none")

        self._qlog("\n  [Pivot]  Assembling matrices from per-ticker series...", "dim")

        try:
            # ── Build closes DataFrame ─────────────────────────────────────
            # pd.DataFrame from dict-of-Series aligns on index automatically:
            # dates present in some tickers but not others become NaN rows.
            closes_df = pd.DataFrame(closes_acc).sort_index()
            closes_df.index.name = "Date"
            n_dates   = closes_df.shape[0]
            n_tickers = closes_df.shape[1]

            if mode == "none":
                # ── Closes only ───────────────────────────────────────────
                fname = os.path.join(out, f"{base}_closes_{ts}.csv")
                closes_df.to_csv(fname)
                self._qlog(
                    f"  [Pivot]  Closes  "
                    f"({n_dates} dates × {n_tickers} tickers)  →  "
                    f"{os.path.basename(fname)}", "ok"
                )

            elif mode == "separate":
                # ── Closes CSV ────────────────────────────────────────────
                fname_c = os.path.join(out, f"{base}_closes_{ts}.csv")
                closes_df.to_csv(fname_c)
                self._qlog(
                    f"  [Pivot]  Closes  "
                    f"({n_dates} dates × {n_tickers} tickers)  →  "
                    f"{os.path.basename(fname_c)}", "ok"
                )

                # ── Volume CSV ────────────────────────────────────────────
                # Build volume only now — closes_df already written, can be
                # garbage-collected by Python if nothing else holds a ref.
                volume_df = pd.DataFrame(volume_acc).sort_index()
                volume_df.index.name = "Date"
                fname_v = os.path.join(out, f"{base}_volume_{ts}.csv")
                volume_df.to_csv(fname_v)
                self._qlog(
                    f"  [Pivot]  Volume  "
                    f"({volume_df.shape[0]} dates × {volume_df.shape[1]} tickers)  →  "
                    f"{os.path.basename(fname_v)}", "ok"
                )

            elif mode == "multiindex":
                # ── MultiIndex: (Close | Volume) × Ticker ─────────────────
                volume_df = pd.DataFrame(volume_acc).sort_index()
                volume_df.index.name = "Date"

                closes_df.columns = pd.MultiIndex.from_product(
                    [["Close"],  closes_df.columns], names=["Metric", "Ticker"]
                )
                volume_df.columns = pd.MultiIndex.from_product(
                    [["Volume"], volume_df.columns], names=["Metric", "Ticker"]
                )

                multi = pd.concat([closes_df, volume_df], axis=1) \
                          .sort_index(axis=1, level="Ticker")

                fname = os.path.join(out, f"{base}_pivot_multi_{ts}.csv")
                multi.to_csv(fname)
                self._qlog(
                    f"  [Pivot]  MultiIndex  "
                    f"({multi.shape[0]} dates × {n_tickers} tickers × 2 metrics)  →  "
                    f"{os.path.basename(fname)}", "ok"
                )

        except Exception as e:
            self._qlog(f"  [Pivot]  Error during save: {e}", "err")

    def _save(self, all_rows, cfg):
        cols     = ALL_COLS if cfg["enrich"] else OUTPUT_COLS
        full_df  = pd.DataFrame(all_rows).reindex(columns=cols)
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        base     = cfg["filename"]
        out      = cfg["output"]

        # ── Long format (original) ────────────────────────────
        if cfg["mode"] == "Past Years" or not cfg["separate"]:
            fname = os.path.join(out, f"{base}_{ts}.csv")
            full_df.to_csv(fname, index=False)
            self._qlog(
                f"\n  [Long format]  Saved combined CSV  ({len(full_df)} rows)  →  "
                f"{os.path.basename(fname)}", "ok"
            )
        else:
            self._qlog(f"\n  [Long format]  Saving separate files...", "dim")
            for company, grp in full_df.groupby("Company"):
                safe  = re.sub(r"\W+", "_", company.strip())
                fname = os.path.join(out, f"{base}_{safe}_{ts}.csv")
                grp.to_csv(fname, index=False)
                self._qlog(
                    f"    {company[:40]}  →  {os.path.basename(fname)}", "ok"
                )

        # ── Pivot / time-series format (optional) ─────────────
        if cfg.get("closes_pivot"):
            self._save_pivot(full_df, cfg, ts)

    def _save_pivot(self, full_df, cfg, ts):
        """
        Save wide-format pivot CSV(s): rows = dates, columns = tickers.

        pivot_vol_mode:
          "none"       → one CSV, Close prices only
          "separate"   → two CSVs: one for Close, one for Volume
          "multiindex" → one CSV with two-level column header (metric, ticker)
        """
        base = cfg["filename"]
        out  = cfg["output"]
        mode = cfg.get("pivot_vol_mode", "none")

        df = full_df.copy()
        df["Date"] = pd.to_datetime(df["Date"])

        self._qlog(f"\n  [Pivot format]  Building pivot tables...", "dim")

        try:
            if mode == "none":
                # ── Closes only ───────────────────────────────
                closes = (
                    df.pivot_table(index="Date", columns="Ticker",
                                   values="Close", aggfunc="first")
                    .sort_index()
                )
                closes.index = closes.index.date
                closes.index.name = "Date"
                fname = os.path.join(out, f"{base}_closes_{ts}.csv")
                closes.to_csv(fname)
                self._qlog(
                    f"  [Pivot]  Closes pivot  "
                    f"({closes.shape[0]} dates × {closes.shape[1]} tickers)  →  "
                    f"{os.path.basename(fname)}", "ok"
                )

            elif mode == "separate":
                # ── Closes CSV ────────────────────────────────
                closes = (
                    df.pivot_table(index="Date", columns="Ticker",
                                   values="Close", aggfunc="first")
                    .sort_index()
                )
                closes.index = closes.index.date
                closes.index.name = "Date"
                fname_c = os.path.join(out, f"{base}_closes_{ts}.csv")
                closes.to_csv(fname_c)
                self._qlog(
                    f"  [Pivot]  Closes pivot  "
                    f"({closes.shape[0]} dates × {closes.shape[1]} tickers)  →  "
                    f"{os.path.basename(fname_c)}", "ok"
                )

                # ── Volume CSV ────────────────────────────────
                volume = (
                    df.pivot_table(index="Date", columns="Ticker",
                                   values="Volume", aggfunc="first")
                    .sort_index()
                )
                volume.index = volume.index.date
                volume.index.name = "Date"
                fname_v = os.path.join(out, f"{base}_volume_{ts}.csv")
                volume.to_csv(fname_v)
                self._qlog(
                    f"  [Pivot]  Volume pivot  "
                    f"({volume.shape[0]} dates × {volume.shape[1]} tickers)  →  "
                    f"{os.path.basename(fname_v)}", "ok"
                )

            elif mode == "multiindex":
                # ── MultiIndex: (Close|Volume) × Ticker ───────
                multi = (
                    df.pivot_table(index="Date", columns="Ticker",
                                   values=["Close", "Volume"], aggfunc="first")
                    .sort_index()
                )
                multi.index = multi.index.date
                multi.index.name = "Date"
                fname = os.path.join(out, f"{base}_pivot_multi_{ts}.csv")
                multi.to_csv(fname)
                tickers = multi.columns.get_level_values("Ticker").nunique()
                self._qlog(
                    f"  [Pivot]  MultiIndex pivot  "
                    f"({multi.shape[0]} dates × {tickers} tickers × 2 metrics)  →  "
                    f"{os.path.basename(fname)}", "ok"
                )

        except Exception as e:
            self._qlog(f"  [Pivot]  Could not build pivot: {e}", "err")

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
                    self._maxhist_btn.config(state="normal")
                    # Only reset the stop button if it hasn't been turned into RESUME
                    if self._stop_btn.cget("text") != "RESUME":
                        self._stop_btn.config(
                            state="disabled", text="STOP",
                            bg=C["surface"], fg=C["dim"],
                        )
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
