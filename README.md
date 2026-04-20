# OHLC Extractor.

We will use older code as base to build a new version of the Stock-OHLC-Reporter, which will be more optimized for flexibility in user requirments.

A utility function to provide user with historical OHLC (Open, High, Low, Close) data for multiple stocks using yfinance. Designed for investors, analysts, and developers who need structured, time-bounded market data for backtesting, research, or portfolio tracking.

Flow:
- User enters list of stocks
- User enters time interval, example 5, i.e., last 5 years.
- Date range, e.g., 2018-01-01 to 2023-01-01, the specific date range for each year.
- It creates reports in .csv and .xlsx formats.
- It also generates a summary report in text file and markdown file.

![Output Screenshot](outputs/capture1.PNG)

Run Program

```python
venv\Scripts\activate
```

```python
python main.py
```
