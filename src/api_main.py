from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
import json, os
import pandas as pd

app = FastAPI(title="Superstore Insight API")

# ---- Paths to artifacts ----
CLEAN_CSV   = os.path.join("artifacts", "clean_superstore.csv")
EDA_JSON    = os.path.join("artifacts", "reports", "eda_summary.json")
ALL_JSON    = os.path.join("artifacts", "reports", "all_reports.json")
SCRAPE_JSON = os.path.join("artifacts", "reports", "scraped_excerpt.json")

# ---- Serve /static from artifacts/ (for images & files) ----
app.mount("/static", StaticFiles(directory="artifacts"), name="static")

def load_df() -> pd.DataFrame:
    if not os.path.exists(CLEAN_CSV):
        raise FileNotFoundError("Run 01_load_clean.py first to create artifacts/clean_superstore.csv")
    return pd.read_csv(CLEAN_CSV)

# ---------- Root & favicon ----------
@app.get("/", include_in_schema=False)
def root_redirect():
    """Redirect base URL to the HTML dashboard."""
    return RedirectResponse(url="/dashboard")

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    """Silence the browser's favicon request (no icon)."""
    return Response(status_code=204)

# ---------- JSON APIs ----------
@app.get("/summary")
def get_summary():
    """Return EDA summary JSON."""
    if not os.path.exists(EDA_JSON):
        raise HTTPException(status_code=404, detail="EDA summary not found. Run 02_eda_kpis.py")
    with open(EDA_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/top-losses")
def top_losses(n: int = Query(10, ge=1, le=1000)):
    """Return worst-N products by profit as JSON."""
    df = load_df()
    worst = df.sort_values("profit").head(int(n))[["product_name","category","sub_category","sales","profit"]]
    return json.loads(worst.to_json(orient="records"))

@app.get("/category/{name}")
def category_stats(name: str):
    """Return aggregated sales/profit per sub-category for a given category."""
    df = load_df()
    sub = df[df["category"].str.lower() == name.lower()]
    if sub.empty:
        raise HTTPException(status_code=404, detail=f"No rows for category '{name}'")
    stats = sub.groupby("sub_category")[["sales","profit"]].sum().sort_values("profit", ascending=False)
    return json.loads(stats.reset_index().to_json(orient="records"))

# ---------- Pretty HTML views (easy "Back to Dashboard") ----------
@app.get("/summary_html", response_class=HTMLResponse)
def summary_html():
    if not os.path.exists(EDA_JSON):
        return HTMLResponse(
            "<p>Run <code>python src/02_eda_kpis.py</code> first.</p><p><a href='/dashboard'>Back to Dashboard</a></p>",
            status_code=200,
        )
    txt = open(EDA_JSON, "r", encoding="utf-8").read()
    return HTMLResponse(f"""
        <h2>/summary (pretty)</h2>
        <pre style="white-space:pre-wrap">{txt}</pre>
        <p><a href="/dashboard">Back to Dashboard</a></p>
    """)

@app.get("/top-losses-html", response_class=HTMLResponse)
def top_losses_html(n: int = Query(10, ge=1, le=1000)):
    df = load_df()
    worst = df.sort_values("profit").head(int(n))[["product_name","category","sub_category","sales","profit"]]
    rows = "".join(
        f"<tr><td>{r.product_name}</td><td>{r.category}</td><td>{r.sub_category}</td>"
        f"<td>{r.sales}</td><td>{r.profit}</td></tr>"
        for r in worst.itertuples()
    )
    return HTMLResponse(f"""
      <h2>Top Losses (first {n})</h2>
      <table border="1" cellpadding="6" cellspacing="0">
        <tr><th>Product</th><th>Category</th><th>Sub-Category</th><th>Sales</th><th>Profit</th></tr>
        {rows}
      </table>
      <p><a href="/dashboard">Back to Dashboard</a></p>
    """)

# ---------- Scrape endpoints ----------
@app.get("/scrape")
def get_scrape():
    """Return scrape result JSON."""
    if not os.path.exists(SCRAPE_JSON):
        raise HTTPException(status_code=404, detail="scraped_excerpt.json not found. Run 05_scrape_demo.py")
    with open(SCRAPE_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/scrape_html", response_class=HTMLResponse)
def scrape_html():
    """Pretty HTML view of scrape results with Back link."""
    if not os.path.exists(SCRAPE_JSON):
        return HTMLResponse("<p>Run 05_scrape_demo.py first.</p><p><a href='/dashboard'>Back</a></p>")
    data = json.load(open(SCRAPE_JSON, "r", encoding="utf-8"))

    def rows(block):
        items = (block or {}).get("items_preview") or []
        return "".join(
            f"<tr><td>{i.get('title','')}</td><td>{i.get('price','')}</td>"
            f"<td><a href='{i.get('href','')}' target='_blank'>link</a></td></tr>"
            for i in items
        )

    city  = data.get("citycenter")  or {}
    timec = data.get("timecenter")  or {}
    readr = data.get("readers")     or {}
    html = f"""
      <h2>Scraped Results</h2>
      <h3>City Center ({city.get('items_count','0')})</h3>
      <table border="1" cellpadding="6" cellspacing="0">
        <tr><th>Title</th><th>Price</th><th>Link</th></tr>{rows(city)}
      </table>
      <h3>Time Center ({timec.get('items_count','0')})</h3>
      <table border="1" cellpadding="6" cellspacing="0">
        <tr><th>Title</th><th>Price</th><th>Link</th></tr>{rows(timec)}
      </table>
      <h3>Readers ({readr.get('items_count','0')})</h3>
      <table border="1" cellpadding="6" cellspacing="0">
        <tr><th>Title</th><th>Price</th><th>Link</th></tr>{rows(readr)}
      </table>
      <p><a href="/dashboard">Back to Dashboard</a></p>
    """
    return HTMLResponse(html)

# ---------- HTML Dashboard ----------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    summary = {}
    all_reports = {}
    scrape = {}

    if os.path.exists(EDA_JSON):
        with open(EDA_JSON, "r", encoding="utf-8") as f:
            summary = json.load(f)
    if os.path.exists(ALL_JSON):
        with open(ALL_JSON, "r", encoding="utf-8") as f:
            all_reports = json.load(f)
    if os.path.exists(SCRAPE_JSON):
        with open(SCRAPE_JSON, "r", encoding="utf-8") as f:
            scrape = json.load(f)

    top_cat     = summary.get("top_category_by_profit", "N/A")
    best_region = summary.get("best_region_by_profit", "N/A")
    best_segment= summary.get("best_segment_by_profit", "N/A")

    # Build table for top losses (first 10)
    losses_rows_html = ""
    for row in all_reports.get("top_losses", [])[:10]:
        losses_rows_html += f"""
          <tr>
            <td>{row.get('product_name','')}</td>
            <td>{row.get('category','')}</td>
            <td>{row.get('sub_category','')}</td>
            <td>{row.get('sales','')}</td>
            <td>{row.get('profit','')}</td>
          </tr>
        """

    # Scrape snapshot
    scrape_city  = (scrape.get("citycenter") or {}).get("items_count", 0)
    scrape_time  = (scrape.get("timecenter") or {}).get("items_count", 0)
    scrape_read  = (scrape.get("readers") or {}).get("items_count", 0)

    # Static image URLs
    monthly_png = "/static/figures/monthly_sales.png"
    subcat_png  = "/static/figures/top10_subcategories_profit.png"
    scatter_png = "/static/figures/discount_vs_profit.png"

    # Notes to help if artifacts are missing
    notes = []
    if not summary:
        notes.append("Run <code>python src/02_eda_kpis.py</code> to generate EDA summary and charts.")
    if not all_reports:
        notes.append("Run <code>python src/06_parallel_reports.py</code> to generate merged reports.")
    if not scrape:
        notes.append("Run <code>python src/05_scrape_demo.py</code> to generate scrape results.")
    note_html = "<br>".join(notes) if notes else ""

    return HTMLResponse(f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8"/>
      <title>Superstore Dashboard</title>
      <link rel="icon" href="data:,">
      <style>
        body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px}}
        h1{{margin-bottom:0.2rem}}
        .nav{{margin:-6px 0 12px}} .nav a{{margin-right:8px}}
        .kpis{{display:flex;gap:16px;margin:12px 0 24px;flex-wrap:wrap}}
        .card{{border:1px solid #ddd;border-radius:10px;padding:12px 16px;background:#fafafa}}
        img{{max-width:100%;height:auto;border:1px solid #eee;border-radius:8px}}
        table{{border-collapse:collapse;width:100%}}
        th,td{{padding:8px 10px;border-bottom:1px solid #eee;text-align:left}}
        th{{background:#f7f7f7}}
        .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:18px}}
        .muted{{color:#666}} .note{{margin:8px 0 16px;color:#b45309}}
        a.btn{{display:inline-block;padding:8px 12px;background:#2563eb;color:white;border-radius:8px;text-decoration:none}}
      </style>
    </head>
    <body>
      <h1>Superstore Dashboard</h1>
      <p class="nav muted">
        <a href="/dashboard">Dashboard</a> ·
        <a href="/docs" target="_blank" rel="noopener">Docs</a>
      </p>
      {f'<p class="note">{note_html}</p>' if note_html else ''}

      <div class="kpis">
        <div class="card"><b>Top Category (Profit)</b><br>{top_cat}</div>
        <div class="card"><b>Best Region</b><br>{best_region}</div>
        <div class="card"><b>Best Segment</b><br>{best_segment}</div>
      </div>

      <div class="grid">
        <div><h3>Monthly Sales</h3><img src="{monthly_png}" alt="Monthly Sales"/></div>
        <div><h3>Top 10 Sub-Categories (Profit)</h3><img src="{subcat_png}" alt="Top Sub-Categories by Profit"/></div>
        <div><h3>Discount vs Profit</h3><img src="{scatter_png}" alt="Discount vs Profit"/></div>
      </div>

      <h3 style="margin-top:28px;">Top Loss Products</h3>
      <div class="card">
        <table>
          <thead><tr><th>Product</th><th>Category</th><th>Sub-Category</th><th>Sales</th><th>Profit</th></tr></thead>
          <tbody>{losses_rows_html or '<tr><td colspan="5" class="muted">Run 06_parallel_reports.py to populate this table.</td></tr>'}</tbody>
        </table>
      </div>

      <h3 style="margin-top:28px;">Web Scrape Snapshot</h3>
      <div class="kpis">
        <div class="card"><b>City Center items</b><br>{scrape_city}</div>
        <div class="card"><b>Time Center items</b><br>{scrape_time}</div>
        <div class="card"><b>Readers items</b><br>{scrape_read}</div>
      </div>
      <p>
        <a class="btn" href="/scrape_html">View Scrape (HTML)</a>
        <a class="btn" href="/scrape" target="_blank" rel="noopener" style="background:#059669;">View Scrape JSON</a>
      </p>

      <p style="margin-top:22px;">
        <!-- Open JSON endpoints in new tabs so you don't lose the dashboard -->
        <a class="btn" href="/summary" target="_blank" rel="noopener">View /summary JSON</a>
        <a class="btn" href="/top-losses?n=10" target="_blank" rel="noopener" style="background:#0ea5e9;">View /top-losses JSON</a>
        <!-- Also provide HTML views that include a Back link -->
        <a class="btn" href="/summary_html" style="background:#7c3aed;">Summary (HTML)</a>
        <a class="btn" href="/top-losses-html?n=10" style="background:#9333ea;">Top Losses (HTML)</a>
      </p>
    </body>
    </html>
    """)
