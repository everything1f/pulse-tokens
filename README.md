# Solana Pulse Token Fetcher

Fetches 300+ tokens per category (New Pairs / Final Stretch / Migrated)
and saves them to `data/tokens.json`. Runs free on GitHub Actions.

## Setup (5 minutes)

### 1. Create a GitHub repo
- Go to github.com → New repository
- Name it anything, e.g. `pulse-tokens`
- Set to **Public** (Actions are free for public repos, 2000 min/month for private)

### 2. Upload these files keeping the folder structure:
```
.github/workflows/fetch_tokens.yml
fetch_tokens.py
README.md
```

### 3. Enable Actions write permissions
- Go to your repo → Settings → Actions → General
- Scroll to "Workflow permissions"
- Select **"Read and write permissions"**
- Save

### 4. Run it
- Go to your repo → **Actions** tab
- Click **"Fetch Solana Pulse Tokens"** in the left sidebar
- Click **"Run workflow"** → **"Run workflow"**
- Go home — it'll run in the background

### 5. Get your data
When the run is green, go to `data/tokens.json` in your repo.
You can fetch it directly in your frontend:

```javascript
const res = await fetch(
  'https://raw.githubusercontent.com/YOUR_USERNAME/YOUR_REPO/main/data/tokens.json'
);
const data = await res.json();

const { new_pairs, final_stretch, migrated } = data;
```

---

## How long does it take?

| Category       | Tokens | Time estimate |
|----------------|--------|---------------|
| New Pairs      | 300    | ~3 min        |
| Final Stretch  | 300    | ~15-40 min    |
| Migrated       | 300    | ~5-10 min     |
| **Total**      | **900**| **~30-60 min**|

Final Stretch takes longest because it has to scan through thousands
of pump.fun tokens to find ones at 70-99% bonding curve.
GitHub Actions allows up to 6 hours — more than enough.

---

## Auto-refresh

The workflow is also scheduled to run every 6 hours automatically.
To disable auto-refresh, remove the `schedule:` block from the yml file.

---

## Adjusting targets

Edit the top of `fetch_tokens.py`:

```python
TARGET_NEW_PAIRS     = 300
TARGET_FINAL_STRETCH = 300
TARGET_MIGRATED      = 300
```

---

## Token fields (per token)

| Field | Description |
|-------|-------------|
| `name`, `symbol`, `ca` | Token identity |
| `ca_short` | Shortened CA for display e.g. `AbCd...XyZw` |
| `image_url` | Token image (from pump.fun or DexScreener) |
| `age` | Human readable age e.g. `4m`, `2h`, `1d` |
| `price_usd` | Current price |
| `market_cap` | Market cap in USD |
| `liquidity_usd` | Liquidity pool size |
| `bonding_curve` | % complete (100 = graduated/migrated) |
| `volume_24h` | 24h volume USD |
| `change_5m/1h/6h/24h` | Price change % (DexScreener tokens only) |
| `txns_24h_buys/sells` | Transaction counts |
| `buy_pct`, `sell_pct` | Buy/sell ratio % |
| `holders` | Estimated holder count |
| `website`, `twitter`, `telegram` | Socials |
| `source` | `pumpfun` or `dexscreener` |
| `category` | `new_pairs`, `final_stretch`, or `migrated` |
