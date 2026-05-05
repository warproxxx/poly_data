"""Generate examples/05-ml-dataset-and-baseline.ipynb."""
from __future__ import annotations

from pathlib import Path

import nbformat as nbf


def main() -> None:
    nb = nbf.v4.new_notebook()
    md = nbf.v4.new_markdown_cell
    code = nbf.v4.new_code_cell

    cells: list = []

    cells.append(md(
        "# ML Dataset + XGBoost Baseline\n\n"
        "Build a per-category training dataset where features summarise the past N days of "
        "betting from a panel of high-skill players, and the target is whether each market "
        "resolves YES / NO / PASS in the next M days. Save train + test as parquet, then fit "
        "an XGBoost classifier as a baseline.\n\n"
        "**Prerequisites:** `python update_all.py` has populated `data/trades/` and `data/markets/`."
    ))

    cells.append(code(
        "from __future__ import annotations\n"
        "from pathlib import Path\n"
        "import json\n"
        "import os\n\n"
        "import numpy as np\n"
        "import polars as pl\n"
        "import matplotlib.pyplot as plt\n"
        "import seaborn as sns\n"
        "sns.set_theme(style='whitegrid', context='notebook')\n"
        "plt.rcParams['figure.dpi'] = 110\n\n"
        "from poly_data.analysis.io import scan_trades, scan_markets, rss_guard\n"
        "from poly_data.analysis.positions import compute_player_stats\n"
        "from poly_data.analysis.ranking import select_top_n, score_C\n"
        "from poly_data.analysis.ml_dataset import build_dataset, FEATURE_NAMES\n"
        "from poly_data.analysis.dataloader import load as load_split\n\n"
        "DATA_ROOT = Path(os.environ.get('POLY_DATA_ROOT', '../data'))\n"
        "ML_ROOT = DATA_ROOT / 'ml'\n"
        "ML_ROOT.mkdir(parents=True, exist_ok=True)\n\n"
        "assert (DATA_ROOT / 'trades').is_dir(),  'run `python update_all.py` first'\n"
        "assert (DATA_ROOT / 'markets').is_dir(), 'run `python update_all.py` first'\n\n"
        "trades_lf = scan_trades(DATA_ROOT)\n"
        "markets_df = scan_markets(DATA_ROOT).collect()\n"
        "print(f'markets rows: {markets_df.height:,}')"
    ))

    cells.append(md(
        "## Pick the biggest category by # markets\n\n"
        "Default metric: number of markets per category (most rows). Override `MEASURE` to use "
        "fills or USDC volume."
    ))
    cells.append(code(
        "MEASURE = 'n_markets'   # one of: 'n_markets', 'n_fills', 'usd_volume'\n\n"
        "if MEASURE == 'n_markets':\n"
        "    cat_counts = (markets_df.group_by('category')\n"
        "                  .agg(pl.len().alias('score'))\n"
        "                  .sort('score', descending=True))\n"
        "elif MEASURE == 'n_fills':\n"
        "    cat_to_market = markets_df.select(['id', 'category']).rename({'id': 'market_id'})\n"
        "    cat_counts = (trades_lf.join(cat_to_market.lazy(), on='market_id', how='left')\n"
        "                  .group_by('category').agg(pl.len().alias('score'))\n"
        "                  .sort('score', descending=True).collect())\n"
        "else:\n"
        "    cat_to_market = markets_df.select(['id', 'category']).rename({'id': 'market_id'})\n"
        "    cat_counts = (trades_lf.join(cat_to_market.lazy(), on='market_id', how='left')\n"
        "                  .group_by('category').agg(pl.col('usd_amount').sum().alias('score'))\n"
        "                  .sort('score', descending=True).collect())\n\n"
        "print(cat_counts.head(10))\n"
        "non_empty = cat_counts.filter(pl.col('category') != '')\n"
        "TARGET_CATEGORY = non_empty['category'][0] if non_empty.height else cat_counts['category'][0]\n"
        "print(f'\\ntarget category: {TARGET_CATEGORY!r}')"
    ))

    cells.append(md(
        "## Compute player stats + select top 128\n\n"
        "Pluggable scoring fn. Default `score_C = win_rate * log(max(1, total_won_usd))` with "
        "`min_win_rate=0.5`, `min_n_bets=20` filters."
    ))
    cells.append(code(
        "with rss_guard('compute_player_stats'):\n"
        "    stats = compute_player_stats(trades_lf, player_side='both')\n\n"
        "top128 = select_top_n(stats, n=128, min_win_rate=0.5, min_n_bets=20,\n"
        "                     score_fn=score_C)\n"
        "print(f'top-128 rows: {top128.height}')\n"
        "top128.head()"
    ))

    cells.append(md(
        "## Build dataset (window=7d, horizon=1d) + save\n\n"
        "For each `decision_date`, summarise the prior 7 days of top-128 panel activity per market; "
        "label by whether the market resolves in the next 1 day (YES / NO / PASS). Filter to markets "
        "where at least `ceil(0.5 * 128) = 64` panel members traded in the window. 80/20 train/test "
        "split by date."
    ))
    cells.append(code(
        "def slug(c: str) -> str:\n"
        "    return c.lower().replace(' ', '-').replace('/', '-')\n\n"
        "def build_and_save(cat: str, window: int, horizon: int):\n"
        "    out = ML_ROOT / slug(cat) / f'window={window}d' / f'horizon={horizon}d'\n"
        "    out.mkdir(parents=True, exist_ok=True)\n"
        "    with rss_guard(f'build({cat},w={window},h={horizon})'):\n"
        "        train, test, meta = build_dataset(\n"
        "            trades_lf, markets_df, top128,\n"
        "            category=cat, window_days=window, horizon_days=horizon,\n"
        "            min_active_frac=0.5, test_frac=0.20,\n"
        "        )\n"
        "    train.write_parquet(out / 'train.parquet')\n"
        "    test.write_parquet(out / 'test.parquet')\n"
        "    top128.write_parquet(out / 'top_n.parquet')\n"
        "    (out / 'meta.json').write_text(json.dumps(meta, indent=2, default=str))\n"
        "    return train, test, meta\n\n"
        "train7, test7, meta7 = build_and_save(TARGET_CATEGORY, 7, 1)\n"
        "print('train rows:', meta7['n_train_rows'], 'test rows:', meta7['n_test_rows'])\n"
        "print('class counts (train):', meta7['target_class_counts_train'])\n"
        "print('class counts (test): ', meta7['target_class_counts_test'])"
    ))

    cells.append(md("## Build dataset (window=30d, horizon=1d) + save"))
    cells.append(code(
        "train30, test30, meta30 = build_and_save(TARGET_CATEGORY, 30, 1)\n"
        "print('train rows:', meta30['n_train_rows'], 'test rows:', meta30['n_test_rows'])\n"
        "print('class counts (train):', meta30['target_class_counts_train'])\n"
        "print('class counts (test): ', meta30['target_class_counts_test'])"
    ))

    cells.append(md(
        "## Class balance — train vs test\n\n"
        "Both windows side-by-side. Heavy class imbalance (lots of `PASS`) is expected when the "
        "horizon is short relative to typical resolution time."
    ))
    cells.append(code(
        "fig, axes = plt.subplots(1, 2, figsize=(11, 3.5))\n"
        "for ax, meta, label in zip(axes, [meta7, meta30], ['window=7d', 'window=30d']):\n"
        "    df = pl.DataFrame({\n"
        "        'class': ['YES', 'NO', 'PASS'] * 2,\n"
        "        'split': ['train']*3 + ['test']*3,\n"
        "        'count': [meta['target_class_counts_train'].get(k,0) for k in ('YES','NO','PASS')]\n"
        "                + [meta['target_class_counts_test'].get(k,0) for k in ('YES','NO','PASS')],\n"
        "    }).to_pandas()\n"
        "    sns.barplot(data=df, x='class', y='count', hue='split', ax=ax)\n"
        "    ax.set_title(label)\n"
        "plt.tight_layout(); plt.show()"
    ))

    cells.append(md(
        "## XGBoost baseline (window=7d)\n\n"
        "Histogram-based boosted trees with native NaN handling — no imputation needed. Multiclass "
        "log-loss. The baseline is a *floor*; tune later."
    ))
    cells.append(code(
        "from sklearn.metrics import classification_report, confusion_matrix\n"
        "from sklearn.preprocessing import LabelEncoder\n"
        "import xgboost as xgb\n\n"
        "split = load_split(slug(TARGET_CATEGORY), window_days=7, horizon_days=1, data_root=ML_ROOT)\n"
        "le = LabelEncoder().fit(['YES', 'NO', 'PASS'])\n"
        "y_train = le.transform(split.y_train)\n"
        "y_test  = le.transform(split.y_test)\n\n"
        "n_classes_train = len(set(y_train.tolist()))\n"
        "if n_classes_train < 2:\n"
        "    print(f'!! train has only {n_classes_train} class — skipping fit. '\n"
        "          'This typically means the dataset is too small or the horizon is '\n"
        "          'too short for the chosen window. Increase data span or horizon.')\n"
        "    clf = None\n"
        "    pred = None\n"
        "else:\n"
        "    clf = xgb.XGBClassifier(\n"
        "        tree_method='hist', n_estimators=400, max_depth=6,\n"
        "        learning_rate=0.05, eval_metric='mlogloss',\n"
        "        n_jobs=4, random_state=0, num_class=len(le.classes_),\n"
        "    )\n"
        "    clf.fit(split.X_train, y_train)\n"
        "    pred = clf.predict(split.X_test)\n"
        "    if len(y_test) > 0:\n"
        "        print(classification_report(y_test, pred, target_names=le.classes_, zero_division=0))\n"
        "    else:\n"
        "        print('test split is empty')"
    ))

    cells.append(md("## Confusion matrix + feature importances"))
    cells.append(code(
        "if clf is None or pred is None or len(y_test) == 0:\n"
        "    print('skipping plots — model was not fit (insufficient data)')\n"
        "else:\n"
        "    cm = confusion_matrix(y_test, pred, labels=range(len(le.classes_)))\n"
        "    fig, axes = plt.subplots(1, 2, figsize=(11, 4))\n"
        "    sns.heatmap(cm, annot=True, fmt='d',\n"
        "                xticklabels=le.classes_, yticklabels=le.classes_, ax=axes[0])\n"
        "    axes[0].set_xlabel('predicted'); axes[0].set_ylabel('actual')\n"
        "    axes[0].set_title('confusion matrix (window=7d)')\n\n"
        "    importances = clf.feature_importances_\n"
        "    order = np.argsort(importances)[::-1]\n"
        "    axes[1].barh([split.feature_names[i] for i in order],\n"
        "                 [importances[i] for i in order])\n"
        "    axes[1].invert_yaxis()\n"
        "    axes[1].set_title('feature importances')\n"
        "    plt.tight_layout(); plt.show()"
    ))

    cells.append(md(
        "## Notes\n\n"
        "- **Train/test split is strictly temporal** — no leakage. The split date is the 80th "
        "percentile of unique decision dates.\n"
        "- **NaN preserved** in features when a market saw zero buys/sells on a token side in the "
        "window — XGBoost handles natively.\n"
        "- The `window=30d` dataset is saved alongside; rerun `load_split` with `window_days=30` to "
        "fit a comparison model.\n"
        "- For production, swap the criterion (`score_fn=score_money_ratio`, etc.) or change "
        "`PLAYER_SIDE` in `compute_player_stats` to study maker- vs taker-only behaviour."
    ))

    nb.cells = cells
    out = Path(__file__).resolve().parents[1] / "examples" \
          / "05-ml-dataset-and-baseline.ipynb"
    out.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(nb, out)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
