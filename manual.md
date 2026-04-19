# 監理・整理銘柄一覧取得マニュアル

## 概要

`scrape_jpx_supervision_history.py` は、JPX の「監理・整理銘柄一覧」の指定履歴ページを巡回し、`supervision.csv` と `state.json` を生成するスクリプトです。

詳細仕様は [spec.md](C:\dropbox\Dropbox\codex_works\py\監理整理銘柄一覧取得\spec.md) を参照してください。

`supervision.csv` の5列目以降は、JPX の `内容` ごとの理由列です。
履歴ページURLは `01.html` から自動検出します。

## 必要環境

- Python 3.11 以上
- `requests`
- `beautifulsoup4`
- `pandas`

インストール例:

```powershell
pip install requests beautifulsoup4 pandas
```

## 実行ファイル

- `scrape_jpx_supervision_history.py`

## 実行方法

### 通常実行

```powershell
python .\scrape_jpx_supervision_history.py
```

用途:

- `state.json` を使った差分更新
- 出力CSVが無い場合は全件巡回して理由列を作成

### 全件再取得

```powershell
python .\scrape_jpx_supervision_history.py --full
```

用途:

- `supervision.csv` を全件で再生成
- `state.json` を最新状態に更新
- 理由列を対象URL全件から再調査

### state 無視実行

```powershell
python .\scrape_jpx_supervision_history.py --no-state
```

用途:

- `state.json` を読まずに取得
- 既存CSVとマージして保存

## オプション

- `--full`
- `--no-state`
- `--output OUTPUT`
- `--state STATE`

例:

```powershell
python .\scrape_jpx_supervision_history.py --output .\out.csv --state .\out-state.json
```

## 実行結果

実行後に標準出力へ以下を表示します。

- 総件数
- 保存先
- 年ごとの取得件数

出力CSV:

- 1-4列目は `コード`, `市場`, `日付`, `日付`
- 5列目以降は `上場廃止の決定・整理銘柄指定` など `内容` ごとの理由列
- 該当列に `1`、非該当列に `0`

表示例:

```text
総件数: 716
保存先: C:\dropbox\Dropbox\codex_works\py\監理整理銘柄一覧取得\supervision.csv
年ごとの取得件数:
  2022: 106
  2023: 121
  2024: 159
  2025: 229
  2026: 101
```
