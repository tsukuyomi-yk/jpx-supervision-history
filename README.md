# 監理・整理銘柄一覧取得

JPX の「監理・整理銘柄一覧」の指定履歴を取得し、イザナミ向けの `supervision.csv` を生成するスクリプトです。

詳細は [manual.md](manual.md) と [spec.md](spec.md) を参照してください。

## 必要環境

- Python 3.11 以上

## セットアップ

```powershell
pip install -r requirements.txt
```

## 実行例

### 通常更新

```powershell
python .\scrape_jpx_supervision_history.py
```

### 全件再取得

```powershell
python .\scrape_jpx_supervision_history.py --full
```

### state無視して更新

```powershell
python .\scrape_jpx_supervision_history.py --no-state
```

## 生成ファイル

- `supervision.csv`
- `state.json`

上記 2 ファイルは実行時に生成し、リポジトリには含めません。

## 注意

- JPX のページ構造変更により取得できなくなる場合があります。
- 実行にはネットワーク接続が必要です。

## ライセンス

MIT
