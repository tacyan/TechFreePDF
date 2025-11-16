# TechFreePDF

GitHub上で公開されている無料の技術書PDFを128並列で自動ダウンロードするPythonスクリプトです。

## 概要

このプロジェクトは、OS、システムプログラミング、コンピュータサイエンスなど、普遍的な技術に関する無料のPDF書籍を自動的に収集・ダウンロードするシステムです。128並列処理により、大量のPDFファイルを効率的にダウンロードできます。

## 主な機能

### 1. 128並列ダウンロード
- `asyncio`と`aiohttp`を使用した非同期処理により、最大128並列でPDFをダウンロード
- セマフォによる並列実行数の制御
- TCPコネクタの最適化による高速ダウンロード

### 2. 複数ソースからのURL収集
以下のソースからPDFのURLを自動収集します：

- **GitHub API**: GitHubリポジトリ内のPDFファイルを検索（複数ブランチ対応）
- **HTML解析**: HTMLページからPDFリンクを抽出（BeautifulSoup使用）
- **Markdown解析**: Markdownファイル内のPDFリンクを抽出
- **対象サイト**:
  - EbookFoundation/free-programming-books
  - Rust関連リポジトリ（rust-lang/book、comprehensive-rust等）
  - Go関連リポジトリ（progit/progit2等）
  - OS・システムプログラミング関連リポジトリ
  - コンピュータサイエンス関連リポジトリ
  - Build Insider、O'Reilly Open Books、MIT OpenCourseWare等

### 3. 重複チェック機能
- **URLベース**: 同じURLの重複を防止
- **ファイル名ベース**: 同じファイル名の重複を防止
- **ハッシュ値ベース**: MD5ハッシュによる内容の重複検出
- 番号付きファイル（例: `file_123.pdf`）の自動削除

### 4. PDFファイル検証
- マジックナンバー（`%PDF`）によるPDF形式の検証
- ファイルサイズの最小値チェック（100バイト以上）
- PDFバージョンの確認
- 無効なファイルの自動削除

### 5. 自動ファイル名リネーム
- PDFメタデータからタイトルを抽出（`pypdf`または`PyPDF2`使用）
- ページ内容からタイトルを推測（最初の3ページを解析）
- 不適切なファイル名（数字のみ、短すぎる名前等）の自動修正
- ファイルシステムで使用できない文字の自動置換

### 6. エラーハンドリングとリトライ
- 最大3回のリトライ機能
- 指数バックオフによるリトライ間隔の調整
- タイムアウト処理（30秒）
- 詳細なエラーメッセージの出力

### 7. 進捗表示
- ダウンロード進捗のリアルタイム表示
- 成功/失敗/スキップ件数の集計
- 実行時間の計測と表示

## システムアーキテクチャ

### 非同期処理フロー

```
main_async()
├── remove_duplicate_files()          # 既存の重複ファイル削除
├── collect_pdf_urls_from_sources()  # 128並列でURL収集
│   ├── extract_pdf_urls_from_github_api()
│   ├── extract_pdf_urls_from_html()
│   └── extract_pdf_urls_from_markdown()
├── download_all_pdfs()               # 128並列でダウンロード
│   └── download_pdf()               # 個別PDFダウンロード（リトライ機能付き）
├── remove_duplicate_files()          # ダウンロード後の重複削除
├── remove_duplicate_content_files() # 128並列でハッシュ値計算・重複削除
├── remove_non_pdf_files()            # 128並列でPDF検証・無効ファイル削除
└── rename_pdfs_with_titles()        # 128並列でタイトル抽出・リネーム
```

### 主要関数一覧

| 関数名 | 機能 | 並列数 |
|--------|------|--------|
| `collect_pdf_urls_from_sources()` | 複数ソースからPDFのURLを収集 | 128 |
| `download_all_pdfs()` | すべてのPDFをダウンロード | 128 |
| `remove_duplicate_content_files()` | ハッシュ値による重複削除 | 128 |
| `remove_non_pdf_files()` | PDF検証と無効ファイル削除 | 128 |
| `rename_pdfs_with_titles()` | タイトル抽出とリネーム | 128 |

## インストール

### 必要な環境
- Python 3.7以上
- pip

### 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

または個別にインストール：

```bash
pip install aiohttp beautifulsoup4 pypdf
```

### オプション: PyPDF2の使用
`pypdf`が利用できない場合、`PyPDF2`をインストールすることでタイトル抽出機能が使用できます：

```bash
pip install PyPDF2
```

## 使用方法

### 基本的な使用方法

```bash
python download_pdfs.py
```

### 実行フロー

1. **既存の重複ファイル削除**: 番号付きファイルなどの重複を削除
2. **URL収集**: 128並列で複数ソースからPDFのURLを収集
3. **ダウンロード**: 128並列でPDFをダウンロード
4. **重複削除**: ファイル名とハッシュ値による重複を削除
5. **PDF検証**: 無効なPDFファイルを削除
6. **ファイル名リネーム**: PDFタイトルから適切なファイル名を生成

### ダウンロード先

PDFファイルは `downloaded_pdfs/` ディレクトリに保存されます。

## 設定

### 並列数の変更

デフォルトでは128並列で実行されます。並列数を変更する場合は、以下の関数の`concurrency`パラメータを変更してください：

- `collect_pdf_urls_from_sources(concurrency=128)`
- `download_all_pdfs(urls, concurrency=128)`
- `remove_duplicate_content_files(concurrency=128)`
- `remove_non_pdf_files(concurrency=128)`
- `rename_pdfs_with_titles(concurrency=128)`

### PDFソースの追加

`PDF_SOURCE_URLS`リストに新しいURLを追加することで、収集対象のソースを増やすことができます。

### デフォルトPDFリストの追加

`PDF_URLS`リストに`(URL, ファイル名)`のタプルを追加することで、常にダウンロードするPDFを指定できます。

## ファイル構成

```
TechFreePDF/
├── download_pdfs.py          # メインスクリプト
├── requirements.txt          # 依存パッケージリスト
├── README.md                 # このファイル
├── .gitignore               # Git除外設定
└── downloaded_pdfs/        # ダウンロードしたPDFファイル（自動生成）
```

## エラーハンドリング

### リトライ機能
- 各ダウンロードは最大3回までリトライされます
- リトライ間隔は指数バックオフ（1秒、2秒、3秒）

### エラーメッセージ
- HTTPエラー: `HTTP {status_code} (attempt {attempt_number})`
- タイムアウト: `Timeout (attempt {attempt_number})`
- その他のエラー: `Error: {error_message} (attempt {attempt_number})`

### スキップされるケース
- 既に存在するファイル名
- 既存ファイルと内容が同じ（ハッシュ値一致）
- 無効なPDFコンテンツ
- タイムアウトやネットワークエラー（リトライ後も失敗）

## 制限事項

- GitHubのファイルサイズ制限（100MB）を超えるPDFは自動的に除外されます
- 無効なURLはスキップされます
- ダウンロードに失敗した場合はリトライを試みますが、最終的に失敗した場合はスキップされます
- 同じ書籍は1つだけダウンロードされます（重複チェック機能により）

## パフォーマンス

- **並列数**: 128並列
- **タイムアウト**: 30秒（ダウンロード）、60秒（URL収集）
- **リトライ**: 最大3回
- **TCPコネクタ**: 並列数に合わせて最適化

## トラブルシューティング

### PDFライブラリがインストールされていない場合
タイトル抽出機能が使用できませんが、ダウンロード機能は正常に動作します。

```
警告: PDFライブラリ（pypdfまたはPyPDF2）がインストールされていません。PDFタイトル取得機能が使用できません。
```

### メモリ不足が発生する場合
並列数を減らすことでメモリ使用量を削減できます。

### ネットワークエラーが頻発する場合
タイムアウト時間を延長するか、リトライ回数を増やすことができます。

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 貢献

プルリクエストやイシューの報告を歓迎します。

## 謝辞

以下のリソースからPDFを収集しています：
- [EbookFoundation/free-programming-books](https://github.com/EbookFoundation/free-programming-books)
- [rust-lang/book](https://github.com/rust-lang/book)
- [progit/progit2](https://github.com/progit/progit2)
- その他、多くのオープンソースプロジェクトと無料技術書サイト

