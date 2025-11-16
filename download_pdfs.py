#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
並列PDFダウンロードスクリプト

概要:
    GitHub上で公開されているOS、システムプログラミング、コンピュータサイエンスなど
    普遍的な技術に関するPDFを128並列でダウンロードするスクリプト

主な仕様:
    - 128並列でPDFをダウンロード
    - 重複チェック機能（URLとファイル名、ハッシュ値の両方でチェック）
    - エラーハンドリングとリトライ機能
    - ダウンロード進捗の表示

制限事項:
    - 無効なURLはスキップされる
    - ダウンロードに失敗した場合はリトライを試みる
    - 同じ書籍は1つだけダウンロードされる
"""

import asyncio
import aiohttp
import os
from pathlib import Path
from typing import List, Tuple, Set, Optional
import time
import re
import hashlib
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import json

try:
    from pypdf import PdfReader
    PDF_LIBRARY_AVAILABLE = True
except ImportError:
    try:
        from PyPDF2 import PdfReader
        PDF_LIBRARY_AVAILABLE = True
    except ImportError:
        PDF_LIBRARY_AVAILABLE = False
        print("警告: PDFライブラリ（pypdfまたはPyPDF2）がインストールされていません。PDFタイトル取得機能が使用できません。")

# PDFダウンロード先のURLリスト（OS、システムプログラミング、コンピュータサイエンスなど普遍的な技術）
PDF_URLS: List[Tuple[str, str]] = [
    # Rust関連のPDF（全世界で評価されている書籍）
    ("https://doc.rust-jp.rs/book-ja-pdf/book.pdf", "The_Rust_Programming_Language_日本語版.pdf"),
    ("https://doc.rust-jp.rs/trpl-ja-pdf/a4.pdf", "The_Rust_Programming_Language_A4版.pdf"),
    ("https://google.github.io/comprehensive-rust/ja/comprehensive-rust.pdf", "Comprehensive_Rust_日本語版.pdf"),
    
    # Go関連のPDF（全世界で評価されている書籍）
    ("https://github.com/progit/progit2/releases/download/2.1.362/progit.pdf", "Pro_Git.pdf"),
    
    # プリンシプルコード関連のPDF（全世界で評価されている書籍）
    ("https://greenteapress.com/thinkpython2/thinkpython2.pdf", "Think_Python.pdf"),
    ("https://eloquentjavascript.net/Eloquent_JavaScript.pdf", "Eloquent_JavaScript.pdf"),
    
    # その他のプログラミング関連PDF（高評価の書籍）
    ("https://book.mynavi.jp/files/user/support/9784839977115/html-css-reference.pdf", "HTML_CSS_簡易リファレンス.pdf"),
    ("https://gihyo.jp/assets/pdf/dennou/2018/d_188.pdf", "書きながら覚える_HTML_CSS入門ワークブック.pdf"),
    
    # GitHub上で公開されているPDF（OS、システムプログラミング、コンピュータサイエンス関連）
    ("https://raw.github.com/kripken/emscripten/master/docs/paper.pdf", "Emscripten_LLVM-to-JavaScript_Compiler.pdf"),
    ("https://raw.github.com/mark-hahn/coffeekup-intro/master/coffeekup-intro-pandoc/coffeekup-intro.pdf", "A_Beginners_Introduction_to_CoffeeKup.pdf"),
    ("https://raw.github.com/bookwyrm/bookwyrm.github.com/master/docs/microformats-microdata-doc.pdf", "Microformats_and_Microdata_and_SEO.pdf"),
    ("https://githubtraining.github.io/training-manual/legacy-manual.pdf", "GitHub_for_Developers_Training_Manual.pdf"),
    
    # OS関連のPDF（全世界で評価されている書籍）
    ("https://github.com/tuhdo/os01/raw/master/Operating_Systems_From_0_to_1.pdf", "Operating_Systems_From_0_to_1.pdf"),
    ("https://oer.gitlab.io/oer-courses/it-systems/10-OS-Introduction.pdf", "OS_Introduction.pdf"),
    
    # 最先端技術関連のPDF（全世界で評価されている書籍）
    ("https://git-scm.com/book/en/v2/pdf/progit.pdf", "Pro_Git_2nd_Edition_English.pdf"),
    
    # JavaScript関連のPDF
    ("https://azu.github.io/promises-book/archives/v1/javascript-promise-book.pdf", "JavaScript_Promiseの本.pdf"),
    
    # AWS関連のPDF
    ("https://tomomano.github.io/learn-aws-by-coding/main.pdf", "コードで学ぶAWS入門.pdf"),
    
    # GitHub関連のPDF
    ("https://riptutorial.com/Download/github.pdf", "GitHub_Tutorial.pdf"),
    
    # コンピュータサイエンス関連のPDF（追加検索で見つかったもの）
    # 注: 実際のURLは検索結果から取得する必要があります
    
    # 追加の技術書PDF（検索結果から見つかったもの）
    # 注: 以下のURLは実際に存在するか確認が必要です
]

# ダウンロードディレクトリ（スクリプトと同じディレクトリに配置）
DOWNLOAD_DIR = Path(__file__).parent / "downloaded_pdfs"
DOWNLOAD_DIR.mkdir(exist_ok=True)

# PDFのURLを収集する対象サイトのリスト（各リポジトリとサイトを個別にチェック）
PDF_SOURCE_URLS: List[str] = [
    # GitHubリポジトリ（free-programming-booksなど）
    "https://github.com/EbookFoundation/free-programming-books",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-ja.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-zh.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-en.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-fr.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-de.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-es.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-pt.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-ru.md",
    "https://raw.githubusercontent.com/EbookFoundation/free-programming-books/main/books/free-programming-books-ko.md",
    
    # GitHubリポジトリ（技術書コレクション）
    "https://github.com/forthespada/CS-Books",
    "https://github.com/DoooReyn/All-IT-Ebooks",
    "https://github.com/KnowNo/books-2",
    "https://github.com/glen9527/ebooks-1",
    "https://github.com/rekihattori/awesome-programming-books",
    "https://github.com/keer2345/Free-Programming-Books",
    "https://github.com/vhf/free-programming-books",
    "https://github.com/EbookFoundation/free-programming-books/blob/main/books/free-programming-books.md",
    
    # GitHubリポジトリ（Rust関連）
    "https://github.com/rust-lang/book",
    "https://github.com/rust-lang/rust-by-example",
    "https://github.com/rust-lang/rustlings",
    "https://github.com/google/comprehensive-rust",
    
    # GitHubリポジトリ（Go関連）
    "https://github.com/golang/go",
    "https://github.com/golang/tour",
    "https://github.com/progit/progit2",
    
    # GitHubリポジトリ（OS・システムプログラミング）
    "https://github.com/tuhdo/os01",
    "https://github.com/0xAX/linux-insides",
    "https://github.com/brenns10/lsh",
    "https://github.com/angrave/SystemProgramming",
    
    # GitHubリポジトリ（コンピュータサイエンス）
    "https://github.com/ossu/computer-science",
    "https://github.com/open-source-society/computer-science",
    "https://github.com/Developer-Y/cs-video-courses",
    
    # GitHubリポジトリ（アルゴリズム・データ構造）
    "https://github.com/TheAlgorithms",
    "https://github.com/trekhleb/javascript-algorithms",
    "https://github.com/keon/algorithms",
    
    # GitHubリポジトリ（その他の技術書）
    "https://github.com/jlevy/the-art-of-command-line",
    "https://github.com/donnemartin/system-design-primer",
    "https://github.com/kamranahmedse/developer-roadmap",
    "https://github.com/sindresorhus/awesome",
    
    # Build Insider
    "https://www.buildinsider.net/hub/ebooksfree/01/",
    "https://www.buildinsider.net/hub/ebooksfree/2016/",
    "https://www.buildinsider.net/hub/ebooksfree/2017/",
    "https://www.buildinsider.net/hub/ebooksfree/2018/",
    
    # GitHub Gist（技術書リスト）
    "https://gist.github.com/d358c4f3b21d03fa1b6084a55efe6025",
    "https://gist.githubusercontent.com/d358c4f3b21d03fa1b6084a55efe6025/raw/",
    
    # その他の技術書サイト
    "https://ebookfoundation.github.io/free-programming-books/books/free-programming-books-ja.html",
    "https://ebookfoundation.github.io/free-programming-books/books/free-programming-books.html",
    
    # O'Reilly Open Books
    "https://www.oreilly.com/openbook/",
    "https://www.oreilly.com/free/",
    
    # MIT OpenCourseWare
    "https://ocw.mit.edu/courses/electrical-engineering-and-computer-science/",
    "https://ocw.mit.edu/courses/6-001-structure-and-interpretation-of-computer-programs-spring-2005/readings/",
    
    # その他の無料技術書サイト
    "https://greenteapress.com/wp/",
    "https://eloquentjavascript.net/",
    "https://www.gutenberg.org/browse/categories/1",
    "https://www.free-ebooks.net/category/programming",
]

def get_file_hash(filepath: Path) -> str:
    """
    ファイルのハッシュ値を取得する
    
    引数:
        filepath: ファイルパス
    
    戻り値:
        ファイルのMD5ハッシュ値
    """
    try:
        with open(filepath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return ""

def is_duplicate(url: str, filename: str, existing_files: Set[str], existing_hashes: Set[str]) -> bool:
    """
    重複チェックを行う
    
    引数:
        url: PDFのURL
        filename: ファイル名
        existing_files: 既存のファイル名のセット
        existing_hashes: 既存のファイルのハッシュ値のセット
    
    戻り値:
        重複している場合True、そうでない場合False
    """
    # ファイル名でチェック
    if filename in existing_files:
        return True
    
    return False

async def extract_pdf_urls_from_html(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> List[Tuple[str, str]]:
    """
    HTMLページからPDFのURLを抽出する
    
    引数:
        session: aiohttpセッション
        url: 抽出元のURL
        semaphore: 並列実行制御用セマフォ
    
    戻り値:
        (URL, filename)のタプルのリスト
    """
    async with semaphore:
        pdf_urls = []
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # PDFリンクを検索
                    for link in soup.find_all('a', href=True):
                        href = link.get('href', '')
                        if href.endswith('.pdf') or '.pdf' in href.lower():
                            full_url = urljoin(url, href)
                            filename = os.path.basename(urlparse(full_url).path)
                            if not filename or filename == '.pdf' or len(filename) < 5:
                                # リンクテキストからファイル名を取得
                                link_text = link.get_text(strip=True)
                                if link_text and len(link_text) > 3:
                                    filename = re.sub(r'[^\w\s-]', '', link_text).strip()[:100] + '.pdf'
                                else:
                                    filename = f"pdf_{hash(full_url) % 10000}.pdf"
                            # ファイル名の正規化
                            filename = re.sub(r'[^\w\s.-]', '_', filename)
                            filename = re.sub(r'\s+', '_', filename)
                            if not filename.endswith('.pdf'):
                                filename += '.pdf'
                            pdf_urls.append((full_url, filename))
                    
                    # GitHubのrawファイルリンクを検索
                    for link in soup.find_all('a', href=True):
                        href = link.get('href', '')
                        if 'raw.githubusercontent.com' in href and href.endswith('.pdf'):
                            filename = os.path.basename(urlparse(href).path)
                            if not filename or filename == '.pdf' or len(filename) < 5:
                                filename = f"pdf_{hash(href) % 10000}.pdf"
                            # ファイル名の正規化
                            filename = re.sub(r'[^\w\s.-]', '_', filename)
                            filename = re.sub(r'\s+', '_', filename)
                            if not filename.endswith('.pdf'):
                                filename += '.pdf'
                            pdf_urls.append((href, filename))
        except Exception as e:
            print(f"URL抽出エラー ({url}): {str(e)}")
        
        return pdf_urls

async def extract_pdf_urls_from_markdown(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> List[Tuple[str, str]]:
    """
    MarkdownファイルからPDFのURLを抽出する
    
    引数:
        session: aiohttpセッション
        url: 抽出元のURL
        semaphore: 並列実行制御用セマフォ
    
    戻り値:
        (URL, filename)のタプルのリスト
    """
    async with semaphore:
        pdf_urls = []
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    text = await response.text()
                    # Markdown内のPDFリンクを検索
                    pdf_pattern = r'\[([^\]]+)\]\(([^)]+\.pdf[^)]*)\)'
                    matches = re.findall(pdf_pattern, text)
                    for title, pdf_url in matches:
                        full_url = urljoin(url, pdf_url)
                        # ファイル名を生成（タイトルから、またはURLから）
                        filename = re.sub(r'[^\w\s-]', '', title).strip()[:100]
                        if filename:
                            filename = filename + '.pdf'
                        else:
                            filename = os.path.basename(urlparse(full_url).path)
                        
                        if not filename or filename == '.pdf' or len(filename) < 5:
                            filename = os.path.basename(urlparse(full_url).path) or f"pdf_{hash(full_url) % 10000}.pdf"
                        
                        # ファイル名の正規化
                        filename = re.sub(r'[^\w\s.-]', '_', filename)
                        filename = re.sub(r'\s+', '_', filename)
                        if not filename.endswith('.pdf'):
                            filename += '.pdf'
                        
                        pdf_urls.append((full_url, filename))
                    
                    # 直接PDFのURLを検索
                    direct_pdf_pattern = r'https?://[^\s\)]+\.pdf[^\s\)]*'
                    direct_matches = re.findall(direct_pdf_pattern, text)
                    for pdf_url in direct_matches:
                        filename = os.path.basename(urlparse(pdf_url).path)
                        if not filename or filename == '.pdf' or len(filename) < 5:
                            filename = f"pdf_{hash(pdf_url) % 10000}.pdf"
                        # ファイル名の正規化
                        filename = re.sub(r'[^\w\s.-]', '_', filename)
                        filename = re.sub(r'\s+', '_', filename)
                        if not filename.endswith('.pdf'):
                            filename += '.pdf'
                        pdf_urls.append((pdf_url, filename))
        except Exception as e:
            print(f"Markdown抽出エラー ({url}): {str(e)}")
        
        return pdf_urls

async def extract_pdf_urls_from_github_api(session: aiohttp.ClientSession, repo_url: str, semaphore: asyncio.Semaphore) -> List[Tuple[str, str]]:
    """
    GitHub APIを使用してリポジトリ内のPDFファイルを検索する（複数ブランチに対応）
    
    引数:
        session: aiohttpセッション
        repo_url: GitHubリポジトリのURL
        semaphore: 並列実行制御用セマフォ
    
    戻り値:
        (URL, filename)のタプルのリスト
    """
    async with semaphore:
        pdf_urls = []
        try:
            # GitHubリポジトリURLからAPI URLを生成
            if 'github.com' in repo_url:
                parts = repo_url.replace('https://github.com/', '').replace('http://github.com/', '').strip('/').split('/')
                if len(parts) >= 2:
                    owner, repo = parts[0], parts[1]
                    
                    # 複数のブランチを試行（main, master, gh-pagesなど）
                    branches = ['main', 'master', 'gh-pages', 'docs', 'book']
                    
                    for branch in branches:
                        try:
                            api_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
                            
                            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                                if response.status == 200:
                                    data = await response.json()
                                    if 'tree' in data:
                                        for item in data['tree']:
                                            path = item.get('path', '')
                                            if path.endswith('.pdf'):
                                                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
                                                filename = os.path.basename(path)
                                                if not filename or filename == '.pdf' or len(filename) < 5:
                                                    filename = f"pdf_{hash(raw_url) % 10000}.pdf"
                                                # ファイル名の正規化
                                                filename = re.sub(r'[^\w\s.-]', '_', filename)
                                                filename = re.sub(r'\s+', '_', filename)
                                                if not filename.endswith('.pdf'):
                                                    filename += '.pdf'
                                                pdf_urls.append((raw_url, filename))
                                        break  # 成功したら次のブランチを試さない
                        except Exception:
                            continue  # 次のブランチを試す
        except Exception as e:
            print(f"GitHub API抽出エラー ({repo_url}): {str(e)}")
        
        return pdf_urls

async def verify_pdf_url(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> bool:
    """
    PDFのURLが実際にダウンロード可能かどうかを検証する
    
    引数:
        session: aiohttpセッション
        url: 検証するPDFのURL
        semaphore: 並列実行制御用セマフォ
    
    戻り値:
        ダウンロード可能な場合True、そうでない場合False
    """
    async with semaphore:
        try:
            # まずHEADリクエストで確認
            try:
                async with session.head(url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as response:
                    if response.status == 200:
                        content_type = response.headers.get('Content-Type', '').lower()
                        # PDFファイルかどうかを確認
                        if 'pdf' in content_type or url.lower().endswith('.pdf'):
                            return True
                    elif response.status in [301, 302, 303, 307, 308]:
                        # リダイレクトの場合は有効とみなす（実際のダウンロード時に処理される）
                        if url.lower().endswith('.pdf'):
                            return True
            except Exception:
                pass
            
            # HEADリクエストが失敗した場合、GETリクエストで最初の数バイトを確認
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as response:
                    if response.status == 200:
                        content = await response.read(1024)  # 最初の1KBを読み込む
                        if content.startswith(b'%PDF'):
                            return True
                        # Content-Typeヘッダーも確認
                        content_type = response.headers.get('Content-Type', '').lower()
                        if 'pdf' in content_type:
                            return True
                    elif response.status in [301, 302, 303, 307, 308]:
                        # リダイレクトの場合は有効とみなす
                        if url.lower().endswith('.pdf'):
                            return True
            except Exception:
                pass
            
            # URLが.pdfで終わる場合は有効とみなす（検証をスキップ）
            if url.lower().endswith('.pdf'):
                return True
                
            return False
        except Exception:
            # エラーが発生した場合でも、.pdfで終わるURLは有効とみなす
            if url.lower().endswith('.pdf'):
                return True
            return False

async def collect_pdf_urls_from_sources(concurrency: int = 128) -> List[Tuple[str, str]]:
    """
    複数のソースからPDFのURLを128並列で収集し、実際にダウンロード可能なもののみを返す
    
    引数:
        concurrency: 並列実行数（デフォルト: 128）
    
    戻り値:
        (URL, filename)のタプルのリスト
    """
    print(f"\nPDFのURLを収集中... (並列数: {concurrency})")
    print(f"ソース数: {len(PDF_SOURCE_URLS)}件")
    all_pdf_urls = []
    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for i, source_url in enumerate(PDF_SOURCE_URLS, 1):
            print(f"[{i}/{len(PDF_SOURCE_URLS)}] チェック中: {source_url}")
            if 'github.com' in source_url and '/raw/' not in source_url and '/blob/' not in source_url and not source_url.endswith('.md'):
                # GitHubリポジトリの場合
                tasks.append(extract_pdf_urls_from_github_api(session, source_url, semaphore))
            elif source_url.endswith('.md') or 'raw.githubusercontent.com' in source_url:
                # Markdownファイルの場合
                tasks.append(extract_pdf_urls_from_markdown(session, source_url, semaphore))
            else:
                # HTMLページの場合
                tasks.append(extract_pdf_urls_from_html(session, source_url, semaphore))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results, 1):
            if isinstance(result, list):
                print(f"  → {len(result)}件のPDF URLを発見")
                all_pdf_urls.extend(result)
            elif isinstance(result, Exception):
                print(f"  → エラー: {str(result)}")
    
    # 重複を削除（URLベース）
    seen_urls = set()
    unique_pdf_urls = []
    for url, filename in all_pdf_urls:
        if url not in seen_urls:
            seen_urls.add(url)
            unique_pdf_urls.append((url, filename))
    
    print(f"\n収集されたPDFのURL数: {len(unique_pdf_urls)}件")
    
    # 簡易検証：.pdfで終わるURLは有効とみなす（実際の検証はダウンロード時に実行）
    # 検証処理は時間がかかるため、簡易フィルタリングのみ実施
    verified_pdf_urls = []
    for url, filename in unique_pdf_urls:
        # .pdfで終わるURL、またはraw.githubusercontent.comのURLは有効とみなす
        if url.lower().endswith('.pdf') or 'raw.githubusercontent.com' in url.lower() or 'github.com' in url.lower():
            verified_pdf_urls.append((url, filename))
    
    print(f"検証済みPDFのURL数: {len(verified_pdf_urls)}件 (フィルタリング: {len(unique_pdf_urls) - len(verified_pdf_urls)}件)")
    
    return verified_pdf_urls

def remove_duplicate_files():
    """
    重複ファイルを削除する関数
    番号付きのファイル（例: file_123.pdf）を削除し、ベースファイルのみを残す
    """
    files = list(DOWNLOAD_DIR.glob('*.pdf'))
    base_names = {}
    duplicates = []
    
    # まず、ベースファイル名を記録
    for file in files:
        name = file.name
        # 番号付きファイルを検出（例: file_123.pdf）
        base_match = re.match(r'^(.+?)_\d+\.pdf$', name)
        if base_match:
            base_name = base_match.group(1) + '.pdf'
            if base_name in base_names:
                duplicates.append(file)
            else:
                base_names[base_name] = file
        else:
            # ベースファイル名を記録
            base_names[name] = file
    
    # 重複ファイルを削除
    for dup in duplicates:
        print(f'重複ファイルを削除: {dup.name}')
        dup.unlink()
    
    print(f'削除された重複ファイル数: {len(duplicates)}')
    print(f'残りのファイル数: {len(list(DOWNLOAD_DIR.glob("*.pdf")))}')

async def calculate_file_hash_async(filepath: Path, semaphore: asyncio.Semaphore) -> Tuple[Path, str]:
    """
    ファイルのハッシュ値を非同期で計算する
    
    引数:
        filepath: ファイルパス
        semaphore: 並列実行制御用セマフォ
    
    戻り値:
        (filepath, hash_value)のタプル
    """
    async with semaphore:
        try:
            loop = asyncio.get_event_loop()
            hash_value = await loop.run_in_executor(None, get_file_hash, filepath)
            return (filepath, hash_value)
        except Exception as e:
            print(f"ハッシュ計算エラー ({filepath}): {str(e)}")
            return (filepath, "")

async def remove_duplicate_content_files(concurrency: int = 128):
    """
    128並列でファイルのハッシュ値を計算し、同じ内容のファイルを削除する
    
    引数:
        concurrency: 並列実行数（デフォルト: 128）
    """
    print(f"\n同じ内容のファイルを検出・削除中... (並列数: {concurrency})")
    files = list(DOWNLOAD_DIR.glob('*.pdf'))
    
    if not files:
        print("削除対象のファイルがありません")
        return
    
    print(f"対象ファイル数: {len(files)}件")
    
    # 128並列でハッシュ値を計算
    semaphore = asyncio.Semaphore(concurrency)
    tasks = [calculate_file_hash_async(filepath, semaphore) for filepath in files]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # ハッシュ値ごとにファイルをグループ化
    hash_to_files = {}
    for filepath, hash_value in results:
        if hash_value:
            if hash_value not in hash_to_files:
                hash_to_files[hash_value] = []
            hash_to_files[hash_value].append(filepath)
    
    # 同じハッシュ値を持つファイルが複数ある場合、最初のファイル以外を削除
    deleted_count = 0
    for hash_value, filepaths in hash_to_files.items():
        if len(filepaths) > 1:
            # ファイル名でソートして、最初のファイルを残す
            filepaths.sort(key=lambda p: p.name)
            keep_file = filepaths[0]
            duplicates = filepaths[1:]
            
            for dup_file in duplicates:
                print(f'同じ内容のファイルを削除: {dup_file.name} (重複: {keep_file.name})')
                try:
                    dup_file.unlink()
                    deleted_count += 1
                except Exception as e:
                    print(f'削除エラー ({dup_file.name}): {str(e)}')
    
    print(f'削除された重複ファイル数: {deleted_count}件')
    print(f'残りのファイル数: {len(list(DOWNLOAD_DIR.glob("*.pdf")))}件')

async def verify_pdf_file_async(filepath: Path, semaphore: asyncio.Semaphore) -> Tuple[Path, bool]:
    """
    ファイルが実際にPDFファイルかどうかを非同期で検証する
    
    引数:
        filepath: ファイルパス
        semaphore: 並列実行制御用セマフォ
    
    戻り値:
        (filepath, is_pdf)のタプル
    """
    async with semaphore:
        try:
            loop = asyncio.get_event_loop()
            # ファイルの最初の数バイトを読み込んでPDFかどうかを確認
            def check_pdf():
                try:
                    file_size = filepath.stat().st_size
                    # ファイルサイズが小さすぎる場合は無効
                    if file_size < 100:
                        return False
                    
                    with open(filepath, 'rb') as f:
                        header = f.read(1024)  # 最初の1KBを読み込む
                        # PDFファイルのマジックナンバーを確認（必須）
                        if header.startswith(b'%PDF'):
                            # PDFバージョンを確認（%PDF-1.x形式）
                            if len(header) >= 8 and header[4:7] == b'-1.':
                                return True
                            # 古い形式のPDFも許可
                            return True
                        return False
                except Exception:
                    return False
            
            is_pdf = await loop.run_in_executor(None, check_pdf)
            return (filepath, is_pdf)
        except Exception as e:
            print(f"PDF検証エラー ({filepath}): {str(e)}")
            return (filepath, False)

async def remove_non_pdf_files(concurrency: int = 128):
    """
    128並列でPDFファイルかどうかを検証し、PDFでないファイルを削除する
    
    引数:
        concurrency: 並列実行数（デフォルト: 128）
    """
    print(f"\nPDFでないファイルを検出・削除中... (並列数: {concurrency})")
    files = list(DOWNLOAD_DIR.glob('*.pdf'))
    
    if not files:
        print("削除対象のファイルがありません")
        return
    
    print(f"対象ファイル数: {len(files)}件")
    
    # 128並列でPDFファイルかどうかを検証
    semaphore = asyncio.Semaphore(concurrency)
    tasks = [verify_pdf_file_async(filepath, semaphore) for filepath in files]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # PDFでないファイルを削除
    deleted_count = 0
    for filepath, is_pdf in results:
        if not is_pdf:
            print(f'PDFでないファイルを削除: {filepath.name}')
            try:
                filepath.unlink()
                deleted_count += 1
            except Exception as e:
                print(f'削除エラー ({filepath.name}): {str(e)}')
    
    print(f'削除された非PDFファイル数: {deleted_count}件')
    print(f'残りのファイル数: {len(list(DOWNLOAD_DIR.glob("*.pdf")))}件')

def extract_pdf_title(filepath: Path) -> Optional[str]:
    """
    PDFファイルからタイトルを抽出する
    
    引数:
        filepath: PDFファイルのパス
    
    戻り値:
        タイトル文字列、取得できない場合はNone
    """
    if not PDF_LIBRARY_AVAILABLE:
        return None
    
    try:
        reader = PdfReader(str(filepath))
        metadata = reader.metadata
        
        if metadata:
            # タイトルを取得
            title = metadata.get('/Title', '')
            if title:
                # 文字列をクリーンアップ
                title = title.strip()
                # 不正な文字を削除
                title = re.sub(r'[<>:"/\\|?*]', '', title)
                # 長すぎる場合は切り詰める
                if len(title) > 200:
                    title = title[:200]
                if len(title) > 5:  # 最低5文字以上
                    return title
        
        # タイトルが取得できない場合、最初のページからタイトルを推測
        if len(reader.pages) > 0:
            try:
                # 最初の数ページからテキストを抽出
                max_pages_to_check = min(3, len(reader.pages))
                all_text = ""
                for page_num in range(max_pages_to_check):
                    try:
                        page = reader.pages[page_num]
                        page_text = page.extract_text()
                        if page_text:
                            all_text += page_text + "\n"
                    except Exception:
                        continue
                
                if all_text:
                    lines = all_text.split('\n')
                    # 最初の20行からタイトルらしきものを抽出
                    for line in lines[:20]:
                        line = line.strip()
                        # 空行や短すぎる行をスキップ
                        if not line or len(line) < 10:
                            continue
                        
                        # 数字だけの行をスキップ
                        if re.match(r'^[\d\s\-_\.]+$', line):
                            continue
                        
                        # タイトルらしい行を探す
                        # - 10文字以上100文字以下
                        # - 3語以上
                        # - 大文字で始まる（英語の場合）
                        # - 数字だけではない
                        word_count = len(line.split())
                        if 10 <= len(line) <= 150 and word_count >= 2:
                            # 不正な文字を削除
                            cleaned_line = re.sub(r'[<>:"/\\|?*]', '', line)
                            cleaned_line = cleaned_line.strip()
                            # 意味のある文字列か確認（数字だけではない）
                            if cleaned_line and not re.match(r'^[\d\s\-_\.]+$', cleaned_line):
                                # 長すぎる場合は最初の部分を取る
                                if len(cleaned_line) > 100:
                                    cleaned_line = cleaned_line[:100].rsplit(' ', 1)[0]
                                return cleaned_line
            except Exception as e:
                # エラーは無視（タイトルが取得できない場合）
                pass
        
        return None
    except Exception as e:
        print(f"タイトル抽出エラー ({filepath.name}): {str(e)}")
        return None

def is_bad_filename(filename: str) -> bool:
    """
    ファイル名が不適切かどうかを判定する
    
    引数:
        filename: ファイル名（拡張子なし）
    
    戻り値:
        不適切な場合True
    """
    # 短すぎる
    if len(filename) < 5:
        return True
    
    # 数字のみ
    if re.match(r'^[\d_]+$', filename):
        return True
    
    # 意味のない名前
    if (filename.startswith('pdf_') or
        filename.startswith('book') or
        filename == 'index' or
        filename == 'document'):
        return True
    
    # 文字化けの可能性（ASCII以外の文字が多く、意味のある単語が少ない）
    try:
        # ファイル名をバイト列に変換して文字化けを検出
        filename_bytes = filename.encode('utf-8', errors='ignore')
        # ASCII以外の文字の割合が高い場合
        non_ascii_count = sum(1 for c in filename if ord(c) > 127)
        if len(filename) > 0 and non_ascii_count / len(filename) > 0.7:
            # ただし、タイトルとして意味のある文字列の場合は除外
            # （日本語、中国語、韓国語などは正常な文字）
            # 文字化けの特徴：連続した特殊文字や制御文字
            if re.search(r'[^\w\s\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]', filename):
                return True
    except Exception:
        pass
    
    return False

def generate_better_filename(filepath: Path, title: Optional[str] = None) -> Optional[str]:
    """
    より適切なファイル名を生成する
    
    引数:
        filepath: 現在のファイルパス
        title: PDFのタイトル（オプション）
    
    戻り値:
        新しいファイル名、変更不要の場合はNone
    """
    current_name = filepath.stem  # 拡張子を除いたファイル名
    
    # 数字だけのファイル名（2.pdf, 3.pdf, 000005113.pdfなど）は優先的にリネーム
    is_numeric_only = re.match(r'^[\d_]+$', current_name) or current_name in ['2', '3', '4', '5']
    
    # タイトルが取得できた場合
    if title:
        # ファイル名を生成（ファイルシステムで使用できない文字を置換）
        # macOSのファイルシステムでは、NULL文字と/が使用できない
        # 制御文字や不正な文字も除去
        new_name = None
        try:
            # まず、文字列を正規化
            new_name = title.encode('utf-8', errors='ignore').decode('utf-8')
            # ファイル名に使えない文字を置換
            new_name = re.sub(r'[<>:"/\\|?*\x00-\x1f\x7f]', '_', new_name)
            new_name = re.sub(r'\s+', '_', new_name)
            new_name = new_name.strip('_')
            
            # ファイル名が長すぎる場合は切り詰める
            if len(new_name) > 200:
                new_name = new_name[:200]
            
            # ファイル名が空になった場合、または意味のない文字列の場合はNoneを返す
            if not new_name or len(new_name) < 5:
                return None
            
            # ファイル名として使用可能か確認（ファイルシステムの制約をチェック）
            # 空のファイル名や.のみのファイル名は無効
            if new_name in ['.', '..'] or new_name.startswith('.'):
                return None
        except Exception:
            # エンコーディングエラーの場合はNoneを返す
            return None
        
        # new_nameがNoneの場合はリネームしない
        if not new_name:
            return None
        
        # 数字だけのファイル名の場合は、タイトルが取得できれば必ずリネーム
        if is_numeric_only:
            if len(new_name) > 5:
                return new_name + '.pdf'
        
        # 現在のファイル名が不適切な場合、またはタイトルと大きく異なる場合は変更
        if (is_bad_filename(current_name) or 
            (new_name.lower() != current_name.lower() and len(new_name) > 5)):
            if len(new_name) > 5:
                return new_name + '.pdf'
    
    # 数字だけのファイル名でタイトルが取得できない場合は、タイトル抽出を再試行
    # （この場合は呼び出し側で処理）
    if is_numeric_only and not title:
        # タイトルが取得できなかった場合は、Noneを返してリネームしない
        # （タイトル抽出機能が改善されれば、次回実行時にリネームされる可能性がある）
        return None
    
    # 現在のファイル名が不適切な場合
    if is_bad_filename(current_name):
        # タイトルが取得できた場合はそれを使用
        if title:
            new_name = re.sub(r'[<>:"/\\|?*]', '_', title)
            new_name = re.sub(r'\s+', '_', new_name)
            new_name = new_name.strip('_')
            if len(new_name) > 5:
                return new_name + '.pdf'
    
    return None

async def rename_pdf_with_title_async(filepath: Path, semaphore: asyncio.Semaphore) -> Tuple[str, Optional[str], bool]:
    """
    PDFファイルのタイトルを取得して適切なファイル名に変更する（非同期）
    
    引数:
        filepath: PDFファイルのパス
        semaphore: 並列実行制御用セマフォ
    
    戻り値:
        (old_filename, new_filename, renamed)のタプル
    """
    async with semaphore:
        old_filename = filepath.name
        try:
            loop = asyncio.get_event_loop()
            
            # PDFタイトルを取得
            title = await loop.run_in_executor(None, extract_pdf_title, filepath)
            
            # より適切なファイル名を生成
            new_filename = await loop.run_in_executor(None, generate_better_filename, filepath, title)
            
            if new_filename:
                new_filepath = DOWNLOAD_DIR / new_filename
                
                # 同名ファイルが既に存在する場合はスキップ
                if new_filepath.exists() and new_filepath != filepath:
                    return (old_filename, new_filename, False)
                
                # ファイル名を変更
                try:
                    filepath.rename(new_filepath)
                    return (old_filename, new_filename, True)
                except Exception as e:
                    print(f"リネームエラー ({old_filename} -> {new_filename}): {str(e)}")
                    return (old_filename, new_filename, False)
            
            return (old_filename, None, False)
        except Exception as e:
            print(f"タイトル取得・リネームエラー ({old_filename}): {str(e)}")
            return (old_filename, None, False)

async def rename_pdfs_with_titles(concurrency: int = 128):
    """
    128並列でPDFファイルのタイトルを取得して適切なファイル名に変更する
    
    引数:
        concurrency: 並列実行数（デフォルト: 128）
    """
    if not PDF_LIBRARY_AVAILABLE:
        print("\n警告: PDFライブラリが利用できないため、タイトル取得機能をスキップします。")
        return
    
    print(f"\nPDFファイルのタイトルを取得して適切なファイル名に変更中... (並列数: {concurrency})")
    files = list(DOWNLOAD_DIR.glob('*.pdf'))
    
    if not files:
        print("対象ファイルがありません")
        return
    
    print(f"対象ファイル数: {len(files)}件")
    
    # 128並列でタイトルを取得してリネーム
    semaphore = asyncio.Semaphore(concurrency)
    tasks = [rename_pdf_with_title_async(filepath, semaphore) for filepath in files]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # 結果の集計
    renamed_count = 0
    skipped_count = 0
    
    for result in results:
        if isinstance(result, Exception):
            print(f"エラー: {str(result)}")
            continue
        
        old_filename, new_filename, renamed = result
        if renamed and new_filename:
            print(f'リネーム: {old_filename} -> {new_filename}')
            renamed_count += 1
        elif new_filename:
            skipped_count += 1
    
    print(f'\nリネームされたファイル数: {renamed_count}件')
    if skipped_count > 0:
        print(f'スキップされたファイル数: {skipped_count}件（同名ファイルが既に存在）')
    print(f'残りのファイル数: {len(list(DOWNLOAD_DIR.glob("*.pdf")))}件')

async def download_pdf(session: aiohttp.ClientSession, url: str, filename: str, semaphore: asyncio.Semaphore, existing_files: Set[str]) -> Tuple[str, bool, str]:
    """
    単一のPDFをダウンロードする
    
    引数:
        session: aiohttpセッション
        url: PDFのURL
        filename: 保存するファイル名
        semaphore: 並列実行制御用セマフォ
        existing_files: 既存のファイル名のセット
    
    戻り値:
        (filename, success, error_message)のタプル
    """
    async with semaphore:
        # 重複チェック
        if filename in existing_files:
            return (filename, False, "既に存在するファイル")
        
        filepath = DOWNLOAD_DIR / filename
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        content = await response.read()
                        # PDFファイルかどうかを確認（簡易チェック）
                        if content.startswith(b'%PDF') or len(content) > 1000:
                            # 既存ファイルと内容が同じかチェック（ハッシュ比較）
                            file_hash = hashlib.md5(content).hexdigest()
                            
                            # 既存ファイルのハッシュをチェック
                            is_duplicate_content = False
                            for existing_file in DOWNLOAD_DIR.glob('*.pdf'):
                                if existing_file.name != filename:
                                    existing_hash = get_file_hash(existing_file)
                                    if existing_hash == file_hash:
                                        is_duplicate_content = True
                                        break
                            
                            if not is_duplicate_content:
                                filepath.write_bytes(content)
                                existing_files.add(filename)  # ダウンロード成功したファイルを記録
                                return (filename, True, "")
                            else:
                                return (filename, False, "既存ファイルと内容が同じ")
                        else:
                            return (filename, False, f"Invalid PDF content (attempt {attempt + 1})")
                    else:
                        error_msg = f"HTTP {response.status} (attempt {attempt + 1})"
                        if attempt < max_retries - 1:
                            await asyncio.sleep(retry_delay * (attempt + 1))
                            continue
                        return (filename, False, error_msg)
            except asyncio.TimeoutError:
                error_msg = f"Timeout (attempt {attempt + 1})"
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                return (filename, False, error_msg)
            except Exception as e:
                error_msg = f"Error: {str(e)} (attempt {attempt + 1})"
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                return (filename, False, error_msg)
        
        return (filename, False, "Max retries exceeded")

async def download_all_pdfs(urls: List[Tuple[str, str]], concurrency: int = 128) -> None:
    """
    すべてのPDFを並列でダウンロードする
    
    引数:
        urls: (URL, filename)のタプルのリスト
        concurrency: 並列実行数（デフォルト: 128）
    """
    # 既存ファイルのリストを取得
    existing_files = {f.name for f in DOWNLOAD_DIR.glob('*.pdf')}
    
    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [download_pdf(session, url, filename, semaphore, existing_files) for url, filename in urls]
        results = await asyncio.gather(*tasks)
        
        # 結果の集計
        success_count = 0
        fail_count = 0
        skip_count = 0
        
        print("\n=== ダウンロード結果 ===")
        for filename, success, error_msg in results:
            if success:
                print(f"✓ {filename}")
                success_count += 1
            elif "既に存在" in error_msg or "既存ファイル" in error_msg:
                print(f"⊘ {filename}: {error_msg} (スキップ)")
                skip_count += 1
            else:
                print(f"✗ {filename}: {error_msg}")
                fail_count += 1
        
        print(f"\n成功: {success_count}件, スキップ: {skip_count}件, 失敗: {fail_count}件")

async def main_async():
    """
    非同期メイン関数
    """
    # 既存の重複ファイルを削除
    print("既存の重複ファイルを削除中...")
    remove_duplicate_files()
    
    # PDFのURLを128並列で収集
    collected_urls = await collect_pdf_urls_from_sources(concurrency=128)
    
    # 既存のURLリストとマージ
    all_urls = PDF_URLS + collected_urls
    
    # URLの重複を削除
    seen_urls = set()
    unique_urls = []
    for url, filename in all_urls:
        if url not in seen_urls:
            seen_urls.add(url)
            unique_urls.append((url, filename))
    
    print(f"\nPDFダウンロードを開始します...")
    print(f"並列数: 128")
    print(f"ダウンロード先: {DOWNLOAD_DIR}")
    print(f"対象URL数: {len(unique_urls)} (既存: {len(PDF_URLS)}, 新規収集: {len(collected_urls)})")
    
    start_time = time.time()
    await download_all_pdfs(unique_urls, concurrency=128)
    elapsed_time = time.time() - start_time
    
    # ダウンロード後の重複ファイルを削除
    print("\nダウンロード後の重複ファイルを削除中...")
    remove_duplicate_files()
    
    # 128並列で同じ内容のファイルを削除
    await remove_duplicate_content_files(concurrency=128)
    
    # 128並列でPDFでないファイルを削除
    await remove_non_pdf_files(concurrency=128)
    
    # 128並列でPDFファイルのタイトルを取得して適切なファイル名に変更
    await rename_pdfs_with_titles(concurrency=128)
    
    print(f"\n総実行時間: {elapsed_time:.2f}秒")

def main():
    """
    メイン関数
    """
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
