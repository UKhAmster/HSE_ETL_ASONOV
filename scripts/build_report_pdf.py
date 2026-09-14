#!/usr/bin/env python3
"""Собирает PDF-отчёт из README.md: markdown -> HTML (со встроенными скриншотами) -> PDF через Chrome.

Использование: build_report_pdf.py <README.md> <out.pdf>
"""
import html
import os
import re
import subprocess
import sys
import tempfile
from datetime import date

import markdown

REPO_URL = "https://github.com/UKhAmster/HSE_ETL_ASONOV"
CHROME = "/usr/bin/google-chrome"

CSS = """
@page { size: A4; margin: 18mm 16mm 18mm 16mm; }
body { font-family: "DejaVu Sans", "Noto Sans", sans-serif; font-size: 10.5pt; line-height: 1.4; color: #111; }
h1 { font-size: 20pt; margin: 0 0 6pt; }
h2 { font-size: 15pt; margin: 22pt 0 8pt; border-bottom: 1.5px solid #333; padding-bottom: 3pt; page-break-after: avoid; }
h3 { font-size: 12pt; margin: 14pt 0 6pt; page-break-after: avoid; }
p { margin: 5pt 0; }
a { color: #0b5cad; text-decoration: none; word-break: break-all; }
code { font-family: "DejaVu Sans Mono", monospace; font-size: 8.8pt; background: #f2f2f2; padding: 0 2px; border-radius: 2px; }
pre { background: #f4f4f4; border: 1px solid #ddd; padding: 6pt 8pt; font-size: 8.3pt; white-space: pre-wrap; word-break: break-all; page-break-inside: avoid; }
pre code { background: none; padding: 0; font-size: inherit; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 8pt; font-size: 8.8pt; page-break-inside: auto; }
th, td { border: 1px solid #bbb; padding: 3pt 5pt; vertical-align: top; text-align: left; }
th { background: #e9eef5; }
tr { page-break-inside: avoid; }
figure { margin: 8pt 0 12pt; page-break-inside: avoid; }
figure img { width: 100%; border: 1px solid #ccc; }
figcaption { font-size: 8.5pt; color: #555; margin-top: 3pt; }
.cover { border: 1px solid #999; padding: 10pt 12pt; margin: 8pt 0 16pt; background: #fafafa; }
.cover .repo { font-size: 12pt; font-weight: bold; }
ul, ol { margin: 4pt 0 4pt 18pt; padding: 0; }
li { margin: 2pt 0; }
blockquote { margin: 4pt 0 4pt 10pt; color: #444; border-left: 3px solid #ccc; padding-left: 8pt; }
"""


def rel_to_github(html_text: str) -> str:
    """Относительные ссылки репозитория -> абсолютные ссылки на GitHub."""
    def repl(m):
        href = m.group(1)
        if href.startswith(("http://", "https://", "#", "mailto:")):
            return m.group(0)
        kind = "tree" if href.endswith("/") else "blob"
        return f'href="{REPO_URL}/{kind}/main/{href}"'
    return re.sub(r'href="([^"]+)"', repl, html_text)


def embed_screenshots(html_text: str, base_dir: str) -> str:
    """После каждого блока (таблица/абзац) со ссылками на screenshots/*.png вставить сами картинки."""
    def repl(m):
        block = m.group(0)
        names = []
        for n in re.findall(r'screenshots/([^"#?]+?\.png)', block):
            if n not in names:
                names.append(n)
        if not names:
            return block
        figs = []
        for n in names:
            path = os.path.join(base_dir, "screenshots", n)
            if not os.path.exists(path):
                continue
            figs.append(f'<figure><img src="file://{html.escape(path)}"><figcaption>{html.escape(n)}</figcaption></figure>')
        return block + "\n" + "\n".join(figs)
    return re.sub(r"<table>.*?</table>|<p>.*?</p>", repl, html_text, flags=re.S)


def build_html(md_path: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(md_path))
    text = open(md_path, encoding="utf-8").read()
    # убрать markdown-оглавление с якорями: в PDF оно бесполезно
    text = re.sub(r"## Содержание\n.*?(?=\nСтруктура репозитория)", "", text, flags=re.S)
    body = markdown.markdown(text, extensions=["tables", "fenced_code", "sane_lists"])
    body = embed_screenshots(body, base_dir)
    body = rel_to_github(body)
    cover = f"""
<div class="cover">
  <div class="repo">Репозиторий с кодом, скриптами и скриншотами: <a href="{REPO_URL}">{REPO_URL}</a></div>
  <div>Отчёт собран {date.today():%d.%m.%Y} из README.md репозитория. Все ссылки в документе ведут на GitHub.</div>
</div>"""
    body = body.replace("</h1>", "</h1>" + cover, 1)
    return f"<!doctype html><html lang='ru'><head><meta charset='utf-8'><title>Отчёт ETL</title><style>{CSS}</style></head><body>{body}</body></html>"


def main(md_path: str, out_pdf: str) -> None:
    html_text = build_html(md_path)
    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
        f.write(html_text)
        tmp = f.name
    cmd = [CHROME, "--headless=new", "--disable-gpu", "--no-sandbox", "--allow-file-access-from-files",
           "--no-pdf-header-footer", f"--print-to-pdf={os.path.abspath(out_pdf)}", f"file://{tmp}"]
    subprocess.run(cmd, check=True, capture_output=True, timeout=180)
    os.unlink(tmp)
    print(f"PDF: {out_pdf} ({os.path.getsize(out_pdf) / 1024 / 1024:.1f} МБ)")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: build_report_pdf.py <README.md> <out.pdf>")
    main(sys.argv[1], sys.argv[2])
