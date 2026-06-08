#!/usr/bin/env python3
"""Generate docs/project_report.pdf from docs/PROJECT_REPORT.md.

This is intentionally dependency-light: it uses only ReportLab.
Markdown is rendered in a simple way (headings/bullets/paragraphs).
"""

from __future__ import annotations

from pathlib import Path

from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (  # type: ignore
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    ListFlowable,
    ListItem,
)


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _strip_inline_md(text: str) -> str:
    # Minimal inline cleanup: code ticks and emphasis markers.
    return (
        text.replace("`", "")
        .replace("**", "")
        .replace("*", "")
    )


def md_to_flowables(md_text: str):
    styles = getSampleStyleSheet()

    h1 = ParagraphStyle("H1", parent=styles["Heading1"], spaceAfter=12)
    h2 = ParagraphStyle("H2", parent=styles["Heading2"], spaceBefore=10, spaceAfter=8)
    h3 = ParagraphStyle("H3", parent=styles["Heading3"], spaceBefore=8, spaceAfter=6)
    body = ParagraphStyle("Body", parent=styles["BodyText"], leading=14, spaceAfter=6)
    mono = ParagraphStyle(
        "Mono",
        parent=styles["BodyText"],
        fontName="Courier",
        fontSize=9,
        leading=11,
        spaceAfter=4,
    )

    flow = []

    lines = md_text.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i].rstrip("\n")
        line = raw.strip()

        # Horizontal rule
        if line == "---":
            flow.append(Spacer(1, 10))
            i += 1
            continue

        # Headings
        if line.startswith("# "):
            title = _strip_inline_md(line[2:].strip())
            flow.append(Paragraph(_escape(title), h1))
            i += 1
            continue
        if line.startswith("## "):
            title = _strip_inline_md(line[3:].strip())
            flow.append(Paragraph(_escape(title), h2))
            i += 1
            continue
        if line.startswith("### "):
            title = _strip_inline_md(line[4:].strip())
            flow.append(Paragraph(_escape(title), h3))
            i += 1
            continue

        # Code blocks (```)
        if line.startswith("```"):
            block_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                block_lines.append(lines[i].rstrip("\n"))
                i += 1
            # Skip closing fence if present
            if i < len(lines) and lines[i].strip().startswith("```"):
                i += 1
            if block_lines:
                for bl in block_lines:
                    flow.append(Paragraph(_escape(bl).replace(" ", "&nbsp;"), mono))
                flow.append(Spacer(1, 6))
            continue

        # Bullets
        if line.startswith("- "):
            items = []
            while i < len(lines) and lines[i].strip().startswith("- "):
                item_text = _strip_inline_md(lines[i].strip()[2:].strip())
                items.append(ListItem(Paragraph(_escape(item_text), body)))
                i += 1
            flow.append(ListFlowable(items, bulletType="bullet", leftIndent=18))
            flow.append(Spacer(1, 6))
            continue

        # Blank line
        if not line:
            i += 1
            continue

        # Paragraph (coalesce until blank line)
        para = [line]
        i += 1
        while i < len(lines) and lines[i].strip():
            para.append(lines[i].strip())
            i += 1

        text = _strip_inline_md(" ".join(para))
        flow.append(Paragraph(_escape(text), body))

    return flow


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    md_path = root / "docs" / "PROJECT_REPORT.md"
    pdf_path = root / "docs" / "project_report.pdf"

    md_text = md_path.read_text(encoding="utf-8")

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=LETTER,
        leftMargin=0.85 * inch,
        rightMargin=0.85 * inch,
        topMargin=0.85 * inch,
        bottomMargin=0.85 * inch,
        title="Pipeline V3 — Project Report",
        author="pipeline-v3",
    )

    flowables = md_to_flowables(md_text)
    doc.build(flowables)

    print(f"Wrote: {pdf_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
