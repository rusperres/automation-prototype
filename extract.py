import pdfplumber


def extract(file):
    """
    Extract all elements from a PDF in document order.
    Returns a flat list of elements, each being either a text block or table.

    Text block element:
    {
        "type": "text",
        "page": int,
        "bbox": (x0, y0, x1, y1),
        "content": str,
        "font": {
            "name": str | None,
            "size": float | None,
            "bold": bool
        }
    }

    Table element:
    {
        "type": "table",
        "page": int,
        "bbox": (x0, y0, x1, y1),
        "content": list[list[str | None]]   # rows x cols
    }
    """
    raw = []

    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            page_num = page.page_number

            # --- Find table bounding boxes so we can exclude their area from text ---
            tables = page.find_tables()
            table_bboxes = [t.bbox for t in tables]

            # --- Extract tables ---
            for table in tables:
                raw.append({
                    "type": "table",
                    "page": page_num,
                    "bbox": table.bbox,
                    "content": table.extract()
                })

            # --- Extract text lines, skipping anything inside a table bbox ---
            for line in page.extract_text_lines(return_chars=True):
                lx0 = line["x0"]
                ly0 = line["top"]
                lx1 = line["x1"]
                ly1 = line["bottom"]

                # Skip if this line falls inside any table
                if _inside_any(lx0, ly0, lx1, ly1, table_bboxes):
                    continue

                # Derive font metadata from the line's chars
                chars = line.get("chars", [])
                font_name, font_size, bold = _font_meta(chars)

                raw.append({
                    "type": "text",
                    "page": page_num,
                    "bbox": (lx0, ly0, lx1, ly1),
                    "content": line["text"],
                    "font": {
                        "name": font_name,
                        "size": font_size,
                        "bold": bold
                    }
                })

    # Sort everything by page then by vertical position (top of element)
    raw.sort(key=lambda el: (el["page"], el["bbox"][1]))

    return raw


# ── helpers ──────────────────────────────────────────────────────────────────

def _inside_any(x0, y0, x1, y1, bboxes, tolerance=2):
    """Return True if the given bbox is substantially inside any of the bboxes."""
    for bx0, by0, bx1, by1 in bboxes:
        if (x0 >= bx0 - tolerance and y0 >= by0 - tolerance and
                x1 <= bx1 + tolerance and y1 <= by1 + tolerance):
            return True
    return False


def _font_meta(chars):
    """
    Derive representative font metadata from a list of char dicts.
    Uses the most common fontname and the modal size across all chars.
    Bold detection: checks if 'Bold' or 'bold' appears in the fontname.
    """
    if not chars:
        return None, None, False

    names = [c.get("fontname") for c in chars if c.get("fontname")]
    sizes = [c.get("size") for c in chars if c.get("size")]

    font_name = max(set(names), key=names.count) if names else None
    font_size = round(max(set(sizes), key=sizes.count), 2) if sizes else None
    bold = "Bold" in (font_name or "") or "bold" in (font_name or "")

    return font_name, font_size, bold


# ── quick print test ──────────────────────────────────────────────────────────

def _fmt_table(rows, max_rows=4):
    if not rows:
        return "  (empty table)"
    col_widths = []
    for row in rows[:max_rows]:
        for i, cell in enumerate(row):
            w = len(str(cell) if cell is not None else "")
            if i >= len(col_widths):
                col_widths.append(w)
            else:
                col_widths[i] = max(col_widths[i], w)

    lines = []
    for row in rows[:max_rows]:
        parts = []
        for i, cell in enumerate(row):
            val = str(cell) if cell is not None else ""
            width = col_widths[i] if i < len(col_widths) else len(val)
            parts.append(val.ljust(width))
        lines.append("  " + "   ".join(parts))
    if len(rows) > max_rows:
        lines.append(f"  ... ({len(rows) - max_rows} more rows)")
    return "\n".join(lines)

def write_txt(elements, output_path):
    """
    Write extracted elements to a plain text file for inspection.
    Text printed as-is. Tables printed with column spacing, no borders.
    """
    with open(output_path, "w", encoding="utf-8") as f:
        for el in elements:
            if el["type"] == "text":
                f.write(el["content"] + "\n")
            else:
                rows = el["content"]
                if not rows:
                    continue
 
                col_widths = []
                for row in rows:
                    for i, cell in enumerate(row):
                        w = len(str(cell) if cell is not None else "")
                        if i >= len(col_widths):
                            col_widths.append(w)
                        else:
                            col_widths[i] = max(col_widths[i], w)
 
                for row in rows:
                    parts = []
                    for i, cell in enumerate(row):
                        val = str(cell) if cell is not None else ""
                        width = col_widths[i] if i < len(col_widths) else len(val)
                        parts.append(val.ljust(width))
                    f.write("  ".join(parts).rstrip() + "\n")
 
                f.write("\n")

if __name__ == "__main__":
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "ATL0347N25 Contract.pdf"
    elements = extract(path)

    print(f"Total elements extracted: {len(elements)}\n")
    print("=" * 70)

    for el in elements:
        if el["type"] == "text":
            font = el["font"]
            meta = f"  [font: {font['name']} | size: {font['size']} | bold: {font['bold']}]"
            print(f"[TEXT]  page={el['page']}  bbox={tuple(round(v,1) for v in el['bbox'])}")
            print(f"  {el['content']}")
            print(meta)
        else:
            rows = el["content"]
            print(f"[TABLE] page={el['page']}  bbox={tuple(round(v,1) for v in el['bbox'])}  ({len(rows)} rows x {len(rows[0]) if rows else 0} cols)")
            print(_fmt_table(rows))
        print()

    write_txt(elements, "output.txt")
    print("Written to output.txt")
