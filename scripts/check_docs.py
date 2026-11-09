"""Check repository Markdown file links, image paths, and heading anchors (offline)."""

from pathlib import Path
import re
import subprocess
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"!?\[[^\]]*\]\(([^\s)]+)(?:\s+\"[^\"]*\")?\)")


def headings(path):
    counts, anchors = {}, set()
    text = re.sub(r"```.*?```", "", path.read_text(), flags=re.S)
    for line in text.splitlines():
        if not line.startswith("#"):
            continue
        label = re.sub(r"^#+\s+|\s+#+$", "", line).lower()
        slug = re.sub(r"[^\w\- ]", "", label).replace(" ", "-")
        number = counts.get(slug, 0)
        counts[slug] = number + 1
        anchors.add(f"{slug}-{number}" if number else slug)
    return anchors


def main():
    files = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*.md"],
        cwd=ROOT, text=True,
    ).splitlines()
    failures, checked = [], 0
    for name in sorted(set(files)):
        path = ROOT / name
        if not path.exists():  # Pending deletions are absent from the working tree.
            continue
        content = re.sub(r"```.*?```", "", path.read_text(), flags=re.S)
        for match in LINK.finditer(content):
            url = urlsplit(match.group(1))
            if url.scheme or url.netloc:
                continue  # External sources are reviewed separately, not fetched by this check.
            checked += 1
            target = (path.parent / unquote(url.path)).resolve() if url.path else path
            if not target.is_relative_to(ROOT) or not target.exists():
                failures.append(f"{name}: missing file {match.group(1)}")
            elif url.fragment and target.suffix == ".md" and unquote(url.fragment) not in headings(target):
                failures.append(f"{name}: missing heading {match.group(1)}")
    if failures:
        raise SystemExit("\n".join(failures))
    print(f"Checked {checked} local Markdown links, images, and anchors.")


if __name__ == "__main__":
    main()
