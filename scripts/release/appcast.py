#!/usr/bin/env python3
"""Build Katamaran's Sparkle appcast from its GitHub releases.

Every published release carries a `sparkle.json` asset written by
scripts/release/build-release.sh. The appcast is generated from those alone, so
it never drifts from what is published:

    scripts/release/appcast.py --repo OWNER/REPO --output docs/dist/appcast.xml

Uses the `gh` CLI (GH_TOKEN in CI).
"""

import argparse
import json
import subprocess
import sys
from xml.sax.saxutils import escape, quoteattr

SPARKLE_NS = "http://www.andymatuschak.org/xml-namespaces/sparkle"


def gh(*args):
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def download_meta(repo, tag):
    try:
        meta = json.loads(gh("release", "download", tag, "--repo", repo,
                             "--pattern", "sparkle.json", "--output", "-"))
    except subprocess.CalledProcessError as e:
        print(f"skipping {tag}: no sparkle.json ({e.stderr.strip()})", file=sys.stderr)
        return None
    meta["tag"] = tag
    return meta


def flatten_releases(raw):
    """Accept either a flat release list or a `gh api --paginate --slurp` list of pages."""
    if raw and isinstance(raw[0], list):
        return [r for page in raw for r in page]
    return raw


def fetch_releases(repo):
    """Fetch all GitHub releases across every page."""
    raw = json.loads(
        gh("api", "--paginate", "--slurp", f"repos/{repo}/releases?per_page=100")
    )
    return flatten_releases(raw)


def sparkle_release_tags(releases, limit=None):
    """Tags of published non-draft, non-prerelease releases that carry sparkle.json."""
    tags = [
        r["tag_name"]
        for r in releases
        if not r.get("draft")
        and not r.get("prerelease")
        and any(a.get("name") == "sparkle.json" for a in r.get("assets", []))
    ]
    return tags if limit is None else tags[:limit]


def collect(repo, limit, releases=None):
    """sparkle.json of the newest `limit` non-draft, non-prerelease releases."""
    if releases is None:
        releases = fetch_releases(repo)
    tags = sparkle_release_tags(releases, limit=limit)
    return [m for m in (download_meta(repo, t) for t in tags) if m]


def cdata(text):
    return "<![CDATA[" + text.replace("]]>", "]]]]><![CDATA[>") + "]]>"


def render_item(m):
    title = f"Katamaran {m['short_version']}"
    lines = [
        "    <item>",
        f"      <title>{escape(title)}</title>",
        f"      <pubDate>{escape(m['published'])}</pubDate>",
        f"      <sparkle:version>{escape(str(m['version']))}</sparkle:version>",
        f"      <sparkle:shortVersionString>{escape(m['short_version'])}</sparkle:shortVersionString>",
    ]
    if m.get("channel") and m["channel"] != "release":
        lines.append(f"      <sparkle:channel>{escape(m['channel'])}</sparkle:channel>")
    minimum = m.get("minimum_system_version") or "15.0"
    if minimum.count(".") == 1:
        minimum += ".0"
    lines += [
        f"      <sparkle:minimumSystemVersion>{escape(minimum)}</sparkle:minimumSystemVersion>",
        "      <sparkle:hardwareRequirements>arm64</sparkle:hardwareRequirements>",
    ]
    notes = m.get("notes") or ""
    if not notes.strip() and m.get("commit"):
        notes = f"Built from `{m['commit'][:7]}`."
    if notes.strip():
        lines.append(f'      <description sparkle:format="markdown">{cdata(notes)}</description>')
    lines += [
        "      <enclosure url={} length={} type=\"application/octet-stream\" sparkle:edSignature={}/>".format(
            quoteattr(m["url"]), quoteattr(str(m["length"])), quoteattr(m["ed_signature"])
        ),
        "    </item>",
    ]
    return "\n".join(lines)


def render(items, title="Katamaran"):
    items = sorted(items, key=lambda m: int(m["version"]), reverse=True)
    body = "\n".join(render_item(m) for m in items)
    return (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        f'<rss version="2.0" xmlns:sparkle="{SPARKLE_NS}">\n'
        "  <channel>\n"
        f"    <title>{escape(title)}</title>\n"
        f"{body}\n"
        "  </channel>\n"
        "</rss>\n"
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--limit", type=int, default=10, help="newest releases to list")
    ap.add_argument(
        "--releases-json",
        help="path to pre-fetched GitHub releases JSON (flat or `gh api --paginate --slurp`)",
    )
    args = ap.parse_args()
    releases = None
    if args.releases_json:
        with open(args.releases_json, encoding="utf-8") as f:
            releases = flatten_releases(json.load(f))
    items = collect(args.repo, args.limit, releases=releases)
    if not items:
        sys.exit("no release carries a sparkle.json; refusing to publish an empty appcast")
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(render(items))
    for m in sorted(items, key=lambda m: int(m["version"]), reverse=True):
        print(f"{m.get('channel', 'release'):8} {m['version']:>6} {m['short_version']}  ({m['tag']})")


if __name__ == "__main__":
    main()
