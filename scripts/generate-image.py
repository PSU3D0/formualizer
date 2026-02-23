#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["openai>=1.76.0"]
# ///
"""
Generate images with OpenAI image models and save them to disk.

Examples:
  uv run scripts/generate-image.py \
    --prompt "A dark technical illustration of a computation graph" \
    --name hero

  cat prompt.txt | uv run scripts/generate-image.py --name hero-v2

  uv run scripts/generate-image.py \
    --prompt-file docs/prompts/hero.txt \
    -n 3 --size 1536x1024 --quality auto --model gpt-image-1.5
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import OpenAI


DEFAULT_MODEL = "gpt-image-1.5"
DEFAULT_N = 1
DEFAULT_SIZE = "1536x1024"
DEFAULT_QUALITY = "auto"
DEFAULT_OUT_DIR = "docs-site/public/home/generated"


@dataclass
class SavedImage:
    index: int
    path: str
    source: str
    revised_prompt: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate images with OpenAI and write them to files.",
        allow_abbrev=False,
    )
    prompt_group = parser.add_mutually_exclusive_group()
    prompt_group.add_argument("--prompt", help="Prompt text. If omitted, reads from stdin.")
    prompt_group.add_argument(
        "--prompt-file",
        type=Path,
        help="Path to a file containing prompt text.",
    )

    parser.add_argument(
        "-n",
        "--n",
        "--count",
        dest="n",
        type=int,
        default=DEFAULT_N,
        help=f"Number of images (default: {DEFAULT_N}).",
    )
    parser.add_argument(
        "--size",
        default=DEFAULT_SIZE,
        help=f"Image size, e.g. 1024x1024 or 1536x1024 (default: {DEFAULT_SIZE}).",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"OpenAI image model (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--quality",
        default=DEFAULT_QUALITY,
        help=f"Quality setting sent to API (default: {DEFAULT_QUALITY}).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(DEFAULT_OUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUT_DIR}).",
    )
    parser.add_argument(
        "--name",
        default="image",
        help="Base filename prefix (default: image).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Optional API key override. If omitted, uses OPENAI_API_KEY from environment.",
    )
    parser.add_argument(
        "--json-manifest",
        type=Path,
        default=None,
        help="Optional path to write run metadata JSON. Defaults to <out-dir>/<timestamp>-<name>.json.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved inputs without calling API.",
    )

    return parser.parse_args()


def read_prompt(args: argparse.Namespace) -> str:
    if args.prompt is not None:
        return args.prompt.strip()

    if args.prompt_file is not None:
        return args.prompt_file.read_text(encoding="utf-8").strip()

    if sys.stdin.isatty():
        raise SystemExit(
            "No prompt provided. Use --prompt, --prompt-file, or pipe prompt text on stdin."
        )

    return sys.stdin.read().strip()


def sanitize_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", name).strip("-._")
    return cleaned or "image"


def get_api_key(cli_key: str | None) -> str:
    key = cli_key or os.environ.get("OPENAI_API_KEY")
    if not key:
        raise SystemExit(
            "Missing API key. Set OPENAI_API_KEY or pass --api-key <key>."
        )
    return key


def _item_to_dict(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return item

    result: dict[str, Any] = {}
    for field in ("b64_json", "url", "revised_prompt"):
        value = getattr(item, field, None)
        if value is not None:
            result[field] = value
    return result


def _download_url_bytes(url: str) -> tuple[bytes, str]:
    with urllib.request.urlopen(url) as resp:  # nosec B310 - trusted API URL from OpenAI
        content_type = resp.headers.get("Content-Type", "")
        data = resp.read()

    ext = ".png"
    if "jpeg" in content_type or "jpg" in content_type:
        ext = ".jpg"
    elif "webp" in content_type:
        ext = ".webp"
    elif "png" in content_type:
        ext = ".png"

    return data, ext


def save_images(
    response: Any,
    out_dir: Path,
    run_prefix: str,
) -> list[SavedImage]:
    data = getattr(response, "data", None)
    if data is None and isinstance(response, dict):
        data = response.get("data")

    if not data:
        raise RuntimeError("No image data returned from API response.")

    out_dir.mkdir(parents=True, exist_ok=True)
    saved: list[SavedImage] = []

    for idx, item in enumerate(data, start=1):
        obj = _item_to_dict(item)
        revised_prompt = obj.get("revised_prompt")

        if obj.get("b64_json"):
            image_bytes = base64.b64decode(obj["b64_json"])
            ext = ".png"
            source = "b64_json"
        elif obj.get("url"):
            image_bytes, ext = _download_url_bytes(obj["url"])
            source = "url"
        else:
            raise RuntimeError(f"Image item {idx} missing both b64_json and url fields.")

        filename = f"{run_prefix}-{idx:02d}{ext}"
        path = out_dir / filename
        path.write_bytes(image_bytes)

        saved.append(
            SavedImage(
                index=idx,
                path=str(path),
                source=source,
                revised_prompt=revised_prompt,
            )
        )

    return saved


def main() -> int:
    args = parse_args()
    prompt = read_prompt(args)
    if not prompt:
        raise SystemExit("Prompt is empty.")

    if args.n < 1:
        raise SystemExit("-n must be >= 1")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    name = sanitize_name(args.name)
    run_prefix = f"{timestamp}-{name}"

    print("Image generation request")
    print(f"  model:   {args.model}")
    print(f"  n:       {args.n}")
    print(f"  size:    {args.size}")
    print(f"  quality: {args.quality}")
    print(f"  out-dir: {args.out_dir}")
    print(f"  name:    {name}")

    if args.dry_run:
        print("\n[dry-run] Prompt preview:\n")
        print(prompt)
        return 0

    api_key = get_api_key(args.api_key)
    client = OpenAI(api_key=api_key)

    response = client.images.generate(
        model=args.model,
        prompt=prompt,
        n=args.n,
        size=args.size,
        quality=args.quality,
    )

    saved = save_images(response, args.out_dir, run_prefix)

    manifest_path = args.json_manifest
    if manifest_path is None:
        manifest_path = args.out_dir / f"{run_prefix}.json"

    manifest = {
        "timestamp": timestamp,
        "model": args.model,
        "n": args.n,
        "size": args.size,
        "quality": args.quality,
        "prompt": prompt,
        "images": [
            {
                "index": image.index,
                "path": image.path,
                "source": image.source,
                "revised_prompt": image.revised_prompt,
            }
            for image in saved
        ],
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("\nSaved images:")
    for image in saved:
        print(f"  - {image.path}")
    print(f"Manifest:\n  - {manifest_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
