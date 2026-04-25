from __future__ import annotations

import logging
from datetime import datetime, timezone

from huggingface_hub import HfApi

from poly_data.io.parquet_store import ParquetStore

logger = logging.getLogger(__name__)


def push_snapshot(
    store: ParquetStore,
    repo_id: str,
    *,
    sources: list[str] | None = None,
    snapshot_tag: str | None = None,
    private: bool = False,
) -> str:
    api = HfApi()
    api.create_repo(
        repo_id=repo_id, repo_type="dataset", private=private, exist_ok=True
    )

    if sources:
        allow = [f"{s}/**" for s in sources]
    else:
        allow = ["**"]

    tag = snapshot_tag or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    commit_message = f"poly_data snapshot {tag}"

    url = api.upload_large_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=str(store.root),
        allow_patterns=allow,
        commit_message=commit_message,
    )
    logger.info("pushed snapshot to %s", url)
    return url
