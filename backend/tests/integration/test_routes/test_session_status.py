"""Tests for GET /session/status, focused on the job-queue lookup.

The status endpoint has to look up each requested folder in the rq queues and
registries. These tests check the statuses it reports for queued jobs, and that
the cost of reading the queues does not grow with the number of folders.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlencode

import pytest
from fakeredis import FakeStrictRedis
from rq import Queue
from rq.job import Job

from beets_flask.importer.progress import FolderStatus
from tests.mixins.database import IsolatedDBMixin

if TYPE_CHECKING:
    from quart.typing import TestClientProtocol as Client


def noop():
    """Job function; the jobs in these tests are never run."""


@pytest.fixture
def async_queues(monkeypatch):
    """Real (non-eager) queues on fakeredis, so enqueued jobs stay queued."""
    conn = FakeStrictRedis()
    preview = Queue("preview", is_async=True, connection=conn)
    imp = Queue("import", is_async=True, connection=conn)
    monkeypatch.setattr("beets_flask.redis.redis_conn", conn)
    monkeypatch.setattr("beets_flask.redis.queues", [preview, imp])
    return preview, imp


def enqueue_for(queue: Queue, folder_hash: str, job_kind: str) -> Job:
    job = queue.enqueue(noop)
    job.meta["folder_hash"] = folder_hash
    job.meta["folder_path"] = f"/inbox/{folder_hash}"
    job.meta["job_kind"] = job_kind
    job.save_meta()
    return job


def status_url(hashes: list[str]) -> str:
    query = urlencode(
        [
            (k, v)
            for h in hashes
            for k, v in (("folder_hash", h), ("folder_path", f"/inbox/{h}"))
        ]
    )
    return f"/api_v1/session/status?{query}"


class TestSessionStatusQueues(IsolatedDBMixin):
    @pytest.mark.asyncio
    async def test_queued_job_reports_pending(self, client: Client, async_queues):
        preview, _ = async_queues
        enqueue_for(preview, "queued_hash", "preview")

        response = await client.get(status_url(["queued_hash", "unseen_hash"]))
        assert response.status_code == 200
        data = {d["hash"]: d["status"] for d in await response.get_json()}

        assert data["queued_hash"] == FolderStatus.PENDING
        assert data["unseen_hash"] == FolderStatus.UNKNOWN

    @pytest.mark.asyncio
    async def test_queue_reads_do_not_scale_with_folders(
        self, client: Client, async_queues, monkeypatch
    ):
        """The queues are read once per request, not once per folder.

        Before, every folder re-fetched every job of every queue/registry, which
        made the endpoint O(folders x jobs). With a few thousand folders and a
        backlog of jobs a single request took minutes and blocked the worker.
        """
        preview, imp = async_queues
        for i in range(5):
            enqueue_for(preview, f"h{i}", "preview")
        enqueue_for(imp, "h0", "import_auto")

        calls = {"n": 0}
        original = Job.fetch_many

        def counting_fetch_many(*args, **kwargs):
            calls["n"] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(Job, "fetch_many", staticmethod(counting_fetch_many))

        async def fetches_for(n_folders: int) -> int:
            calls["n"] = 0
            hashes = [f"h{i}" for i in range(n_folders)]
            response = await client.get(status_url(hashes))
            assert response.status_code == 200
            assert len(await response.get_json()) == n_folders
            return calls["n"]

        assert await fetches_for(1) == await fetches_for(50)
