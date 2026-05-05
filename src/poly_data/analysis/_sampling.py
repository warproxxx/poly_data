"""Shared RSS sampler. One daemon thread feeds many listeners.

``rss_guard`` and ``Bench`` both want to poll ``psutil.Process().memory_info().rss``
at ~50ms cadence. Without sharing they'd start two daemon threads each making
the syscall — twice the GIL pressure under memory load. This module provides a
reference-counted sampler: the thread starts on first registration and stops on
last deregistration.

Each registrant supplies a callback ``cb(rss_bytes: int) -> None``. Callbacks
fire from the sampler thread; keep them cheap (single comparison + assignment
is the typical use).
"""
from __future__ import annotations

import threading
from typing import Callable

import psutil

_SAMPLE_MS_DEFAULT = 50

ListenerCallback = Callable[[int], None]


class _SharedSampler:
    def __init__(self, sample_ms: int = _SAMPLE_MS_DEFAULT) -> None:
        self.sample_ms = sample_ms
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._listeners: dict[int, ListenerCallback] = {}
        self._next_id = 0

    def register(self, callback: ListenerCallback) -> int:
        with self._lock:
            listener_id = self._next_id
            self._next_id += 1
            self._listeners[listener_id] = callback
            should_start = self._thread is None
            if should_start:
                self._stop.clear()
                self._thread = threading.Thread(
                    target=self._run, daemon=True, name="poly_data-rss-sampler"
                )
                self._thread.start()
        return listener_id

    def deregister(self, listener_id: int) -> None:
        thread_to_join: threading.Thread | None = None
        with self._lock:
            self._listeners.pop(listener_id, None)
            if not self._listeners and self._thread is not None:
                self._stop.set()
                thread_to_join = self._thread
                self._thread = None
        if thread_to_join is not None:
            thread_to_join.join(timeout=1.0)

    def _run(self) -> None:
        proc = psutil.Process()
        while not self._stop.is_set():
            rss = proc.memory_info().rss
            with self._lock:
                listeners = list(self._listeners.values())
            for cb in listeners:
                try:
                    cb(rss)
                except Exception:
                    # Listener failures must not kill the sampler.
                    pass
            self._stop.wait(self.sample_ms / 1000.0)


SAMPLER = _SharedSampler()
