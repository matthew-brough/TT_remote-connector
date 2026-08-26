from __future__ import annotations

import queue
from collections.abc import Generator


class Queue[T](queue.Queue[T]):
    def __enter__(self, block: bool = True, timeout: float | None = None) -> T:
        return self.get(block, timeout)

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.task_done()

    def __iter__(self) -> Generator[T]:
        while not self.empty():
            with self as node:
                yield node
