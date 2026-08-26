from __future__ import annotations

import asyncio
import unittest
from collections.abc import Iterable

import aiohttp

from remote_connector.CDP.remote_connector import CDPClient, CDPConnectionError
from remote_connector.enums.subscriptions import EventSubscriptions


class InitialFailureClient(CDPClient):
    async def _connect_once(
        self,
        subscription: EventSubscriptions | Iterable[EventSubscriptions],
        connection_established: asyncio.Event,
    ) -> None:
        raise CDPConnectionError("target is unavailable")


class RecoveringClient(CDPClient):
    def __init__(self, session: aiohttp.ClientSession, running: asyncio.Event) -> None:
        super().__init__(session, running, "transport-tycoon")
        self.attempts = 0
        self._RECONNECT_DELAY_SECONDS = 0.0

    async def _connect_once(
        self,
        subscription: EventSubscriptions | Iterable[EventSubscriptions],
        connection_established: asyncio.Event,
    ) -> None:
        self.attempts += 1
        if self.attempts == 1:
            connection_established.set()
            raise CDPConnectionError("target disconnected")
        self.running.clear()


class CDPClientTests(unittest.IsolatedAsyncioTestCase):
    def test_resource_from_target_url(self) -> None:
        self.assertEqual(CDPClient._resource_from_target_url("nui://transport-tycoon/main.html"), "transport-tycoon")
        self.assertEqual(
            CDPClient._resource_from_target_url("https://cfx-nui-transport-tycoon/main.html"),
            "transport-tycoon",
        )
        self.assertIsNone(CDPClient._resource_from_target_url("https://example.com/main.html"))

    def test_recv_data_capability(self) -> None:
        self.assertTrue(CDPClient._is_recv_data_available({"result": {"value": True}}))
        self.assertFalse(CDPClient._is_recv_data_available({"result": {"value": False}}))
        self.assertFalse(CDPClient._is_recv_data_available({}))

    def test_frames_from_tree_walks_resource_iframes(self) -> None:
        frames = tuple(
            CDPClient._frames_from_tree(
                {
                    "frameTree": {
                        "frame": {"id": "root", "url": "nui://game/ui/root.html"},
                        "childFrames": [
                            {
                                "frame": {"id": "resource", "url": "nui://transport-tycoon/nui/index.html"},
                                "childFrames": [],
                            }
                        ],
                    }
                }
            )
        )

        self.assertEqual(
            frames,
            (
                {"id": "root", "url": "nui://game/ui/root.html"},
                {"id": "resource", "url": "nui://transport-tycoon/nui/index.html"},
            ),
        )

    async def test_initial_failure_stops_without_retrying(self) -> None:
        running = asyncio.Event()
        running.set()
        async with aiohttp.ClientSession() as session:
            client = InitialFailureClient(session, running, "transport-tycoon")
            await client.enter(EventSubscriptions.enable_position)

        self.assertTrue(running.is_set())

    async def test_established_connection_retries_after_disconnect(self) -> None:
        running = asyncio.Event()
        running.set()
        async with aiohttp.ClientSession() as session:
            client = RecoveringClient(session, running)
            await client.enter(EventSubscriptions.enable_position)

            self.assertEqual(client.attempts, 2)
