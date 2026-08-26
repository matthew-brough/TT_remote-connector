from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import cripy
from cripy import Client

try:
    import ujson as json
except ImportError:
    import json

from ..enums.subscriptions import EventSubscriptions
from ..types.iterable_queue import Queue
from ..types.position import Position

if TYPE_CHECKING:
    from asyncio import Event

    from aiohttp import ClientSession


class CDPConnectionError(RuntimeError):
    """Raised when a FiveM NUI target cannot be used as a connector endpoint."""


class CDPClient:
    _CONTEXT_SETTLE_SECONDS = 5.0
    _RECONNECT_DELAY_SECONDS = 1.0

    def __init__(self, client_session: ClientSession, running: Event, resource_name: str) -> None:
        self.logger = logging.getLogger(__name__)
        self.session = client_session
        self.running = running
        self.resource_name = resource_name
        self.client: Client | None = None
        self.context_id: int | None = None
        self.context_ids_by_frame: dict[str, int] = {}
        self.nodes: Queue[Position] = Queue()

    async def get_ws_url(self) -> str:
        """Find the CDP target belonging to the configured FiveM NUI resource."""
        try:
            async with self.session.get("http://127.0.0.1:13172/json/list") as response:
                response.raise_for_status()
                data: object = await response.json()
        except Exception as error:
            raise CDPConnectionError("FiveM CDP endpoint is unavailable at http://127.0.0.1:13172") from error

        if not isinstance(data, list):
            raise CDPConnectionError("FiveM CDP returned an invalid target list")

        game_root_websocket_url: str | None = None
        for target in data:
            if not isinstance(target, Mapping):
                continue

            target_url = target.get("url")
            websocket_url = target.get("webSocketDebuggerUrl")
            if not isinstance(target_url, str) or not isinstance(websocket_url, str):
                continue
            if self._resource_from_target_url(target_url) == self.resource_name:
                self.logger.info("Found CDP target for resource %s", self.resource_name)
                return websocket_url

            if target_url == "nui://game/ui/root.html":
                game_root_websocket_url = websocket_url

        if game_root_websocket_url is not None:
            self.logger.info("Using the game root CDP target to find resource %s", self.resource_name)
            return game_root_websocket_url

        raise CDPConnectionError(f"FiveM NUI resource {self.resource_name!r} is not an inspectable CDP target")

    @staticmethod
    def _resource_from_target_url(target_url: str) -> str | None:
        parsed = urlparse(target_url)
        if parsed.scheme == "nui":
            return parsed.netloc
        if parsed.scheme == "https" and parsed.netloc.startswith("cfx-nui-"):
            return parsed.netloc.removeprefix("cfx-nui-")
        return None

    async def enter(self, subscription: EventSubscriptions | Iterable[EventSubscriptions]) -> None:
        """Connect once initially, then reconnect while a previously live target recovers."""
        had_live_connection = False
        while self.running.is_set():
            connection_established = asyncio.Event()
            try:
                await self._connect_once(subscription, connection_established)
            except CDPConnectionError as error:
                had_live_connection = had_live_connection or connection_established.is_set()
                if not had_live_connection:
                    self.logger.error("Connector startup aborted: %s", error)
                    return
                self.logger.warning("FiveM CDP connection lost: %s", error)
            finally:
                await self.close()

            if self.running.is_set():
                await asyncio.sleep(self._RECONNECT_DELAY_SECONDS)

    async def _connect_once(
        self,
        subscription: EventSubscriptions | Iterable[EventSubscriptions],
        connection_established: Event,
    ) -> None:
        websocket_url = await self.get_ws_url()
        self.context_id = None
        self.context_ids_by_frame.clear()

        try:
            client_connection = await cripy.connect(websocket_url, remote=False, flatten_sessions=True)
            if not isinstance(client_connection, Client):
                await client_connection.dispose()
                raise CDPConnectionError("CDP client did not expose the static protocol interface")
            client = client_connection
            self.client = client
            client.Runtime.bindingCalled(self.on_binding_called)
            client.Runtime.executionContextCreated(self._on_execution_context_created)
            await client.Page.enable()
            await client.Runtime.enable()
            await asyncio.sleep(self._CONTEXT_SETTLE_SECONDS)

            self.context_id = await self._resolve_context()
            context_id = self._require_context_id()
            capability = await client.Runtime.evaluate(
                expression="typeof globalThis.recvData === 'function'",
                contextId=context_id,
                returnByValue=True,
            )
            if not self._is_recv_data_available(capability):
                raise CDPConnectionError(
                    f"FiveM NUI resource {self.resource_name!r} does not expose the legacy recvData bridge"
                )

            await client.Runtime.addBinding("sendDevtools", context_id)
            for event in self._subscriptions(subscription):
                await self.send_app_command(event)
            connection_established.set()
        except CDPConnectionError:
            raise
        except Exception as error:
            raise CDPConnectionError("Failed to establish a FiveM CDP session") from error

        while self.running.is_set() and self.client is not None and not self.client.closed:
            await asyncio.sleep(1)

        if self.client is None or self.client.closed:
            raise CDPConnectionError("FiveM closed the CDP target")

    @staticmethod
    def _subscriptions(subscription: EventSubscriptions | Iterable[EventSubscriptions]) -> Iterable[EventSubscriptions]:
        if isinstance(subscription, EventSubscriptions):
            return (subscription,)
        return subscription

    @staticmethod
    def _is_recv_data_available(result: object) -> bool:
        if not isinstance(result, Mapping):
            return False
        remote_result = result.get("result")
        return isinstance(remote_result, Mapping) and remote_result.get("value") is True

    def _on_execution_context_created(self, payload: object) -> None:
        if not isinstance(payload, Mapping):
            return
        context = payload.get("context")
        if not isinstance(context, Mapping):
            return
        context_id = context.get("id")
        auxiliary_data = context.get("auxData")
        if not isinstance(context_id, int):
            return
        if not isinstance(auxiliary_data, Mapping):
            return
        frame_id = auxiliary_data.get("frameId")
        if not isinstance(frame_id, str):
            return

        self.context_ids_by_frame[frame_id] = context_id

    async def _resolve_context(self) -> int | None:
        if self.client is None:
            raise CDPConnectionError("Cannot resolve a context without a CDP client")

        frame_tree_response: object = await self.client.Page.getFrameTree()
        for frame in self._frames_from_tree(frame_tree_response):
            frame_id = frame.get("id")
            frame_url = frame.get("url")
            if (
                isinstance(frame_id, str)
                and isinstance(frame_url, str)
                and self._resource_from_target_url(frame_url) == self.resource_name
                and (context_id := self.context_ids_by_frame.get(frame_id)) is not None
            ):
                return context_id

        for context_id in self.context_ids_by_frame.values():
            capability = await self.client.Runtime.evaluate(
                expression="typeof globalThis.recvData === 'function'",
                contextId=context_id,
                returnByValue=True,
            )
            if self._is_recv_data_available(capability):
                return context_id

        return None

    @classmethod
    def _frames_from_tree(cls, response: object) -> Iterable[Mapping[str, object]]:
        if not isinstance(response, Mapping):
            return ()
        frame_tree = response.get("frameTree")
        if not isinstance(frame_tree, Mapping):
            return ()
        return cls._walk_frame_tree(frame_tree)

    @classmethod
    def _walk_frame_tree(cls, frame_tree: Mapping[str, object]) -> Iterable[Mapping[str, object]]:
        frame = frame_tree.get("frame")
        if isinstance(frame, Mapping):
            yield frame
        child_frames = frame_tree.get("childFrames")
        if not isinstance(child_frames, list):
            return
        for child_frame in child_frames:
            if isinstance(child_frame, Mapping):
                yield from cls._walk_frame_tree(child_frame)

    def _require_context_id(self) -> int:
        if self.context_id is None:
            raise CDPConnectionError("FiveM did not provide a JavaScript execution context")
        return self.context_id

    async def close(self) -> None:
        if self.client is not None:
            await self.client.dispose()
            self.client = None
        self.context_id = None
        self.context_ids_by_frame.clear()

    async def on_binding_called(self, payload: Mapping[str, object]) -> None:
        encoded_payload = payload.get("payload")
        if not isinstance(encoded_payload, str):
            self.logger.warning("Ignoring malformed sendDevtools payload")
            return

        decoded_payload: object = json.loads(encoded_payload)
        if not isinstance(decoded_payload, Mapping) or decoded_payload.get("type") != "position":
            return
        position_data = decoded_payload.get("data")
        if not (
            isinstance(position_data, list)
            and len(position_data) == 4
            and all(isinstance(value, int | float) for value in position_data)
        ):
            self.logger.warning("Ignoring malformed position payload")
            return

        position = Position(*position_data)
        self.nodes.put_nowait(position)
        self.logger.info("Position: %s", position_data)

    async def send_app_command(self, event: EventSubscriptions) -> object:
        if self.client is None:
            raise CDPConnectionError("Cannot send a command without a CDP client")
        return await self.client.Runtime.evaluate(
            expression=f"recvData({event.value!r})",
            contextId=self._require_context_id(),
            awaitPromise=True,
        )
