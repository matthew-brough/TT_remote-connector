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
    _CONTEXT_TIMEOUT_SECONDS = 5.0
    _RECONNECT_DELAY_SECONDS = 1.0

    def __init__(self, client_session: ClientSession, running: Event, resource_name: str) -> None:
        self.logger = logging.getLogger(__name__)
        self.session = client_session
        self.running = running
        self.resource_name = resource_name
        self.client: Client | None = None
        self.context_id: int | None = None
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
        context_ready = asyncio.Event()

        try:
            client_connection = await cripy.connect(websocket_url, remote=False, flatten_sessions=True)
            if not isinstance(client_connection, Client):
                await client_connection.dispose()
                raise CDPConnectionError("CDP client did not expose the static protocol interface")
            client = client_connection
            self.client = client
            client.Runtime.bindingCalled(self.on_binding_called)
            client.Runtime.executionContextCreated(
                lambda payload: self._on_execution_context_created(payload, context_ready)
            )
            await client.Runtime.enable()
            await asyncio.wait_for(context_ready.wait(), timeout=self._CONTEXT_TIMEOUT_SECONDS)

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

    def _on_execution_context_created(self, payload: object, context_ready: Event) -> None:
        if not isinstance(payload, Mapping):
            return
        context = payload.get("context")
        if not isinstance(context, Mapping):
            return
        context_id = context.get("id")
        auxiliary_data = context.get("auxData")
        if not isinstance(context_id, int):
            return
        if isinstance(auxiliary_data, Mapping) and auxiliary_data.get("isDefault") is False:
            return

        self.context_id = context_id
        context_ready.set()

    def _require_context_id(self) -> int:
        if self.context_id is None:
            raise CDPConnectionError("FiveM did not provide a JavaScript execution context")
        return self.context_id

    async def close(self) -> None:
        if self.client is not None:
            await self.client.dispose()
            self.client = None
        self.context_id = None

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
