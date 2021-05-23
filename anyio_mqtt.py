"""
AnyIO wrapper around Paho MQTT client.
"""

import enum
import logging
import math
import socket
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union, cast

import anyio
import anyio.abc
import paho.mqtt.client as paho  # type: ignore

if TYPE_CHECKING:
    from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream

_LOG = logging.getLogger(__name__)


class State(enum.Enum):
    """
    Finite states of AnyIOMQTTClient.
    """

    INITIAL = enum.auto()
    DISCONNECTED = enum.auto()
    CONNECTING = enum.auto()
    CONNECTED = enum.auto()


class AnyIOMQTTClient:
    """
    AnyIO wrapper around Paho MQTT client.
    """

    # pylint: disable=unused-argument

    def __init__(
        self, task_group: anyio.abc.TaskGroup, config: Optional[Dict[str, Any]] = None
    ) -> None:
        if config is None:
            config = {}

        self._task_group = task_group
        self._sock: Optional[socket.socket] = None

        self._client: paho.Client = paho.Client(**config)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_socket_open = self._on_socket_open
        self._client.on_socket_close = self._on_socket_close
        self._client.on_socket_register_write = self._on_socket_register_write

        self._state: State = State.INITIAL
        self._internal_state_tx: "MemoryObjectSendStream[Tuple[State, State]]"
        self._internal_state_rx: "MemoryObjectReceiveStream[Tuple[State, State]]"
        (
            self._internal_state_tx,
            self._internal_state_rx,
        ) = anyio.create_memory_object_stream(1)
        self._external_state_tx: "MemoryObjectSendStream[State]"
        self._external_state_rx: "MemoryObjectReceiveStream[State]"
        (
            self._external_state_tx,
            self._external_state_rx,
        ) = anyio.create_memory_object_stream()
        self.state_changed = anyio.Event()  # type: ignore[no-untyped-call]

        self._inbound_msgs_tx: "MemoryObjectSendStream[paho.MQTTMessage]"
        self._inbound_msgs_rx: "MemoryObjectReceiveStream[paho.MQTTMessage]"
        (
            self._inbound_msgs_tx,
            self._inbound_msgs_rx,
        ) = anyio.create_memory_object_stream()

        self.write_tx: "MemoryObjectSendStream[None]"
        self.write_rx: "MemoryObjectReceiveStream[None]"
        self._write_tx, self._write_rx = anyio.create_memory_object_stream()

        self._subscriptions: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
        self._last_disconnect = datetime.min

        self._reconnect_loop_cancel_scope: Optional[anyio.CancelScope] = None
        self._other_loops_cancel_scope: Optional[anyio.CancelScope] = None
        self._io_loops_cancel_scope: Optional[anyio.CancelScope] = None

        task_group.start_soon(self._hold_stream_open, self._internal_state_tx)
        task_group.start_soon(self._hold_stream_open, self._external_state_tx)
        task_group.start_soon(self._hold_stream_open, self._external_state_rx)
        task_group.start_soon(self._handle_state_changes)

    # Public API
    @property
    def messages(self) -> "MemoryObjectReceiveStream[paho.MQTTMessage]":
        """
        A MemoryObjectReceiveStream on which all received MQTT messages will appear.
        """
        return self._inbound_msgs_rx

    @property
    def states(self) -> "MemoryObjectReceiveStream[State]":
        """
        A MemoryObjectReceiveStream on which the name of the state will appear on changes.
        """
        return self._external_state_rx.clone()

    @property
    def state(self) -> State:
        """
        The client's current state.
        """
        return self._state

    @wraps(paho.Client.connect)
    def connect(  # pylint: disable=missing-function-docstring
        self, *args: Any, **kwargs: Any
    ) -> None:
        _LOG.debug("connect() called")
        self._client.connect_async(*args, **kwargs)
        (
            self._inbound_msgs_tx,
            self._inbound_msgs_rx,
        ) = anyio.create_memory_object_stream()
        self._update_state(State.CONNECTING)

    @wraps(paho.Client.subscribe)
    def subscribe(  # pylint: disable=missing-function-docstring
        self, *args: Any, **kwargs: Any
    ) -> Tuple[int, int]:
        _LOG.debug("subscribe() called")
        self._subscriptions.append((args, kwargs))
        return cast(Tuple[int, int], self._client.subscribe(*args, **kwargs))

    async def wait_for_state(self, state: State) -> None:
        """
        Returns when desired state is reached.
        """
        while True:
            await self.state_changed.wait()
            if self.state == state:
                return

    def __getattr__(self, item: str) -> Any:
        """
        Expose the Paho client's attributes as our own if we haven't overridden them.
        """
        return getattr(self._client, item)

    # Private API
    def _update_state(self, state: State) -> None:
        try:
            self._internal_state_tx.send_nowait((self._state, state))
        except anyio.WouldBlock:
            _LOG.debug("Unable to update client state to %s", state)

    async def _start_io_loops(
        self, task_status: anyio.abc.TaskStatus = anyio.TASK_STATUS_IGNORED
    ) -> None:
        try:
            async with anyio.create_task_group() as task_group:
                self._io_loops_cancel_scope = task_group.cancel_scope
                self._write_tx, self._write_rx = anyio.create_memory_object_stream()
                await task_group.start(self._hold_stream_open, self._write_tx)
                task_group.start_soon(self._read_loop)
                task_group.start_soon(self._write_loop)
                task_status.started()
        finally:
            self._io_loops_cancel_scope = None

    def _stop_io_loops(self) -> None:
        if self._io_loops_cancel_scope is not None:
            self._io_loops_cancel_scope.cancel()

    # State machine handlers
    async def after_state_change(self) -> None:
        """
        Perform actions on every state change.
        """
        self.state_changed.set()
        # Ignore mypy error until https://github.com/agronholm/anyio/pull/289 is released
        self.state_changed = anyio.Event()  # type: ignore[no-untyped-call]
        try:
            self._external_state_tx.send_nowait(self._state)
        except anyio.WouldBlock:
            _LOG.debug(
                "Unable to send new state (%s) to external state stream", self._state
            )

    async def on_enter_connecting(self) -> None:
        """
        Perform actions when entering CONNECTING state.
        """

        async def start_other_loops() -> None:
            try:
                async with anyio.create_task_group() as task_group:
                    self._other_loops_cancel_scope = task_group.cancel_scope
                    task_group.start_soon(self._hold_stream_open, self._inbound_msgs_tx)
                    task_group.start_soon(self._misc_loop)
            finally:
                self._other_loops_cancel_scope = None

        async def do_connect() -> None:
            try:
                async with anyio.create_task_group() as task_group:
                    self._reconnect_loop_cancel_scope = task_group.cancel_scope
                    task_group.start_soon(self._reconnect_loop)
            finally:
                self._reconnect_loop_cancel_scope = None

        if self._other_loops_cancel_scope is None:
            self._task_group.start_soon(start_other_loops)
        self._task_group.start_soon(do_connect)

    async def on_exit_connecting(self) -> None:
        """
        Perform actions when exiting CONNECTING state.
        """
        if self._reconnect_loop_cancel_scope is not None:
            self._reconnect_loop_cancel_scope.cancel()

    async def on_enter_disconnected(self) -> None:
        """
        Perform actions when entering DISCONNECTED state.
        """
        if self._io_loops_cancel_scope is not None:
            self._io_loops_cancel_scope.cancel()

        if self._other_loops_cancel_scope is not None:
            self._other_loops_cancel_scope.cancel()

    # Loops
    async def _read_loop(self) -> None:
        _LOG.debug("_read_loop() started")
        while True:
            if self._sock is None:
                raise RuntimeError("Read loop is running, but _sock is None")
            try:
                await anyio.wait_socket_readable(self._sock)
            except ValueError:
                _LOG.exception("Exception when awaiting readable socket")
                await anyio.sleep(1)
                continue
            # TODO: Try/except?
            self._client.loop_read()

    async def _write_loop(self) -> None:
        _LOG.debug("_write_loop() started")
        # https://github.com/agronholm/anyio/issues/297
        async for _ in self._write_rx:  # pylint: disable=not-an-iterable
            if self._sock is None:
                raise RuntimeError("Write loop is running, but _sock is None")
            try:
                await anyio.wait_socket_writable(self._sock)
            except ValueError:
                _LOG.exception("Exception when awaiting writable socket")
                await anyio.sleep(1)
                continue
            self._client.loop_write()

    async def _misc_loop(self) -> None:
        _LOG.debug("_misc_loop() started")
        while True:
            # We don't really care what the return value is.
            # We'll just keep calling until we're cancelled.
            self._client.loop_misc()
            await anyio.sleep(1)

    async def _reconnect_loop(self) -> None:
        _LOG.debug("_reconnect_loop() started")
        connection_status = None
        while connection_status != paho.MQTT_ERR_SUCCESS:
            delay = (
                (self._last_disconnect + timedelta(seconds=1)) - datetime.now()
            ).total_seconds()
            if delay > 0:
                _LOG.info("Waiting %s second(s) before reconnecting", delay)
                await anyio.sleep(delay)
            if self._io_loops_cancel_scope is None:
                _LOG.warning("Reconnecting while the IO loops are not running")
            try:
                _LOG.debug("(Re)connecting...")
                connection_status = await anyio.to_thread.run_sync(self._client.reconnect)
                if connection_status != paho.MQTT_ERR_SUCCESS:
                    _LOG.error(
                        "(Re)connection failed with code %s (%s)",
                        connection_status,
                        paho.error_string(connection_status),
                    )
            # Doesn't really matter why it failed, so catch everything here
            except Exception:  # pylint: disable=broad-except
                _LOG.exception("(Re)connection failed")
            if connection_status != paho.MQTT_ERR_SUCCESS:
                # TODO: Configurable reconnect delay / limit
                await anyio.sleep(1)
        _LOG.debug("_reconnect_loop() finished")

    async def _hold_stream_open(
        self,
        stream: Union["MemoryObjectSendStream[Any]", "MemoryObjectReceiveStream[Any]"],
        task_status: anyio.abc.TaskStatus = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        """
        Hold the stream open so that it doesn't close when we can clone it and then
        discard the clone.
        """
        _LOG.debug("_hold_stream_open(%s) started", stream)
        try:
            async with stream:
                task_status.started()
                while True:
                    await anyio.sleep(math.inf)
        finally:
            _LOG.debug("_hold_stream_open(%s) finished", stream)

    async def _handle_state_changes(self) -> None:
        # https://github.com/agronholm/anyio/issues/297
        # pylint: disable=not-an-iterable
        async for old_state, new_state in self._internal_state_rx:
            self._state = new_state
            if old_state == new_state:
                continue
            _LOG.debug("New state: %s", new_state)

            if old_state == State.CONNECTING:
                await self.on_exit_connecting()
            # elif old_state == State.DISCONNECTED:
            #     await self.on_exit_disconnected()
            # elif old_state == State.CONNECTED:
            #     await self.on_exit_connected()

            if new_state == State.DISCONNECTED:
                await self.on_enter_disconnected()
            elif new_state == State.CONNECTING:
                await self.on_enter_connecting()
            # elif new_state == State.CONNECTED:
            #     await self.on_enter_connected()

            await self.after_state_change()

    # Paho client callbacks
    def _on_connect(  # pylint: disable=invalid-name
        self,
        client: paho.Client,
        userdata: Any,
        flags: Dict[str, int],
        rc: int,
        properties: Optional[paho.Properties] = None,
    ) -> None:
        # Called from main thread (via loop_read())
        _LOG.debug("_on_connect() rc: %s", rc)
        if rc != paho.CONNACK_ACCEPTED:
            _LOG.error(
                "Error connecting to MQTT broker (rc: %s - %s)",
                rc,
                paho.connack_string(rc),
            )
            return
        self._update_state(State.CONNECTED)
        for args, kwargs in self._subscriptions:
            _LOG.debug("Subscribing with %s, %s", args, kwargs)
            client.subscribe(*args, **kwargs)

    def _on_disconnect(  # pylint: disable=invalid-name
        self,
        client: paho.Client,
        userdata: Any,
        rc: int,
        properties: Optional[paho.Properties] = None,
    ) -> None:
        # Called from main thread (via loop_misc())
        _LOG.debug("_on_disconnect() rc: %s", rc)
        self._last_disconnect = datetime.now()
        if rc == paho.MQTT_ERR_SUCCESS:  # rc == 0
            # Deliberately disconnected on client request
            self._update_state(State.DISCONNECTED)
        else:
            self._update_state(State.CONNECTING)

    def _on_message(
        self, client: paho.Client, userdata: Any, msg: paho.MQTTMessage
    ) -> None:
        _LOG.debug("MQTT message received on topic %s", msg.topic)
        try:
            self._inbound_msgs_tx.send_nowait(msg)
        except anyio.WouldBlock:
            _LOG.warning("Discarding message because no handler is listening")

    def _on_socket_open(
        self, client: paho.Client, userdata: Any, sock: socket.socket
    ) -> None:
        _LOG.debug("_on_socket_open()")

        async def on_socket_open() -> None:
            self._sock = sock
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
            await self._task_group.start(self._start_io_loops)

        try:
            anyio.from_thread.run(on_socket_open)
        except RuntimeError:
            self._task_group.start_soon(on_socket_open)

    def _on_socket_close(
        self, client: paho.Client, userdata: Any, sock: socket.socket
    ) -> None:
        _LOG.debug("_on_socket_close()")

        async def on_socket_close() -> None:
            self._stop_io_loops()
            self._sock = None

        try:
            anyio.from_thread.run(on_socket_close)
        except RuntimeError:
            self._task_group.start_soon(on_socket_close)

    def _on_socket_register_write(
        self, client: paho.Client, userdata: Any, sock: socket.socket
    ) -> None:
        async def register_write() -> None:
            try:
                self._write_tx.send_nowait(None)
            except anyio.WouldBlock:
                _LOG.debug("Unable to register write")

        try:
            anyio.from_thread.run(register_write)
        except RuntimeError:
            self._task_group.start_soon(register_write)