import enum
import logging
import math
import socket
from typing import Any, Optional
from datetime import datetime, timedelta

import anyio
import anyio.abc
import paho.mqtt.client as paho

_LOG = logging.getLogger(__name__)


# TODO:
# - Error handling. Work out what constitutes a reason to break out of the connect loop
#   and change back to disconnected state.


class State(enum.Enum):
    INITIAL = enum.auto()
    DISCONNECTED = enum.auto()
    CONNECTING = enum.auto()
    CONNECTED = enum.auto()


class DisconnectedException(Exception):
    pass


class AnyIOMQTTClient:
    def __init__(self, task_group: anyio.abc.TaskGroup, config=None):
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
        # self._client.on_socket_unregister_write = self._on_socket_unregister_write

        self._state: State = State.INITIAL
        (
            self._internal_state_tx,
            self._internal_state_rx,
        ) = anyio.create_memory_object_stream(1)
        (
            self._external_state_tx,
            self._external_state_rx,
        ) = anyio.create_memory_object_stream()
        self.state_changed = anyio.Event()

        (
            self._inbound_msgs_tx,
            self._inbound_msgs_rx,
        ) = anyio.create_memory_object_stream()

        self._write_tx, self._write_rx = anyio.create_memory_object_stream()

        self._subscriptions = []
        self._last_disconnect = datetime.min

        self._reconnect_loop_cancel_scope: Optional[anyio.CancelScope] = None
        self._other_loops_cancel_scope: Optional[anyio.CancelScope] = None
        self._io_loops_cancel_scope: Optional[anyio.CancelScope] = None

        task_group.start_soon(self._hold_stream_open, self._internal_state_tx)
        task_group.start_soon(self._hold_stream_open, self._external_state_tx)
        task_group.start_soon(self._hold_stream_open, self._external_state_rx)
        task_group.start_soon(self.handle_state_changes)

    async def handle_state_changes(self):
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

    async def start_io_loops(self, task_status=anyio.TASK_STATUS_IGNORED):
        try:
            async with anyio.create_task_group() as tg:
                self._io_loops_cancel_scope = tg.cancel_scope
                self._write_tx, self._write_rx = anyio.create_memory_object_stream()
                await tg.start(self._hold_stream_open, self._write_tx)
                tg.start_soon(self._read_loop)
                tg.start_soon(self._write_loop)
                task_status.started()
        finally:
            self._io_loops_cancel_scope = None

    def stop_io_loops(self):
        if self._io_loops_cancel_scope is not None:
            self._io_loops_cancel_scope.cancel()

    # State machine callbacks
    async def after_state_change(self):
        self.state_changed.set()
        self.state_changed = anyio.Event()
        try:
            self._external_state_tx.send_nowait(self._state)
        except anyio.WouldBlock:
            _LOG.debug(
                "Unable to send new state (%s) to external state stream", self._state
            )

    async def on_enter_connecting(self):
        async def start_other_loops():
            try:
                async with anyio.create_task_group() as tg:
                    self._other_loops_cancel_scope = tg.cancel_scope
                    tg.start_soon(self._hold_stream_open, self._inbound_msgs_tx)
                    tg.start_soon(self._misc_loop)
            finally:
                self._other_loops_cancel_scope = None

        async def do_connect():
            try:
                async with anyio.create_task_group() as tg:
                    self._reconnect_loop_cancel_scope = tg.cancel_scope
                    tg.start_soon(self._reconnect_loop)
            finally:
                self._reconnect_loop_cancel_scope = None

        if self._other_loops_cancel_scope is None:
            self._task_group.start_soon(start_other_loops)
        self._task_group.start_soon(do_connect)

    async def on_exit_connecting(self):
        if self._reconnect_loop_cancel_scope is not None:
            self._reconnect_loop_cancel_scope.cancel()

    async def on_enter_disconnected(self):
        if self._io_loops_cancel_scope is not None:
            self._io_loops_cancel_scope.cancel()

        if self._other_loops_cancel_scope is not None:
            self._other_loops_cancel_scope.cancel()

    # Public API
    def connect(self, *args, **kwargs):
        _LOG.debug("connect() called")
        self._client.connect_async(*args, **kwargs)
        (
            self._inbound_msgs_tx,
            self._inbound_msgs_rx,
        ) = anyio.create_memory_object_stream()
        self._update_state(State.CONNECTING)

    def subscribe(self, *args, **kwargs):
        _LOG.debug("subscribe() called")
        self._subscriptions.append((args, kwargs))
        self._client.subscribe(*args, **kwargs)
        # await anyio.to_thread.run_sync(partial(self._client.subscribe, *args, **kwargs))

    def __getattr__(self, item: str):
        """
        Expose the Paho client's attributes as our own.
        """
        return getattr(self._client, item)

    @property
    def messages(self):
        return self._inbound_msgs_rx

    @property
    def states(self):
        return self._external_state_rx.clone()

    @property
    def state(self):
        return self._state

    def _update_state(self, state):
        try:
            self._internal_state_tx.send_nowait((self._state, state))
        except anyio.WouldBlock:
            _LOG.debug("Unable to update client state to %s", state)

    # Paho client callbacks
    def _on_connect(self, client, userdata, flags, rc) -> None:
        # Called from main thread (via loop_read())
        _LOG.debug("_on_connect() rc: %s", rc)
        if rc != paho.CONNACK_ACCEPTED:
            # TODO: What do we do on error?
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

    def _on_disconnect(self, client, userdata, rc, properties=None) -> None:
        # Called from main thread (via loop_misc())
        _LOG.debug("_on_disconnect() rc: %s", rc)
        self._last_disconnect = datetime.now()
        if rc == paho.MQTT_ERR_SUCCESS:  # rc == 0
            # Deliberately disconnected on client request
            self._update_state(State.DISCONNECTED)
        else:
            self._update_state(State.CONNECTING)

    def _on_message(self, client, userdata, msg: paho.MQTTMessage):
        _LOG.debug("MQTT message received on topic %s", msg.topic)
        try:
            self._inbound_msgs_tx.send_nowait(msg)
        except anyio.WouldBlock:
            _LOG.warning("Discarding message because no handler is listening")

    def _on_socket_open(
        self, client: paho.Client, userdata: Any, sock: socket.socket
    ) -> None:
        _LOG.debug("_on_socket_open()")

        async def on_socket_open():
            self._sock = sock
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
            await self._task_group.start(self.start_io_loops)

        try:
            anyio.from_thread.run(on_socket_open)
        except RuntimeError:
            self._task_group.start_soon(on_socket_open)

    def _on_socket_close(self, client, userdata, sock) -> None:
        _LOG.debug("_on_socket_close()")

        async def on_socket_close():
            self.stop_io_loops()
            self._sock = None

        try:
            anyio.from_thread.run(on_socket_close)
        except RuntimeError:
            self._task_group.start_soon(on_socket_close)

    def _on_socket_register_write(self, client, userdata, sock):
        async def register_write():
            try:
                self._write_tx.send_nowait(None)
            except anyio.WouldBlock:
                _LOG.debug("Unable to register write")

        try:
            anyio.from_thread.run(register_write)
        except RuntimeError:
            self._task_group.start_soon(register_write)

    # def _on_socket_unregister_write(self, client, userdata, sock):
    #     # Set the previous event, to make sure we don't deadlock any existing waiters.
    #     self._large_write.set()
    #     self._large_write = anyio.Event()

    # Loops
    async def _read_loop(self) -> None:
        _LOG.debug("_read_loop() started")
        while True:
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
        async for _ in self._write_rx:
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
                connection_status = await anyio.to_thread.run_sync(
                    self._client.reconnect
                )
                if connection_status != paho.MQTT_ERR_SUCCESS:
                    _LOG.error(
                        "(Re)connection failed with code %s (%s)",
                        connection_status,
                        paho.error_string(connection_status),
                    )
            except Exception:
                _LOG.exception("(Re)connection failed")
            if connection_status != paho.MQTT_ERR_SUCCESS:
                # TODO: Configurable reconnect delay / limit
                await anyio.sleep(1)
            _LOG.debug("_reconnect_loop() finished")

    async def _hold_stream_open(self, stream, task_status=anyio.TASK_STATUS_IGNORED):
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
