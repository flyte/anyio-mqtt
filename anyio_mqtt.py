import logging
import math
import socket
from functools import partial, wraps
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import anyio
import anyio.abc
import paho.mqtt.client as paho
import sniffio
from transitions import Machine

_LOG = logging.getLogger(__name__)


# TODO:
# - Error handling. Work out what constitutes a reason to break out of the connect loop
#   and change back to disconnected state.


class DisconnectedException(Exception):
    pass


class AnyIOMQTTClient:
    def __init__(
        self,
        task_group: anyio.abc.TaskGroup,
        config=None
    ):
        if config is None:
            config = {}

        self._connect_message: Optional[Tuple[List[Any], Dict[str, Any]]] = None

        self._task_group = task_group
        self._sock: Optional[socket.socket] = None

        self._client: paho.Client = paho.Client(**config)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_socket_open = self._on_socket_open
        self._client.on_socket_close = self._on_socket_close
        self._client.on_socket_register_write = self._on_socket_register_write
        self._client.on_socket_unregister_write = self._on_socket_unregister_write

        states = ["disconnected", "connecting", "connected"]
        transitions = [
            ["_state_request_connect", "*", "connecting"],
            ["_state_succeed_connect", "connecting", "connected"],
            ["_state_disconnect", "*", "disconnected"],
        ]
        self._machine = Machine(
            model=self, states=states, transitions=transitions, initial="disconnected"
        )

        (
            self._inbound_msgs_tx,
            self._inbound_msgs_rx,
        ) = anyio.create_memory_object_stream()

        self._socket_open = anyio.Event()
        self._large_write = anyio.Event()
        self.disconnect_event = anyio.Event()

        self._subscriptions = []
        self._last_disconnect = datetime.min

        self._reconnect_loop_cancel_scope: Optional[anyio.CancelScope] = None
        self._other_loops_cancel_scope: Optional[anyio.CancelScope] = None
        self._io_loops_cancel_scope: Optional[anyio.CancelScope] = None

    # State machine callbacks
    def on_enter_connected(self):
        if self._connect_message is not None:
            args, kwargs = self._connect_message
            self._client.publish(*args, **kwargs)

    def on_enter_connecting(self):
        async def start_io_loops():
            async with anyio.create_task_group() as tg:
                self._io_loops_cancel_scope = tg.cancel_scope
                tg.start_soon(self._read_loop)
                tg.start_soon(self._write_loop)

        async def start_other_loops():
            async with anyio.create_task_group() as tg:
                self._other_loops_cancel_scope = tg.cancel_scope
                tg.start_soon(self._open_inbound_msgs_tx_stream)
                tg.start_soon(self._misc_loop)
            self._other_loops_cancel_scope = None

        async def do_connect():
            async with anyio.create_task_group() as tg:
                self._reconnect_loop_cancel_scope = tg.cancel_scope
                tg.start_soon(self._reconnect_loop)
            self._reconnect_loop_cancel_scope = None

        if self._io_loops_cancel_scope is None:
            self._task_group.start_soon(start_io_loops)
        if self._other_loops_cancel_scope is None:
            self._task_group.start_soon(start_other_loops)
        self._task_group.start_soon(do_connect)

    def on_exit_connecting(self):
        if self._reconnect_loop_cancel_scope is not None:
            self._reconnect_loop_cancel_scope.cancel()
            self._reconnect_loop_cancel_scope = None

    def on_enter_disconnected(self):
        if self._io_loops_cancel_scope is not None:
            self._io_loops_cancel_scope.cancel()
            self._io_loops_cancel_scope = None

        if self._other_loops_cancel_scope is not None:
            self._other_loops_cancel_scope.cancel()
            self._other_loops_cancel_scope = None

        self.disconnect_event.set()

    def on_exit_disconnected(self):
        (
            self._inbound_msgs_tx,
            self._inbound_msgs_rx,
        ) = anyio.create_memory_object_stream()
        self.disconnect_event = anyio.Event()

    # Public API
    def connect(self, *args, **kwargs):
        _LOG.debug("connect() called")
        self._client.connect_async(*args, **kwargs)
        self._state_request_connect()

    async def subscribe(self, *args, **kwargs):
        _LOG.debug("subscribe() called")
        self._subscriptions.append((args, kwargs))
        await anyio.to_thread.run_sync(partial(self._client.subscribe, *args, **kwargs))

    def set_connect_message(self, *args, **kwargs):
        self._connect_message = (args, kwargs)

    def clear_connect_message(self):
        self._connect_message = None

    def __getattr__(self, item: str):
        """
        Expose the Paho client's attributes as our own.
        """
        return getattr(self._client, item)

    @property
    def messages(self):
        return self._inbound_msgs_rx

    # Paho client callbacks
    def _on_connect(self, client, userdata, flags, rc) -> None:
        _LOG.debug("_on_connect() rc: %s", rc)
        if rc != paho.CONNACK_ACCEPTED:
            # TODO: What do we do on error?
            _LOG.error(
                "Error connecting to MQTT broker (rc: %s - %s)",
                rc,
                paho.connack_string(rc),
            )
            return
        self._state_succeed_connect()
        for args, kwargs in self._subscriptions:
            _LOG.debug("Subscribing with %s, %s", args, kwargs)
            client.subscribe(*args, **kwargs)

    def _on_disconnect(self, client, userdata, rc, properties=None) -> None:
        _LOG.debug("_on_disconnect() rc: %s", rc)
        self._last_disconnect = datetime.now()
        if rc == paho.MQTT_ERR_SUCCESS:  # rc == 0
            # Deliberately disconnected on client request
            self._state_disconnect()
        else:
            self._state_request_connect()

    def _on_message(self, client, userdata, msg: paho.MQTTMessage):
        _LOG.debug("MQTT message received on topic %s", msg.topic)
        msg_tx = self._inbound_msgs_tx.clone()
        try:
            msg_tx.send_nowait(msg)
        except anyio.WouldBlock:
            _LOG.warning("Discarding message because no handler is listening")

    def _on_socket_open(
        self, client: paho.Client, userdata: Any, sock: socket.socket
    ) -> None:
        _LOG.debug("_on_socket_open()")
        self._sock = sock
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        async def set_socket_open():
            self._socket_open.set()

        anyio.from_thread.run(set_socket_open)

    def _on_socket_close(self, client, userdata, sock) -> None:
        self._sock = None
        # Set the previous event, to make sure we don't deadlock any existing waiters.
        # They'll just have to deal with the fact that the socket is actually closed.
        self._socket_open.set()
        try:
            self._socket_open = anyio.Event()
        except sniffio.AsyncLibraryNotFoundError:
            _LOG.debug(
                "Unable to recreate self._socket_open event due to not being in aync context"
            )

    def _on_socket_register_write(self, client, userdata, sock):
        self._large_write.set()

    def _on_socket_unregister_write(self, client, userdata, sock):
        # Set the previous event, to make sure we don't deadlock any existing waiters.
        self._large_write.set()
        self._large_write = anyio.Event()

    # Loops
    async def _read_loop(self) -> None:
        _LOG.debug("_read_loop() started")
        while True:
            await self._socket_open.wait()
            try:
                await anyio.wait_socket_readable(self._sock)
            except ValueError:
                _LOG.exception("Exception when awaiting readable socket")
                await anyio.sleep(1)
                continue
            # TODO: Try/except?
            self._client.loop_read()

    async def _write_loop(self):
        _LOG.debug("_write_loop() started")
        while True:
            await self._large_write.wait()
            await self._socket_open.wait()
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

    async def _open_inbound_msgs_tx_stream(self):
        """
        Hold the tx end of the inbound msgs stream open so that we can clone it and send
        messages from the paho on_message callback.
        """
        _LOG.debug("_open_inbound_msgs_tx_stream() started")
        async with self._inbound_msgs_tx:
            while True:
                await anyio.sleep(math.inf)
