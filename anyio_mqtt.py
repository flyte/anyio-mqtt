import logging
import socket
from functools import partial, wraps
from typing import Optional, Any, Tuple, List, Dict
from transitions import Machine

import anyio
import anyio.abc
import paho.mqtt.client as paho

_LOG = logging.getLogger(__name__)
_LOG.setLevel(logging.DEBUG)

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("transitions").setLevel(logging.DEBUG)

def event_looper(event_name: str):
    def _event_looper(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            event: anyio.Event = getattr(self, event_name)
            while True:
                await event.wait()
                try:
                    await func(self, *args, **kwargs)
                except Exception:
                    _LOG.exception("Exception in %s", func)
                    await anyio.sleep(1)
        return wrapper
    return _event_looper

class AnyIOMQTTClient:
    def __init__(self, task_group: anyio.abc.TaskGroup, config=None):
        if config is None:
            config = {}

        self._task_group = task_group

        states = ["disconnected", "connecting", "connected"]
        transitions = [
            ["_state_request_connect", "*", "connecting"],
            ["_state_succeed_connect", "connecting", "connected"],
            ["_state_disconnect", "*", "disconnected"],
        ]
        self._machine = Machine(
            model=self, states=states, transitions=transitions, initial="disconnected"
        )

        self._sock: Optional[socket.socket] = None

        self._client: paho.Client = paho.Client(**config)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_socket_open = self._on_socket_open
        self._client.on_socket_close = self._on_socket_close
        self._client.on_socket_register_write = self._on_socket_register_write
        self._client.on_socket_unregister_write = self._on_socket_unregister_write

        self._msg_tx, self._msg_rx = anyio.create_memory_object_stream()

        self._socket_open = anyio.Event()
        self._large_write = anyio.Event()

        self._subscriptions = []
        self._connect_args: Optional[Tuple[List, Dict[str, Any]]] = None

        self._task_group.start_soon(self._read_loop)
        self._task_group.start_soon(self._write_loop)
        self._task_group.start_soon(self._misc_loop)

        self._connecting_cancel_scope: Optional[anyio.CancelScope] = None

    # State machine callbacks
    def on_enter_connecting(self):
        async def do_connect():
            async with anyio.create_task_group() as tg:
                self._connecting_cancel_scope = tg.cancel_scope
                await self._connect_loop()
                self._connecting_cancel_scope = None
        self._task_group.start_soon(do_connect)

    def on_exit_connecting(self):
        if self._connecting_cancel_scope is not None:
            self._connecting_cancel_scope.cancel()

    # Public API
    def connect(self, *args, **kwargs):
        self._connect_args = (args, kwargs)
        self._state_request_connect()

    def disconnect(self, *args, **kwargs):
        self._client.disconnect(*args, **kwargs)

    def subscribe(self, *args, **kwargs) -> None:
        self._subscriptions.append((args, kwargs))
        if self.is_connected():
            self._client.subscribe(*args, **kwargs)

    @property
    def messages(self):
        return self._msg_rx

    # Paho client callbacks
    def _on_connect(self, client, userdata, flags, rc) -> None:
        _LOG.debug("_on_connect()")
        self._state_succeed_connect()
        for args, kwargs in self._subscriptions:
            _LOG.debug("Subscribing with %s, %s", args, kwargs)
            client.subscribe(*args, **kwargs)

    def _on_disconnect(self, client, userdata, rc, properties=None) -> None:
        if rc == paho.MQTT_ERR_SUCCESS:
            # Deliberately disconnected on client request
            self._state_disconnect()
        else:
            self._state_request_connect()

    def _on_message(self, client, userdata, msg: paho.MQTTMessage):
        print(f"{msg.topic}: {msg.payload.decode('utf8')}")

    def _on_socket_open(
        self, client: paho.Client, userdata: Any, sock: socket.socket
    ) -> None:
        _LOG.debug("_on_socket_open()")
        self._sock = sock
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        self._socket_open.set()

    def _on_socket_close(self, client, userdata, sock) -> None:
        self._sock = None
        self._socket_open = anyio.Event()

    def _on_socket_register_write(self, client, userdata, sock):
        self._large_write.set()

    def _on_socket_unregister_write(self, client, userdata, sock):
        self._large_write = anyio.Event()

    # Loops
    async def _read_loop(self) -> None:
        while True:
            await self._socket_open.wait()
            try:
                await anyio.wait_socket_readable(self._sock)
            except ValueError:
                _LOG.exception("Exception when awaiting readable socket")
                continue
            # TODO: Try/except?
            self._client.loop_read()

    async def _write_loop(self):
        while True:
            await self._large_write.wait()
            await self._socket_open.wait()
            try:
                await anyio.wait_socket_writable(self._sock)
            except ValueError:
                _LOG.exception("Exception when awaiting writable socket")
                continue
            self._client.loop_write()

    async def _misc_loop(self) -> None:
        while True:
            # We don't really care what the return value is.
            # We'll just keep calling until we're cancelled.
            self._client.loop_misc()
            await anyio.sleep(1)

    async def _connect_loop(self) -> None:
        args, kwargs = self._connect_args
        while True:
            try:
                connect = partial(self._client.connect, *args, **kwargs)
                _LOG.debug("Connecting...")
                await anyio.to_thread.run_sync(connect)
            except Exception:
                _LOG.exception("Connection failed")
                await anyio.sleep(1)
                continue
            _LOG.debug("_connect_loop() setting perform_connect state")
            return
