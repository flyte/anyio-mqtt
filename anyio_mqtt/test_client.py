from typing import Any

import anyio
import pytest

from . import AnyIOMQTTClient, AnyIOMQTTClientConfig, State


@pytest.fixture(params=anyio.get_all_backends(), scope="session")
def anyio_backend(request: Any) -> Any:
    return request.param


@pytest.fixture(scope="session")
async def mosquitto():
    print("Opening mosquitto..")
    result = await anyio.run_process(
        "docker run -d --rm -p 1883:1883 --name mosquitto toke/mosquitto"
    )
    assert result.returncode == 0
    print("Mosquitto opened")
    try:
        yield
    finally:
        result = await anyio.run_process("docker rm -fv mosquitto")
        assert result.returncode == 0
        print("Mosquitto closed")


async def test_connect(anyio_backend, mosquitto):
    async with AnyIOMQTTClient(AnyIOMQTTClientConfig(dict(clean_session=True))) as client:
        client.connect("localhost", 1883)
        with anyio.fail_after(5):
            await client.wait_for_state(State.CONNECTED)


async def test_disconnect_context_exit(anyio_backend, mosquitto):
    client = AnyIOMQTTClient(AnyIOMQTTClientConfig(dict(clean_session=True)))
    async with client:
        client.connect("localhost", 1883)
        with anyio.fail_after(5):
            await client.wait_for_state(State.CONNECTED)
    with anyio.fail_after(5):
        await client.wait_for_state(State.DISCONNECTED)


async def test_disconnect_explicit(anyio_backend, mosquitto):
    async with AnyIOMQTTClient(AnyIOMQTTClientConfig(dict(clean_session=True))) as client:
        client.connect("localhost", 1883)
        with anyio.fail_after(5):
            await client.wait_for_state(State.CONNECTED)
        client.disconnect()
        with anyio.fail_after(5):
            await client.wait_for_state(State.DISCONNECTED)


async def test_subscribe(anyio_backend, mosquitto):
    async with AnyIOMQTTClient(AnyIOMQTTClientConfig(dict(clean_session=True))) as client:
        client.connect("localhost", 1883)
        with anyio.fail_after(5):
            await client.wait_for_state(State.CONNECTED)
        _, mid = client.subscribe("anyio-mqtt/test")
        assert mid is not None
        with anyio.fail_after(5):
            await client.wait_for_subscription(mid)


async def test_receive(anyio_backend, mosquitto):
    topic: str = "anyio-mqtt/test"
    msg: str = "foo"
    async with AnyIOMQTTClient(AnyIOMQTTClientConfig(dict(clean_session=True))) as client:
        client.connect("localhost", 1883)
        with anyio.fail_after(5):
            await client.wait_for_state(State.CONNECTED)
        _, mid = client.subscribe(topic)
        assert mid is not None
        with anyio.fail_after(5):
            await client.wait_for_subscription(mid)

        listening = anyio.Event()
        async with anyio.create_task_group() as tg:

            async def publish():
                await listening.wait()
                client.publish(topic, msg)

            tg.start_soon(publish)
            with anyio.fail_after(5):
                async with client.messages:
                    listening.set()
                    async for msg_rx in client.messages:
                        assert msg_rx.topic == topic
                        assert msg_rx.payload.decode("utf8") == msg
                        break
