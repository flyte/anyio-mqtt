import anyio

from anyio_mqtt import AnyIOMQTTClient

import logging

_LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("anyio_mqtt").setLevel(logging.DEBUG)

PAHO_LOGGER = logging.getLogger("paho")
PAHO_LOGGER.setLevel(logging.DEBUG)


async def main() -> None:
    _LOG.debug("Creating client")
    async with AnyIOMQTTClient() as client:
        client.enable_logger(PAHO_LOGGER)
        client.username_pw_set("test", "tesffffft")
        _LOG.debug("Subscribing to a/b/c")
        client.subscribe("a/b/c")
        _LOG.debug("Connecting to broker")
        client.connect("walternate")
        _LOG.debug("Subscribing to d/e/f")
        client.subscribe("d/e/f")
        _LOG.debug("Publishing message to a/b/c with QoS 0")
        client.publish("a/b/c", "hi0", qos=0)
        _LOG.debug("Publishing message to a/b/c with QoS 1")
        client.publish("a/b/c", "hi1", qos=1)
        _LOG.debug("Publishing message to a/b/c with QoS 2")
        client.publish("a/b/c", "hi2", qos=2)
        i = 0
        _LOG.debug("Waiting for messages (1)")
        async for msg in client.messages:
            print(
                f"Message received in test.py (1): {msg.topic} - {msg.payload.decode('utf8')}"
            )
            i += 1
            if i >= 5:
                break
        _LOG.debug("Publishing message to a/b/c with QoS 0")
        client.publish("a/b/c", "2hi0", qos=0)
        _LOG.debug("Not listening for messages for 3 seconds")
        await anyio.sleep(3)
        i = 0
        _LOG.debug("Waiting for messages (2)")
        async for msg in client.messages:
            print(
                f"Message received in test.py (2): {msg.topic} - {msg.payload.decode('utf8')}"
            )
            i += 1
            if i >= 5:
                _LOG.debug("Calling client.disconnect()")
                client.disconnect()
                break
        _LOG.debug("Publishing message to a/b/c with QoS 0")
        client.publish("a/b/c", "3hi0", qos=0)
        _LOG.debug("Publishing message to a/b/c with QoS 1")
        client.publish("a/b/c", "3hi1", qos=1)
        _LOG.debug("Publishing message to a/b/c with QoS 2")
        client.publish("a/b/c", "3hi2", qos=2)
        _LOG.debug("Waiting 3 seconds")
        await anyio.sleep(3)
        _LOG.debug("Connecting to broker")
        client.connect("localhost")
        i = 0
        _LOG.debug("Waiting for messages (3)")
        async for msg in client.messages:
            print(
                f"Message received in test.py (3): {msg.topic} - {msg.payload.decode('utf8')}"
            )
            i += 1
            if i >= 5:
                print("Breaking out of last msg loop")
                break
        print("Now leaving async context...")
    print("Finished!")


if __name__ == "__main__":
    anyio.run(main)
