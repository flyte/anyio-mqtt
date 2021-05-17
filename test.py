import anyio

from anyio_mqtt import AnyIOMQTTClient


async def main():
    async with anyio.create_task_group() as tg:
        client = AnyIOMQTTClient(tg)
        client.subscribe("a/b/c")
        client.connect("localhost")
        client.subscribe("d/e/f")
        i = 0
        async for msg in client.messages:
            print(
                f"Message received in test.py (1): {msg.topic} - {msg.payload.decode('utf8')}"
            )
            i += 1
            if i >= 5:
                break
        await anyio.sleep(3)
        i = 0
        async for msg in client.messages:
            print(
                f"Message received in test.py (2): {msg.topic} - {msg.payload.decode('utf8')}"
            )
            i += 1
            if i >= 5:
                client.disconnect()
                break
        await anyio.sleep(3)
        client.connect("localhost")
        i = 0
        async for msg in client.messages:
            print(
                f"Message received in test.py (3): {msg.topic} - {msg.payload.decode('utf8')}"
            )


if __name__ == "__main__":
    anyio.run(main)
