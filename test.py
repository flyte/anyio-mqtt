import anyio

from anyio_mqtt import AnyIOMQTTClient

async def main():
    async with anyio.create_task_group() as tg:
        client = AnyIOMQTTClient(tg)
        client.subscribe("a/b/c")
        client.connect("localhost")
        client.subscribe("d/e/f")

if __name__ == "__main__":
    anyio.run(main)