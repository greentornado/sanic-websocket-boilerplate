# Copy from https://gist.github.com/ahopkins/9816b39aedb2d409ef8d1b85f62e8bec
import json
import random
import string
from functools import partial

from sanic import Sanic

import aioredis
import asyncio
import websockets
from dataclasses import dataclass, field
from typing import Optional, Set

app = Sanic(__name__)

PUBSUB_HOST = "localhost"
PUBSUB_PORT = "6379"
TIMEOUT = 10
INTERVAL = 20


def generate_code(length=12, include_punctuation=False):
    characters = string.ascii_letters + string.digits
    if include_punctuation:
        characters += string.punctuation
    return "".join(random.choice(characters) for x in range(length))


@dataclass
class Client:
    interface: websockets.server.WebSocketServerProtocol = field(repr=False)
    sid: str = field(default_factory=partial(generate_code, 36))

    def __hash__(self):
        return hash(str(self))

    async def keep_alive(self) -> None:
        while True:
            try:
                try:
                    pong_waiter = await self.interface.ping()
                    await asyncio.wait_for(pong_waiter, timeout=TIMEOUT)
                except asyncio.TimeoutError:
                    print("NO PONG!!")
                    await self.feed.unregister(self)
                else:
                    print(f"ping: {self.sid} on <{self.feed.name}>")
                await asyncio.sleep(INTERVAL)
            except websockets.exceptions.ConnectionClosed:
                print(f"broken connection: {self.sid} on <{self.feed.name}>")
                await self.feed.unregister(self)
                break

    async def shutdown(self) -> None:
        self.interface.close()

    async def receiver(self) -> None:
        try:
            self.feed.app.add_task(self.keep_alive())
            async for message in self.interface:
                await self.feed.publish(message)
        except websockets.exceptions.ConnectionClosed:
            print("connection closed")
        finally:
            await self.feed.unregister(self)


class FeedCache(dict):
    def __repr__(self):
        return str({k: len(v.clients) for k, v in self.items()})


class Feed:
    name: str
    app: Sanic
    clients: Set[Client]
    cache = FeedCache()
    lock: asyncio.Lock

    def __init__(self, name):
        self.name = name
        self.clients = set()
        self.lock = asyncio.Lock()

    @classmethod
    async def get(cls, name: str):
        is_existing = False

        if name in cls.cache:
            feed = cls.cache[name]
            await feed.acquire_lock()
            is_existing = True
        else:
            feed = cls(name=name)
            await feed.acquire_lock()

            feed.pool = await aioredis.create_redis_pool(
                address=f"redis://{PUBSUB_HOST}:{PUBSUB_PORT}",
                minsize=2,
                maxsize=500,
                encoding="utf-8",
            )

            cls.cache[name] = feed

            channels = await feed.pool.subscribe(name)
            if channels:
                feed.pubsub = channels[0]

        if not is_existing:
            loop = asyncio.get_event_loop()
            loop.create_task(feed.receiver())

        return feed, is_existing

    async def acquire_lock(self) -> None:
        if not self.lock.locked():
            print("Lock acquired")
            await self.lock.acquire()
        else:
            print("Lock already acquired")

    async def receiver(self) -> None:
        while True:
            try:
                await self.pubsub.wait_message()
                raw = await self.pubsub.get(encoding="utf-8")
                print(f">>> PUBSUB rcvd <{self.name}>: length=={len(raw)}")
            except aioredis.errors.ChannelClosedError:
                print(f">>> PUBSUB closed <{self.name}>")
                break
            else:
                if raw:
                    for client in self.clients:
                        try:
                            print(f"\tSending to {client.sid}")
                            await client.interface.send(raw)
                        except websockets.exceptions.ConnectionClosed:
                            print(f"ConnectionClosed. Client {client.sid}")

    async def register(
        self, websocket: websockets.server.WebSocketServerProtocol
    ) -> Optional[Client]:
        client = Client(interface=websocket)

        print(f">>> register {client} on {self.name}")
        client.feed = self
        self.clients.add(client)

        message = f"New client has joined."
        await self.publish(message)

        print(f"\nAll clients on {self.name}\n{self.clients}\n\n")

        return client

    async def unregister(self, client: Client) -> None:
        print(f">>> unregister {client} on <{self.name}>")
        if client in self.clients:
            await client.shutdown()
            self.clients.remove(client)
            print(
                f"\nAll remaining clients on <{self.name}>\n{self.clients}\n\n"
            )

            if len(self.clients) == 0:
                self.lock.release()
                await self.destroy()
                await self.pool.unsubscribe(self.name)
            else:
                message = f"Client has left."
                await self.publish(message)

    async def destroy(self) -> None:
        if not self.lock.locked():
            print(f">>> DESTROYING <{self.name}>")
            del self.cache[self.name]
            self.pool.close()
            print(f">>> DESTROYED <{self.name}>")
        else:
            print(f">>> <{self.name}> is locked. ABORT DESTROY.")

    async def publish(self, message: str) -> None:
        await self.pool.execute("publish", self.name, message)


@app.websocket("/<feed_name:[A-z][A-z0-9]+>")
async def feed(request, ws, feed_name):
    feed, is_existing = await Feed.get(feed_name)

    if not is_existing:
        feed.app = app

    client = await feed.register(ws)
    await client.receiver()


if __name__ == '__main__':
    try:
        import sys
        port = sys.argv[1]
    except IndexError:
        port = 5000
    app.run(host="0.0.0.0", port=port, debug=True)
