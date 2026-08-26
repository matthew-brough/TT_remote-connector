import argparse
import asyncio
import logging

import aiohttp

try:
    import ujson as json
except ImportError:
    import json

from .CDP.remote_connector import CDPClient
from .enums.subscriptions import EventSubscriptions

logging.basicConfig(level=logging.INFO)
logging.getLogger("asyncio").setLevel(logging.ERROR)
logging.getLogger("websockets").setLevel(logging.ERROR)


async def main(resource_name: str) -> None:
    running_lock = asyncio.Event()

    async with aiohttp.ClientSession(json_serialize=json.dumps) as session:
        running_lock.set()
        await CDPClient(session, running_lock, resource_name).enter(EventSubscriptions.enable_position)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Connect to a specific FiveM NUI resource through CDP.")
    parser.add_argument("resource_name", help="FiveM NUI resource name to attach to")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args().resource_name))
    except (RuntimeError, KeyboardInterrupt):
        logging.info("Exiting...")
    except Exception:
        logging.exception("Unhandled connector failure")
