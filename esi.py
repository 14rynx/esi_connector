import asyncio
import json
import ssl
import sys
from itertools import islice

import aiohttp
import certifi
from async_lru import alru_cache
import nest_asyncio


def flatten(lst: list[list]) -> list:
    """Flattens a 2 dimensional list into a 1 dimensional one"""
    return [y for x in lst for y in x]


def chunks(data: list, size=10000) -> dict[int: list]:
    """Split an iterable into chunks"""
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k: data[k] for k in islice(it, size)}


class ESIConnector:
    """Holds all the data needed to make requests to CCP's ESI Api"""

    def __init__(self):
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.connector = aiohttp.TCPConnector(ssl=ssl_context, limit_per_host=500)
        self.session = aiohttp.ClientSession(connector=self.connector)
        self.url_base = "https://esi.evetech.net/latest/"
        self.url_end = "/?datasource=tranquility"

    def __del__(self):
        asyncio.run(self.close())

    async def close(self):
        await self.session.close()
        await self.connector.close()

    async def get(self, url_extension) -> dict[str]:
        """Makes a get requests to ESI"""

        async with self.session.get(self.url_base + str(url_extension) + self.url_end) as resp:
            try:
                return json.loads(await resp.text())
            except (KeyError, json.decoder.JSONDecodeError):
                return {}

    async def post(self, url_extension, data) -> dict[str]:
        """Makes a post requests to ESI"""

        async with self.session.post(self.url_base + str(url_extension) + self.url_end, data=data) as resp:
            try:
                return json.loads(await resp.text())
            except (KeyError, json.decoder.JSONDecodeError):
                return {}

    async def get_str(self, endpoint, id, key="name") -> str:
        data = await self.get(endpoint + str(id))
        try:
            return data[key]
        except KeyError:
            return ""

    async def get_id(self, endpoint, id, key="group_id") -> int:
        data = await self.get(endpoint + str(id))
        try:
            return int(data[key])
        except KeyError:
            return 0

    async def get_value(self, endpoint, id, key="group_id") -> float:
        data = await self.get(endpoint + str(id))
        try:
            return float(data[key])
        except KeyError:
            return 0.0

    async def get_ids(self, endpoint, id, key=None) -> list[int]:
        if key is None:
            key = ["group_id", "market_group_id"]
        data = await self.get(endpoint + str(id))
        try:
            return [int(data[k]) for k in key]
        except KeyError:
            return [0] * len(key)

    @alru_cache(maxsize=2000)
    async def get_alliance_name(self, id) -> str:
        return await self.get_str("alliances/", id)

    @alru_cache(maxsize=2000)
    async def get_corporation_name(self, id) -> str:
        return await self.get_str("corporations/", id)

    @alru_cache(maxsize=2000)
    async def get_character_name(self, id) -> str:
        return await self.get_str("characters/", id)

    @alru_cache(maxsize=2000)
    async def get_item_name(self, id) -> str:
        return await self.get_str("universe/types/", id)

    @alru_cache(maxsize=2000)
    async def get_item_group(self, id) -> int:
        return await self.get_id("universe/types/", id)

    @alru_cache(maxsize=2000)
    async def get_item_groups(self, id) -> list[int]:
        return await self.get_ids("universe/types/", id)

    @alru_cache(maxsize=2000)
    async def lookup(self, string) -> dict[str]:
        """Finds all ids that can relate to this string"""
        return await self.post("universe/ids", json.dumps([string]))

    @alru_cache(maxsize=2000)
    async def get_alliance_corporations(self, alliance_id) -> list[int]:
        return await self.get(f"/alliances/{alliance_id}/corporations/")

    @alru_cache(maxsize=2000)
    async def get_corp_alliance(self, corporation_id) -> int:
        return await self.get_id("corporations/", corporation_id, key="alliance_id")

    @alru_cache(maxsize=2000)
    async def get_system_name(self, system_id) -> str:
        return await self.get_str("universe/systems/", system_id, key="name")

    @alru_cache(maxsize=2000)
    async def get_system_security(self, system_id) -> float:
        return await self.get_value("universe/systems/", system_id, key="security_status")

    @alru_cache(maxsize=2000)
    async def get_system_name(self, system_id) -> str:
        return await self.get_str(f"universe/systems/", system_id, key="name")

    @alru_cache(maxsize=2000)
    async def get_system_constellation_id(self, system_id) -> int:
        return await self.get_id(f"universe/systems/", system_id, key="constellation_id")

    @alru_cache(maxsize=2000)
    async def get_system_region_id(self, system_id) -> int:
        constellation_id = await self.get_system_constellation_id(system_id)
        return await self.get_id(f"universe/constellations/", constellation_id, key="region_id")

    @alru_cache(maxsize=2000)
    async def get_system_region_name(self, system_id) -> str:
        region_id = await self.get_system_region_id(system_id)
        return await self.get_str(f"universe/regions/", region_id, key="name")

    @alru_cache(maxsize=2000)
    async def get_system_space_name(self, system_id) -> str:
        region_id = await self.get_system_region_id(system_id)
        sec_status = await self.get_system_security(system_id)
        if system_id > 31000000:
            return "Wormholes"
        elif region_id == 10000070:
            return "Pochven"
        elif region_id == 10001000:
            return "Zarzakh"
        elif sec_status >= 0.5:
            return "Highsec"
        elif sec_status > 0:
            return "Lowsec"
        else:
            return "Nullsec"


@alru_cache(maxsize=2000)
async def get_character_corporation(self, character_id) -> int:
    return await self.get_id("characters/", character_id, key="corporation_id")


@alru_cache(maxsize=2000)
async def get_corp_member_count(self, corporation_id) -> int:
    return int(await self.get_value("corporations/", corporation_id, key="member_count"))


@alru_cache(maxsize=2000)
async def get_jumps(self, origin_system_id, destination_system_id):
    response = await self.get(f"route/{origin_system_id}/{destination_system_id}/")
    try:
        return len(response)
    except ValueError:
        return sys.maxsize


@alru_cache(maxsize=2000)
async def get_gate_destination(self, gate_id):
    response = await self.get(f"universe/stargates/{gate_id}/")
    return response["destination"]["system_id"]


@alru_cache(maxsize=2000)
async def get_connected_systems(self, system_id):
    response = await self.get(f"universe/systems/{system_id}/")
    try:
        gates = response["stargates"]
        destination_tasks = [self.get_gate_destination(gate_id) for gate_id in gates]
        return await asyncio.gather(*destination_tasks)
    except KeyError:
        pass
    return []


async def get_proximity_systems(self, start, max_distance=10):
    known = [start]
    front = [start]
    out = [(start, 0)]
    for x in range(1, max_distance + 1):
        connection_tasks = [self.get_connected_systems(s) for s in front]
        connected = flatten(await asyncio.gather(*connection_tasks))
        front = list(set([i for i in connected if i not in known]))
        known.extend(connected)
        out.extend([(n, x) for n in front])
    return out


# Functions to get a kill from esi
async def get_kill(self, kill_id, kill_hash, progress_bar=None):
    async with self.session.get(
            f"https://esi.evetech.net/latest/killmails/{kill_id}/{kill_hash}/?datasource=tranquility") as response:
        try:
            kill = await response.json(content_type=None)
            if progress_bar:
                progress_bar.update(1)
            return kill
        except json.decoder.JSONDecodeError:
            return await self.get_kill(kill_id, kill_hash, progress_bar)


async def gather_kills(self, kills, progress_bar=None):
    tasks = [self.get_kill(*kill, progress_bar) for kill in kills.items()]
    return await asyncio.gather(*tasks)


async def gather_kills_chunked(self, hashes, pbar=None):
    for chunk in chunks(hashes, 1000):
        for kill in (await self.gather_kills(chunk, pbar)):
            yield kill


nest_asyncio.apply()
