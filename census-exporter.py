#!/usr/bin/env python3

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import reduce
from multiprocessing.pool import ThreadPool
from operator import getitem
from typing import TYPE_CHECKING

import click
import requests
import structlog
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from voluptuous import Invalid, MultipleInvalid, Schema

if TYPE_CHECKING:
    from collections.abc import Callable


log = structlog.get_logger()

VERSION_PATTERN = re.compile(r"^(?P<version>gluon-v\d{4}\.\d(?:\.\d)?(?:-\d+)?).*")
BASE_PATTERN = re.compile(r"^(?P<base>gluon-v\d{4}\.\d(?:\.\d)?).*")

seen = set()
duplicates = 0


# minimal schema definitions to recognize formats
SCHEMA_MESHVIEWER = Schema({"timestamp": str, "nodes": [dict], "links": [dict]})

SCHEMA_MESHVIEWER_OLD = Schema(
    {"meta": {"timestamp": str}, "nodes": [dict], "links": [dict]},
)

SCHEMA_NODESJSONV1 = Schema({"timestamp": str, "version": 1, "nodes": dict})

SCHEMA_NODESJSONV2 = Schema({"timestamp": str, "version": 2, "nodes": [dict]})

FORMATS: dict[str, Format] = {}


@dataclass
class Format:
    name: str
    schema: Schema
    parser: Callable[[dict], tuple[dict[str, int], dict[str, int], dict[str, int]]]


def normalize_model_name(name: str) -> str:
    return re.sub(r"\s+", " ", name)


def register_hook(
    name: str,
    schema: Schema,
    parser: Callable[[dict], tuple[dict[str, int], dict[str, int], dict[str, int]]],
) -> None:
    FORMATS[name] = Format(name=name, schema=schema, parser=parser)


def already_seen(node_id: str) -> bool:
    global duplicates, seen
    if node_id in seen:
        duplicates += 1
        return True
    seen.add(node_id)
    return False


def parse_meshviewer(
    data: dict,
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    global seen, duplicates
    bases: dict[str, int] = defaultdict(int)
    models: dict[str, int] = defaultdict(int)
    domains: dict[str, int] = defaultdict(int)
    for node in data["nodes"]:
        try:
            node_id = node["node_id"]
            if already_seen(node_id):
                continue
            base = node["firmware"]["base"]
            match = VERSION_PATTERN.match(base)
            if match:
                bases[match.group("version")] += 1
            model = normalize_model_name(node["model"])
            models[model] += 1
            domain = node["domain"]
            domains[domain] += 1
        except KeyError as ex:
            continue
    return bases, models, domains


def parse_nodes_json_v1(
    data: dict,
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    global seen, duplicates
    bases: dict[str, int] = defaultdict(int)
    for node_id, node in data["nodes"].items():
        if already_seen(node_id):
            continue
        try:
            base = node["nodeinfo"]["software"]["firmware"]["base"]
        except KeyError as ex:
            continue
        match = VERSION_PATTERN.match(base)
        if match:
            bases[match.group("version")] += 1
    return bases, {}, {}


def parse_nodes_json_v2(
    data: dict,
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    global seen, duplicates
    bases: dict[str, int] = defaultdict(int)
    models: dict[str, int] = defaultdict(int)
    domains: dict[str, int] = defaultdict(int)
    for node in data["nodes"]:
        try:
            node_id = node["nodeinfo"]["node_id"]
            if already_seen(node_id):
                continue
            base = node["nodeinfo"]["software"]["firmware"]["base"]
            match = VERSION_PATTERN.match(base)
            if match:
                bases[match.group("version")] += 1
            model = normalize_model_name(node["nodeinfo"]["hardware"]["model"])
            models[model] += 1
            domain = node["nodeinfo"]["system"]["domain_code"]
            domains[domain] += 1
        except KeyError as ex:
            continue

    return bases, models, domains


register_hook("meshviewer", SCHEMA_MESHVIEWER, parse_meshviewer)
register_hook("meshviewer (old)", SCHEMA_MESHVIEWER_OLD, parse_meshviewer)
register_hook("nodes.json v1", SCHEMA_NODESJSONV1, parse_nodes_json_v1)
register_hook("nodes.json v2", SCHEMA_NODESJSONV2, parse_nodes_json_v2)


def download(url: str, timeout: float = 5) -> requests.models.Response:
    try:
        response = requests.get(url, timeout=timeout)
    except requests.exceptions.RequestException as ex:
        log.msg("Exception caught while fetching url", ex=ex)
        raise ex

    if response.status_code != 200:
        log.msg(
            "Unexpected HTTP status code",
            status_code=response.status_code,
            url=url,
        )
        msg = "Unexpected HTTP status code"
        raise RuntimeError(msg)

    return response


def load(url: str) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    response = download(url)
    if not response:
        msg = "No response for HTTP request"
        raise ValueError(msg)

    try:
        data = response.json()
    except ValueError as ex:
        log.msg("Exception caught while processing url", url=url, ex=ex)
        raise ex

    for name, format_set in FORMATS.items():
        try:
            format_set.schema(data)
            print(f"{name}\t{url}")
            return format_set.parser(data)
        except (Invalid, MultipleInvalid) as ex:
            pass

    msg = "No parser found"
    raise ValueError(msg)


def named_load(
    name_url_tuple: tuple[str, str],
) -> tuple[str, tuple[dict[str, int], dict[str, int], dict[str, int]] | None]:
    community_name, url = name_url_tuple
    try:
        result = load(url)
    except KeyboardInterrupt:
        import sys

        sys.exit(1)
    except Exception:  # noqa: BLE001
        return (community_name, None)
    return (community_name, result)


@click.command(short_help="Collect census information")
@click.argument("outfile", default="./gluon-census.prom")
def main(outfile: str) -> None:
    registry = CollectorRegistry()
    metric_gluon_version_total = Gauge(
        "gluon_base_total",
        "Number of unique nodes running on a certain Gluon base version",
        ["community", "base", "version"],
        registry=registry,
    )
    metric_gluon_model_total = Gauge(
        "gluon_model_total",
        "Number of unique nodes using a certain device model",
        ["community", "model"],
        registry=registry,
    )
    metric_gluon_domain_total = Gauge(
        "gluon_domain_total",
        "Number of unique nodes on a specific Gluon domain",
        ["community", "domain"],
        registry=registry,
    )

    with open("./communities.json") as handle:
        communities = json.load(handle)

    fetchlist = [
        (community, url) for community, urls in communities.items() for url in urls
    ]

    results = ThreadPool(16).imap_unordered(named_load, fetchlist)

    for community, result in results:
        try:
            if result is None:
                continue
            versions, models, domains = result
        except TypeError:
            continue
        for version, version_sum in versions.items():
            match = BASE_PATTERN.match(version)
            if match is None:
                msg = "Could not match version"
                raise ValueError(msg)
            base = match.group("base")
            metric_gluon_version_total.labels(
                community=community,
                version=version,
                base=base,
            ).inc(version_sum)
        for model, model_sum in models.items():
            metric_gluon_model_total.labels(community=community, model=model).inc(
                model_sum,
            )
        for domain, domain_sum in domains.items():
            metric_gluon_domain_total.labels(community=community, domain=domain).inc(
                domain_sum,
            )

    write_to_textfile(outfile, registry)

    print(len(seen), "unique nodes")
    print(duplicates, "duplicates skipped")


if __name__ == "__main__":
    main()
