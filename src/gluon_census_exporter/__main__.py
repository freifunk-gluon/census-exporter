#!/usr/bin/env python3

from __future__ import annotations

import json
import operator
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from functools import reduce
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

import click
import requests
import structlog
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from voluptuous import Invalid, MultipleInvalid, Schema

if TYPE_CHECKING:
    from collections.abc import Callable


log = structlog.get_logger()


class PatternDef(TypedDict):
    version: re.Pattern[str]
    base: re.Pattern[str]
    vtype: str


PATTERNS: list[PatternDef] = [
    {
        "version": re.compile(r"^(?P<version>gluon-v\d{4}\.\d(?:\.\d)?(?:-\d+)?).*"),
        "base": re.compile(r"^(?P<base>gluon-v\d{4}\.\d(?:\.\d)?).*"),
        "vtype": "gluon-base",
    },
    {
        "version": re.compile(r"^(?P<version>gluon-unknown)$"),
        "base": re.compile(r"^(?P<base>gluon-unknown)$"),
        "vtype": "gluon-unknown",
    },
    {
        "version": re.compile(r"^(?P<version>gluon-[0-9a-f]{7,})$"),
        "base": re.compile(r"^(?P<base>gluon-[0-9a-f]{7,})$"),
        "vtype": "gluon-commitid",
    },
    {
        "version": re.compile(r"^(?P<version>gluon-.*)"),
        "base": re.compile(r"^(?P<base>gluon-.*)"),
        "vtype": "gluon-custom",
    },
    {
        "version": re.compile(r"^(?P<version>)$"),
        "base": re.compile(r"^(?P<base>)$"),
        "vtype": "undefined",
    },
    {
        "version": re.compile(r"^(?P<version>.*)"),
        "base": re.compile(r"^(?P<base>.*)"),
        "vtype": "foreign",
    },
]


def get_version(pattern: str) -> str:
    if pattern is None:
        return ""
    for pidx in PATTERNS:
        match = pidx["version"].match(pattern)
        if match:
            return match.group("version")
    return ""


def get_base_version(pattern: str) -> tuple[str, str]:
    if pattern is None:
        return ("", "undefined")
    for pidx in PATTERNS:
        match = pidx["base"].match(pattern)
        if match:
            res = match.group("base")
            return (res, pidx["vtype"])
    return ("", "undefined")


seen = set()
total_version_sum = 0
total_alien_sum = 0
total_model_sum = 0
total_domain_sum = 0
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
class ParseResult:
    bases: defaultdict[str, int] = field(
        default_factory=lambda: defaultdict(int),
    )
    models: defaultdict[str, int] = field(
        default_factory=lambda: defaultdict(int),
    )
    domains: defaultdict[tuple[str, str], int] = field(
        default_factory=lambda: defaultdict(int),
    )


@dataclass
class Format:
    name: str
    schema: Schema
    parser: Callable[[dict], ParseResult]


def normalize_model_name(name: str) -> str:
    return re.sub(r"\s+", " ", name)


def register_hook(
    name: str,
    schema: Schema,
    parser: Callable[[dict], ParseResult],
) -> None:
    FORMATS[name] = Format(name=name, schema=schema, parser=parser)


def already_seen(node_id: str) -> bool:
    global duplicates, seen
    if node_id in seen:
        duplicates += 1
        return True
    seen.add(node_id)
    return False


def get_node_item(
    node: dict,
    keys: list[str] | None,
) -> str | None:
    if keys is None:
        return None
    try:
        value = reduce(operator.getitem, keys, node)
    except KeyError:
        return None
    if isinstance(value, str):
        return value
    return None


def parse_generic(
    node_id: str,
    node: dict,
    keys: dict[str, list[str] | None],
    result: ParseResult,
) -> None:
    if already_seen(node_id):
        return
    base = get_node_item(node, keys["base"])
    version = get_version(base)
    result.bases[version] += 1
    model = get_node_item(node, keys["model"])
    model = "" if model is None else normalize_model_name(model)
    result.models[model] += 1
    domain = get_node_item(node, keys["domain"])
    if domain is None:
        domain = ""
    site = get_node_item(node, keys["site"])
    if site is None:
        site = ""
    result.domains[(site, domain)] += 1


def parse_meshviewer(
    data: dict,
) -> ParseResult:
    result: ParseResult = ParseResult()
    for node in data["nodes"]:
        try:
            node_id = node["node_id"]
        except KeyError:
            continue
        keys: dict[str, list[str] | None] = {
            "base": ["firmware", "base"],
            "model": ["model"],
            "domain": ["domain"],
            "site": None,
        }
        parse_generic(node_id, node, keys, result)
    return result


def parse_nodes_json_v1(
    data: dict,
) -> ParseResult:
    result: ParseResult = ParseResult()
    for node_id, node in data["nodes"].items():
        keys: dict[str, list[str] | None] = {
            "base": ["nodeinfo", "software", "firmware", "base"],
            "model": None,
            "domain": None,
            "site": None,
        }
        parse_generic(node_id, node, keys, result)
    return result


def parse_nodes_json_v2(
    data: dict,
) -> ParseResult:
    result: ParseResult = ParseResult()
    for node in data["nodes"]:
        try:
            node_id = node["nodeinfo"]["node_id"]
        except KeyError:
            continue
        keys: dict[str, list[str] | None] = {
            "base": ["nodeinfo", "software", "firmware", "base"],
            "model": ["nodeinfo", "hardware", "model"],
            "domain": ["nodeinfo", "system", "domain_code"],
            "site": ["nodeinfo", "system", "site_code"],
        }
        parse_generic(node_id, node, keys, result)
    return result


register_hook("meshviewer", SCHEMA_MESHVIEWER, parse_meshviewer)
register_hook("meshviewer (old)", SCHEMA_MESHVIEWER_OLD, parse_meshviewer)
register_hook("nodes.json v1", SCHEMA_NODESJSONV1, parse_nodes_json_v1)
register_hook("nodes.json v2", SCHEMA_NODESJSONV2, parse_nodes_json_v2)


def download(url: str, timeout: float = 5) -> requests.models.Response:
    try:
        response = requests.get(url, timeout=timeout)
    except requests.exceptions.RequestException as ex:
        log.msg("Exception caught while fetching url", ex=ex)
        raise

    if response.status_code != requests.codes.ok:
        log.msg(
            "Unexpected HTTP status code",
            status_code=response.status_code,
            url=url,
        )
        msg = "Unexpected HTTP status code"
        raise RuntimeError(msg)

    return response


def load(url: str) -> ParseResult:
    response = download(url)
    if not response:
        msg = "No response for HTTP request"
        raise ValueError(msg)

    try:
        data = response.json()
    except ValueError as ex:
        log.msg("Exception caught while processing url", url=url, ex=ex)
        raise

    for name, format_set in FORMATS.items():
        try:
            format_set.schema(data)
            log.msg("Processing", format=name, url=url)
            return format_set.parser(data)
        except (Invalid, MultipleInvalid):
            pass

    msg = "No parser found"
    raise ValueError(msg)


def named_load(
    name_url_tuple: tuple[str, str],
) -> tuple[str, ParseResult | None]:
    community_name, url = name_url_tuple
    try:
        result = load(url)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception:  # noqa: BLE001
        return (community_name, None)
    return (community_name, result)


def check_node_counts() -> None:
    if total_version_sum + total_alien_sum != len(seen):
        log.error(
            "Node count mismatch",
            unique=len(seen),
            version_sum=total_version_sum,
            alien_sum=total_alien_sum,
        )
    if total_model_sum != len(seen):
        log.error(
            "Model count mismatch",
            unique=len(seen),
            model_sum=total_model_sum,
        )
    if total_domain_sum != len(seen):
        log.error(
            "Domain count mismatch",
            unique=len(seen),
            domain_sum=total_domain_sum,
        )


@click.command(short_help="Collect census information")
@click.argument("outfile", default="./gluon-census.prom")
def main(outfile: str) -> None:
    global total_version_sum, total_alien_sum, total_model_sum, total_domain_sum
    registry = CollectorRegistry()
    metric_gluon_version_total = Gauge(
        "gluon_base_total",
        "Number of unique nodes running on a certain Gluon base version",
        ["community", "base", "version", "vtype"],
        registry=registry,
    )
    metric_gluon_alien_total = Gauge(
        "gluon_alien_total",
        "Number of unique nodes running on a non-Gluon version",
        ["community", "version", "vtype"],
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
        ["community", "site", "domain"],
        registry=registry,
    )

    with Path("./communities.json").open() as handle:
        communities = json.load(handle)

    fetchlist = [
        (community, url) for community, urls in communities.items() for url in urls
    ]

    results = ThreadPool(16).imap_unordered(named_load, fetchlist)

    for community, result in results:
        try:
            if result is None:
                continue
        except TypeError:
            continue
        for version, version_sum in result.bases.items():
            vbase, vtype = get_base_version(version)
            if vbase is None or vtype is None:
                msg = "Could not match version"
                raise ValueError(msg)
            if vtype == "undefined":
                vbase = "undefined"
                version = "undefined"
            if vtype in {"undefined", "foreign"}:
                metric_gluon_alien_total.labels(
                    community=community,
                    version=version,
                    vtype=vtype,
                ).inc(version_sum)
                total_alien_sum += version_sum
            else:
                metric_gluon_version_total.labels(
                    community=community,
                    version=version,
                    base=vbase,
                    vtype=vtype,
                ).inc(version_sum)
                total_version_sum += version_sum
        for model, model_sum in result.models.items():
            metric_gluon_model_total.labels(community=community, model=model).inc(
                model_sum,
            )
            total_model_sum += model_sum
        for (site, domain), domain_sum in result.domains.items():
            metric_gluon_domain_total.labels(
                community=community,
                site=site,
                domain=domain,
            ).inc(
                domain_sum,
            )
            total_domain_sum += domain_sum

    write_to_textfile(outfile, registry)

    log.msg(
        "Collections summaries",
        version_sum=total_version_sum,
        alien_sum=total_alien_sum,
        model_sum=total_model_sum,
        domain_sum=total_domain_sum,
    )
    log.msg("Summary", unique=len(seen), duplicate=duplicates)

    check_node_counts()


if __name__ == "__main__":
    main()
