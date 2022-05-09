#!/usr/bin/env python3

import json
import re
from collections import defaultdict
from functools import reduce
from multiprocessing.pool import ThreadPool
from operator import getitem

import click
import requests
import structlog
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from voluptuous import Invalid, MultipleInvalid, Schema

log = structlog.get_logger()

version_pattern = re.compile(r"^(?P<version>gluon-v\d{4}\.\d(?:\.\d)?(?:-\d+)?).*")
base_pattern = re.compile(r"^(?P<base>gluon-v\d{4}\.\d(?:\.\d)?).*")
seen = set()
duplicates = 0


# minimal schema definitions to recognize formats
SCHEMA_MESHVIEWER = Schema({"timestamp": str, "nodes": [dict], "links": [dict]})

SCHEMA_MESHVIEWER_OLD = Schema(
    {"meta": {"timestamp": str}, "nodes": [dict], "links": [dict]}
)

SCHEMA_NODESJSONV1 = Schema({"timestamp": str, "version": 1, "nodes": dict})

SCHEMA_NODESJSONV2 = Schema({"timestamp": str, "version": 2, "nodes": [dict]})

FORMATS = {}


def normalize_model_name(name):
    return re.sub(r"\s+", " ", name)


def register_hook(name, schema, parser):
    FORMATS[name] = {"schema": schema, "parser": parser}


def parse_meshviewer(data):
    global seen, duplicates
    bases = defaultdict(int)
    models = defaultdict(int)
    domains = defaultdict(int)
    for node in data["nodes"]:
        try:
            node_id = node["node_id"]
            if node_id in seen:
                duplicates += 1
                continue
            base = node["firmware"]["base"]
            seen.add(node_id)
            match = version_pattern.match(base)
            if match:
                bases[match.group("version")] += 1
            model = normalize_model_name(node["model"])
            models[model] += 1
            domain = node["domain"]
            domains[domain] += 1
        except KeyError as ex:
            continue
    return bases, models, domains


def parse_nodes_json_v1(data, *kwargs):
    global seen, duplicates
    bases = defaultdict(int)
    for node_id, node in data["nodes"].items():
        if node_id in seen:
            duplicates += 1
            continue
        try:
            base = node["nodeinfo"]["software"]["firmware"]["base"]
        except KeyError as ex:
            continue
        seen.add(node_id)
        match = version_pattern.match(base)
        if match:
            bases[match.group("version")] += 1
    return bases, dict(), dict()


def parse_nodes_json_v2(data, *kwargs):
    global seen, duplicates
    bases = defaultdict(int)
    models = defaultdict(int)
    domains = defaultdict(int)
    for node in data["nodes"]:
        try:
            node_id = node["nodeinfo"]["node_id"]
            if node_id in seen:
                duplicates += 1
                continue
            base = node["nodeinfo"]["software"]["firmware"]["base"]
            seen.add(node_id)
            match = version_pattern.match(base)
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


def download(url, timeout=5):
    try:
        response = requests.get(url, timeout=timeout)
    except requests.exceptions.RequestException as ex:
        log.msg("Exception caught while fetching url", ex=ex)
        raise ex

    if response.status_code != 200:
        log.msg(
            "Unexpected HTTP status code", status_code=response.status_code, url=url
        )
        raise ex

    return response


def load(url):
    response = download(url)
    if not response:
        raise ValueError("No response for HTTP request")

    try:
        data = response.json()
    except ValueError as ex:
        log.msg("Exception caught while processing url", url=url, ex=ex)
        raise ex

    for name, format in FORMATS.items():
        try:
            format["schema"](data)
            print(f"{name}\t{url}")
            return format["parser"](data)
        except (Invalid, MultipleInvalid) as ex:
            pass

    raise ValueError("No parser found")


def named_load(name_url_tuple):
    community_name, url = name_url_tuple
    try:
        result = load(url)
    except KeyboardInterrupt:
        import sys

        sys.exit(1)
    except BaseException as ex:
        return (community_name, None)
    return (community_name, result)


@click.command(short_help="Collect census information")
@click.argument("outfile", default="./gluon-census.prom")
def main(outfile):
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
            versions, models, domains = result
        except TypeError:
            continue
        for version, sum in versions.items():
            match = base_pattern.match(version)
            base = match.group("base")
            metric_gluon_version_total.labels(
                community=community, version=version, base=base
            ).inc(sum)
        for model, sum in models.items():
            metric_gluon_model_total.labels(community=community, model=model).inc(sum)
        for domain, sum in domains.items():
            metric_gluon_domain_total.labels(community=community, domain=domain).inc(
                sum
            )

    write_to_textfile(outfile, registry)

    print(len(seen), "unique nodes")
    print(duplicates, "duplicates skipped")


if __name__ == "__main__":
    main()
