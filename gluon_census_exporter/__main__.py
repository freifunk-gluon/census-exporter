#!/usr/bin/env python3

import json
import re
import sys
from collections import defaultdict

import click
import requests
import structlog
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from voluptuous import Invalid, MultipleInvalid, Schema

log = structlog.get_logger()

VERSION_PATTERN = re.compile(r"^(?P<version>gluon-v\d{4}\.\d(?:\.\d)?(?:-\d+)?).*")
BASE_PATTERN = re.compile(r"^(?P<base>gluon-v\d{4}\.\d(?:\.\d)?).*")
SEEN = set()
DUPLICATE = 0

# minimal schema definitions to recognize formats
SCHEMA_MESHVIEWER = Schema({"timestamp": str, "nodes": [dict], "links": [dict]})

SCHEMA_MESHVIEWER_OLD = Schema(
    {"meta": {"timestamp": str}, "nodes": [dict], "links": [dict]}
)

SCHEMA_NODESJSONV1 = Schema({"timestamp": str, "version": 1, "nodes": dict})

SCHEMA_NODESJSONV2 = Schema({"timestamp": str, "version": 2, "nodes": [dict]})

FORMATS = {}


def register_hook(name, schema, parser):
    FORMATS[name] = {"schema": schema, "parser": parser}


def normalize_model_name(name):
    return re.sub(r"\s+", " ", name)


def already_seen(node_id):
    global SEEN, DUPLICATE

    if node_id in SEEN:
        DUPLICATE += 1
        return True

    SEEN.add(node_id)

    return False


def parse_meshviewer(data):
    bases = defaultdict(int)
    models = defaultdict(int)

    for node in data["nodes"]:
        try:
            node_id = node["node_id"]

            if already_seen(node_id):
                continue

            base = node["firmware"]["base"]
            if match := VERSION_PATTERN.match(base):
                bases[match.group("version")] += 1

            model = normalize_model_name(node["model"])
            models[model] += 1
        except KeyError as _:
            continue

    return bases, models


def parse_nodes_json_v1(data):
    bases = defaultdict(int)

    for node_id, node in data["nodes"].items():
        if already_seen(node_id):
            continue

        try:
            base = node["nodeinfo"]["software"]["firmware"]["base"]
        except KeyError:
            continue

        if match := VERSION_PATTERN.match(base):
            bases[match.group("version")] += 1

    return bases, {}


def parse_nodes_json_v2(data):
    global SEEN, DUPLICATE
    bases = defaultdict(int)
    models = defaultdict(int)

    for node in data["nodes"]:
        try:
            node_id = node["nodeinfo"]["node_id"]

            if already_seen(node_id):
                continue

            base = node["nodeinfo"]["software"]["firmware"]["base"]
            if match := VERSION_PATTERN.match(base):
                bases[match.group("version")] += 1

            model = normalize_model_name(node["nodeinfo"]["hardware"]["model"])
            models[model] += 1
        except KeyError:
            continue

    return bases, models


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
        return None

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
            log.msg("Processing", format=name, url=url)
            return format["parser"](data)
        except (Invalid, MultipleInvalid):
            pass

    raise ValueError("No parser found")


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

    with open("./communities.json") as handle:
        communities = json.load(handle)

    for community, urls in communities.items():
        for url in urls:
            try:
                versions, models = load(url)
            except KeyboardInterrupt:
                sys.exit(1)
            except BaseException:
                continue

            for version, sum in versions.items():
                if match := BASE_PATTERN.match(version):
                    base = match.group("base")
                    metric_gluon_version_total.labels(
                        community=community, version=version, base=base
                    ).set(sum)

            for model, sum in models.items():
                metric_gluon_model_total.labels(community=community, model=model).set(
                    sum
                )

    write_to_textfile(outfile, registry)

    log.msg("Summary", unique=len(SEEN), duplicate=DUPLICATE)


if __name__ == "__main__":
    main()
