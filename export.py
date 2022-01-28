import io
import json
from typing import Dict
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry

# * build standard FtM graph
# * reify most node types: countries, phones, emails, etc.
# * turn some properties into labels: topics, offshore
# * generate CSV files
# * fill out template for CSV files

TYPES_INLINE = (
    registry.name,
    registry.date,
)

TYPES_REIFY = (
    registry.name,
    registry.country,
    registry.iban,
    registry.email,
    registry.phone,
    registry.identifier,
)


class Label(object):
    def __init__(self, label, columns, extra_labels=None):
        self.label = label
        self.columns = columns
        self.extra_labels = extra_labels


value_nodes = set()
files: Dict[str, io.TextIOWrapper] = {}


def handle_node_proxy(proxy: EntityProxy):
    return
    row = {"id": proxy.id, "caption": proxy.caption}
    for prop in proxy.schema.sorted_properties:
        if prop.hidden:
            continue
        if prop.type.matchable and not prop.matchable:
            continue
        values = proxy.get(prop)
        if prop.name in proxy.schema.featured:
            full_value = prop.type.join(values)
            row[prop.name] = full_value

        # TODO: reify value nodes

    print(row)
    # if prop.type in TYPES_REIFY:
    #     pass


def handle_edge_proxy(proxy: EntityProxy):
    sources = proxy.get(proxy.schema.source_prop)
    assert len(sources) == 1
    targets = proxy.get(proxy.schema.target_prop)
    assert len(targets) == 1
    row = {"source_id": sources[0], "target_id": targets[0], "caption": proxy.caption}
    for prop_name in proxy.schema.featured:
        prop = proxy.schema.get(prop_name)
        if prop is None:
            continue
        if prop == proxy.schema.source_prop:
            continue
        if prop == proxy.schema.target_prop:
            continue
        value = prop.type.join(proxy.get(prop))
        row[prop.name] = value
    print(row)


def handle_entity(proxy: EntityProxy):
    if proxy.schema.edge:
        handle_edge_proxy(proxy)
    else:
        handle_node_proxy(proxy)


def read_entity_file(file_path):
    with io.open(file_path) as fh:
        while line := fh.readline():
            raw = json.loads(line)
            proxy = model.get_proxy(raw)
            handle_entity(proxy)


if __name__ == "__main__":
    read_entity_file("/Users/pudo/Data/entities.ftm.json")
