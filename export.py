import io
import csv
import json
import stringcase
from typing import Dict
from pathlib import Path
from pprint import pprint
from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry
from followthemoney.types.common import PropertyType

# * build standard FtM graph
# * reify most node types: countries, phones, emails, etc.
# * turn some properties into labels: topics, offshore
# * generate CSV files
# * fill out template for CSV files

export_path = Path("data/exports").resolve()
export_path.mkdir(exist_ok=True, parents=True)


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


class LabelWriter(object):
    def __init__(self, label, columns, extra_labels=None):
        self.label = label
        self.columns = columns
        self.extra_labels = extra_labels
        self.seen_ids = set()

        file_name = export_path.joinpath(f"%s.csv" % label)
        self.fh = open(file_name, "w")
        self.writer = csv.DictWriter(self.fh, fieldnames=columns)
        self.writer.writeheader()

    def write(self, row):
        obj_id = row.get("id")
        if obj_id is None:
            obj_id = (row["source_id"], row["target_id"])
        if obj_id in self.seen_ids:
            return
        self.seen_ids.add(obj_id)
        self.writer.writerow(row)

    def close(self):
        self.fh.close()


writers: Dict[str, LabelWriter] = {}


def emit_label_row(row, label, extra_labels=None):
    if label not in writers:
        columns = list(row.keys())
        writer = LabelWriter(label, columns, extra_labels=extra_labels)
        writers[label] = writer
    writers[label].write(row)


def handle_node_value(proxy: EntityProxy, type: PropertyType, value: str):
    node_id = type.node_id_safe(value)
    if node_id is None:
        return
    node_row = {"id": node_id, "caption": type.caption(value)}
    emit_label_row(node_row, type.name)

    link_row = {"source_id": proxy.id, "target_id": node_id}
    emit_label_row(link_row, stringcase.constcase(type.name))


def handle_node_proxy(proxy: EntityProxy):
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
        if prop.type in TYPES_REIFY:
            for value in values:
                handle_node_value(proxy, prop.type, value)

    schemata = [s for s in proxy.schema.schemata if not s.abstract]
    extra_labels = [s.name for s in schemata if s != proxy.schema]
    emit_label_row(row, proxy.schema.name, extra_labels=extra_labels)

    # TODO: make topics into extra labels
    topics = proxy.get_type_values(registry.topic)
    for topic in topics:
        topic_label = stringcase.pascalcase(registry.topic.caption(topic))
        if topic_label is None:
            continue
        topic_row = {"id": proxy.id, "caption": proxy.caption}
        emit_label_row(topic_row, topic_label)


def handle_edge_proxy(proxy: EntityProxy):
    sources = proxy.get(proxy.schema.source_prop)
    assert len(sources) == 1
    targets = proxy.get(proxy.schema.target_prop)
    assert len(targets) == 1
    row = {
        # "id": proxy.id,
        "source_id": sources[0],
        "target_id": targets[0],
        "caption": proxy.caption,
    }
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
    label = stringcase.constcase(proxy.schema.name)
    emit_label_row(row, label)


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

    for writer in writers.values():
        writer.close()


if __name__ == "__main__":
    read_entity_file("/Users/pudo/Data/entities.ftm.json")
