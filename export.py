from cProfile import label
import io
import csv
import json
import stringcase
from typing import Dict, List, Optional, Set, Tuple, Union
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

ENTITY_LABEL = "Entity"

export_path = Path("data/exports").resolve()
export_path.mkdir(exist_ok=True, parents=True)

public_prefix = "http://localhost:9999/exports"


TYPES_INLINE = (
    registry.name,
    registry.date,
    registry.identifier,
    registry.country,
)

TYPES_REIFY = (
    registry.name,
    # registry.country,
    registry.iban,
    registry.email,
    registry.phone,
    registry.identifier,
)


class LabelWriter(object):
    def __init__(
        self,
        label: str,
        columns: List[str],
        is_edge: bool = False,
        node_label: Optional[str] = None,
        source_label: Optional[str] = None,
        target_label: Optional[str] = None,
        extra_labels: Optional[List[str]] = None,
    ):
        self.label = label
        self.columns = columns
        self.is_edge = is_edge
        self.extra_labels = extra_labels
        self.node_label = node_label
        self.source_label = source_label
        self.target_label = target_label
        self.seen_ids: Set[str] = set()
        self.file_name = f"%s.csv" % label

        file_path = export_path.joinpath(self.file_name)
        self.fh = open(file_path, "w")
        self.writer = csv.DictWriter(self.fh, fieldnames=columns)
        self.writer.writeheader()

    def write(self, row: Dict[str, str]):
        if self.is_edge:
            obj_id = f"{row['source_id']}->{row['target_id']}"
        else:
            obj_id = row["id"]
        if obj_id in self.seen_ids:
            return
        self.seen_ids.add(obj_id)
        self.writer.writerow(row)

    def close(self):
        self.fh.close()

    def get_all_labels(self, ref):
        label_names = [self.label]
        if self.extra_labels is not None:
            label_names.extend(self.extra_labels)
        if self.node_label in label_names:
            label_names.remove(self.node_label)
        if not len(label_names):
            return ""
        labels = ":".join(label_names)
        return f"SET {ref}:{labels}"

    def get_setters(self, ref):
        setters = []
        for column in self.columns:
            if column in ["id", "source_id", "target_id"]:
                continue
            setter = f"SET {ref}.{column} = row.{column}"
            setters.append(setter)
        return " ".join(setters)

    def to_node_load(self, prefix):
        labels = self.get_all_labels("n")
        setters = self.get_setters("n")
        node_label = f":{self.node_label}" if self.node_label else ""
        return f"""LOAD CSV WITH HEADERS FROM '{prefix}/{self.file_name}' AS row
            WITH row WHERE row.id IS NOT NULL
            MERGE (n{node_label} {'{'}id: row.id{'}'})
            {setters}
            {labels};"""

    def to_edge_load(self, prefix):
        setters = self.get_setters("r")
        source_label = f":{self.source_label}" if self.source_label else ""
        target_label = f":{self.target_label}" if self.target_label else ""
        return f"""LOAD CSV WITH HEADERS FROM '{prefix}/{self.file_name}' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            MERGE (s{source_label} {'{'}id: row.source_id{'}'})
            MERGE (t{target_label} {'{'}id: row.target_id{'}'})
            MERGE (s)-[r:{self.label}]->(t)
            {setters};"""


writers: Dict[str, LabelWriter] = {}


def emit_label_row(row, label, **kwargs):
    if label not in writers:
        columns = list(row.keys())
        writer = LabelWriter(label, columns, **kwargs)
        writers[label] = writer
    writers[label].write(row)


def handle_node_value(proxy: EntityProxy, type: PropertyType, value: str):
    node_id = type.node_id_safe(value)
    if node_id is None:
        return
    node_row = {"id": node_id, "caption": type.caption(value)}
    emit_label_row(node_row, type.name, node_label=type.name)

    link_row = {"source_id": proxy.id, "target_id": node_id}
    link_label = stringcase.constcase(type.name)
    link_label = f"HAS_{link_label}"
    emit_label_row(
        link_row,
        link_label,
        is_edge=True,
        source_label=ENTITY_LABEL,
        target_label=type.name,
    )


def handle_node_proxy(proxy: EntityProxy):
    row = {"id": proxy.id, "caption": proxy.caption}
    featured = proxy.schema.featured
    for prop in proxy.schema.sorted_properties:
        if prop.hidden:
            continue
        if prop.type.matchable and not prop.matchable:
            continue
        values = proxy.get(prop)
        if prop.name in featured or prop.type in TYPES_INLINE:
            full_value = prop.type.join(values)
            row[prop.name] = full_value

        if prop.type in TYPES_REIFY:
            for value in values:
                handle_node_value(proxy, prop.type, value)

        # TODO: make plain entity links
        if prop.type == registry.entity:
            for value in values:
                link_row = {"source_id": proxy.id, "target_id": value}
                link_label = stringcase.constcase(prop.name)
                emit_label_row(
                    link_row,
                    link_label,
                    is_edge=True,
                    source_label=ENTITY_LABEL,
                    target_label=ENTITY_LABEL,
                )

    schemata = [s for s in proxy.schema.schemata if not s.abstract]
    extra_labels = [s.name for s in schemata if s != proxy.schema]
    extra_labels.append(ENTITY_LABEL)
    emit_label_row(
        row,
        proxy.schema.name,
        extra_labels=extra_labels,
        node_label=ENTITY_LABEL,
    )

    # TODO: make topics into extra labels
    topics = proxy.get_type_values(registry.topic)
    for topic in topics:
        topic_label = registry.topic.caption(topic)
        if topic_label is None:
            continue
        topic_label = topic_label.replace(" ", "_")
        topic_label = stringcase.pascalcase(topic_label)
        if topic_label is None:
            continue
        topic_row = {"id": proxy.id, "caption": proxy.caption}
        emit_label_row(
            topic_row, topic_label, extra_labels=[ENTITY_LABEL], node_label=ENTITY_LABEL
        )


def handle_edge_proxy(proxy: EntityProxy):
    source_prop = proxy.schema.source_prop
    if source_prop is None:
        return
    target_prop = proxy.schema.target_prop
    if target_prop is None:
        return

    sources = proxy.get(source_prop)
    targets = proxy.get(target_prop)
    for source in sources:
        for target in targets:
            row = {
                # "id": proxy.id,
                "source_id": source,
                "target_id": target,
                "caption": proxy.caption,
            }
            for prop_name in proxy.schema.featured:
                prop = proxy.schema.get(prop_name)
                if prop is None:
                    continue
                if prop == source_prop or prop == target_prop:
                    continue
                value = prop.type.join(proxy.get(prop))
                row[prop.name] = value
            label = stringcase.constcase(proxy.schema.name)
            emit_label_row(
                row,
                label,
                is_edge=True,
                source_label=ENTITY_LABEL,
                target_label=ENTITY_LABEL,
            )


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

    load_script = export_path.joinpath("load.cypher")
    with open(load_script, "w") as fh:
        fh.write("MATCH (n) DETACH DELETE n;\n")
        fh.write(f"CREATE INDEX IF NOT EXISTS FOR(n:{ENTITY_LABEL}) ON (n.id);\n")
        for type in TYPES_REIFY:
            fh.write(f"CREATE INDEX IF NOT EXISTS FOR(n:{type.name}) ON (n.id);\n")

        for writer in writers.values():
            if not writer.is_edge:
                load = writer.to_node_load(public_prefix)
                fh.write(load)
                fh.write("\n")

        for writer in writers.values():
            if writer.is_edge:
                load = writer.to_edge_load(public_prefix)
                fh.write(load)
                fh.write("\n")

        # TODO: prune useless nodes and labels
        # for type in TYPES_REIFY:
        #     fh.write(
        #         f"MATCH (n:{type.name}) WHERE size((n)--()) <= 1 DETACH DELETE (n);"
        #     )
        # fh.write(f"MATCH (n:{ENTITY_LABEL}) REMOVE n:{ENTITY_LABEL};")


if __name__ == "__main__":
    read_entity_file("data/full.json")
