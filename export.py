import io
import csv
import json
from os import PathLike
import click
import logging
import stringcase
from normality import collapse_spaces
from typing import Dict, List, Optional, Set
from pathlib import Path
from followthemoney import model
from followthemoney.proxy import EntityProxy
from followthemoney.types import registry
from followthemoney.types.common import PropertyType

log = logging.getLogger("make_graph")

ENTITY_LABEL = "Entity"


TYPES_INLINE = (
    registry.name,
    registry.date,
    registry.identifier,
    registry.country,
)

TYPES_REIFY = (
    registry.name,
    # registry.country,
    # registry.iban,
    registry.email,
    registry.phone,
    registry.identifier,
)


class LabelWriter(object):
    def __init__(
        self,
        export_path: PathLike,
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
        file_prefix = "edge" if is_edge else "node"
        self.file_name = f"{file_prefix}_{label}.csv"

        file_path = export_path.joinpath(self.file_name)
        self.fh = open(file_path, "w")
        self.writer = csv.DictWriter(
            self.fh,
            fieldnames=columns,
            dialect=csv.unix_dialect,
            escapechar="\\",
            doublequote=False,
        )
        self.writer.writeheader()

    def write(self, row: Dict[str, str]):
        cleaned: Dict[str, str] = {}
        for key, value in row.items():
            value = value.strip("\\")
            value = value.replace("\0", "")
            value = collapse_spaces(value)
            if value is None:
                value = ""
            if key in ("id", "source_id", "target_id"):
                value = value[:1000]
            else:
                value = value[:5000]
            cleaned[key] = value
        if self.is_edge:
            obj_id = f"{cleaned['source_id']}->{cleaned['target_id']}"
        else:
            obj_id = cleaned["id"]
        if obj_id in self.seen_ids:
            return
        self.seen_ids.add(obj_id)
        if len(self.seen_ids) % 10000 == 0:
            log.info("[%s] %d rows written...", self.file_name, len(self.seen_ids))
        self.writer.writerow(cleaned)

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
        return f""":auto USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM '{prefix}/{self.file_name}' AS row
            WITH row WHERE row.id IS NOT NULL
            MERGE (n{node_label} {'{'}id: row.id{'}'})
            {setters}
            {labels};"""

    def to_edge_load(self, prefix):
        setters = self.get_setters("r")
        source_label = f":{self.source_label}" if self.source_label else ""
        target_label = f":{self.target_label}" if self.target_label else ""
        return f""":auto USING PERIODIC COMMIT 50000
            LOAD CSV WITH HEADERS FROM '{prefix}/{self.file_name}' AS row
            WITH row WHERE row.source_id IS NOT NULL AND row.target_id IS NOT NULL
            MERGE (s{source_label} {'{'}id: row.source_id{'}'})
            MERGE (t{target_label} {'{'}id: row.target_id{'}'})
            MERGE (s)-[r:{self.label}]->(t)
            {setters};"""


class GraphExporter(object):
    def __init__(self, export_path: PathLike):
        self.export_path = export_path
        self.writers: Dict[str, LabelWriter] = {}

    def emit_label_row(self, row, label, **kwargs):
        if label not in self.writers:
            columns = list(row.keys())
            writer = LabelWriter(self.export_path, label, columns, **kwargs)
            self.writers[label] = writer
        self.writers[label].write(row)

    def handle_node_value(self, proxy: EntityProxy, type: PropertyType, value: str):
        # filter out short identifiers?
        if type == registry.identifier and len(value) < 7:
            return

        node_id = type.node_id_safe(value)
        if node_id is None:
            return
        node_row = {"id": node_id, "caption": type.caption(value)}
        self.emit_label_row(node_row, type.name, node_label=type.name)

        link_row = {"source_id": proxy.id, "target_id": node_id}
        link_label = stringcase.constcase(type.name)
        link_label = f"HAS_{link_label}"
        self.emit_label_row(
            link_row,
            link_label,
            is_edge=True,
            source_label=ENTITY_LABEL,
            target_label=type.name,
        )

    def handle_node_proxy(self, proxy: EntityProxy):
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
                    self.handle_node_value(proxy, prop.type, value)

            # TODO: make plain entity links
            if prop.type == registry.entity:
                for value in values:
                    link_row = {"source_id": proxy.id, "target_id": value}
                    link_label = stringcase.constcase(prop.name)
                    self.emit_label_row(
                        link_row,
                        link_label,
                        is_edge=True,
                        source_label=ENTITY_LABEL,
                        target_label=ENTITY_LABEL,
                    )

        schemata = [s for s in proxy.schema.schemata if not s.abstract]
        extra_labels = [s.name for s in schemata if s != proxy.schema]
        extra_labels.append(ENTITY_LABEL)
        self.emit_label_row(
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
            # Work-around to name overlap
            if topic == "role.rca":
                topic_label = "CloseAssociate"
            if topic_label is None:
                continue
            topic_row = {"id": proxy.id, "caption": proxy.caption}
            self.emit_label_row(
                topic_row,
                topic_label,
                extra_labels=[ENTITY_LABEL],
                node_label=ENTITY_LABEL,
            )

    def handle_edge_proxy(self, proxy: EntityProxy):
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
                if source == target:
                    continue
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
                self.emit_label_row(
                    row,
                    label,
                    is_edge=True,
                    source_label=ENTITY_LABEL,
                    target_label=ENTITY_LABEL,
                )

    def handle_entity(self, proxy: EntityProxy):
        if proxy.schema.edge:
            self.handle_edge_proxy(proxy)
        else:
            self.handle_node_proxy(proxy)

    def read_entity_file(self, file_path):
        with io.open(file_path) as fh:
            while line := fh.readline():
                raw = json.loads(line)
                proxy = model.get_proxy(raw)
                self.handle_entity(proxy)

    def close_writers(self):
        for writer in self.writers.values():
            writer.close()

    def write_load_script(self, public_prefix):
        load_script = self.export_path.joinpath("load.cypher")
        with open(load_script, "w") as fh:
            # fh.write("MATCH (n) DETACH DELETE n;\n")
            fh.write(f"CREATE INDEX IF NOT EXISTS FOR(n:{ENTITY_LABEL}) ON (n.id);\n")
            for type in TYPES_REIFY:
                fh.write(f"CREATE INDEX IF NOT EXISTS FOR(n:{type.name}) ON (n.id);\n")

            for writer in self.writers.values():
                if not writer.is_edge:
                    load = writer.to_node_load(public_prefix)
                    fh.write(load)
                    fh.write("\n")

            for writer in self.writers.values():
                if writer.is_edge:
                    load = writer.to_edge_load(public_prefix)
                    fh.write(load)
                    fh.write("\n")

            # prune useless nodes and labels
            for type in TYPES_REIFY:
                fh.write(
                    f"MATCH (n:{type.name}) WHERE size((n)--()) <= 1 "
                    "DETACH DELETE (n);\n"
                )
            # fh.write(f"MATCH (n:{ENTITY_LABEL}) REMOVE n:{ENTITY_LABEL};")


@click.command()
@click.option(
    "-o",
    "--out-path",
    default="data/exports",
    type=click.Path(writable=True, file_okay=False),
)
@click.option("-p", "--prefix", default="http://localhost:9999/exports", type=str)
@click.argument("source_files", nargs=-1, type=click.Path(exists=True, file_okay=True))
def make_graph(out_path, prefix, source_files):
    logging.basicConfig(level=logging.INFO)
    export_path = Path(out_path).resolve()
    export_path.mkdir(exist_ok=True, parents=True)

    exporter = GraphExporter(export_path)
    for source_file in source_files:
        exporter.read_entity_file(source_file)
    exporter.close_writers()
    exporter.write_load_script(prefix)


if __name__ == "__main__":
    make_graph()
