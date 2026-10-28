"""Offline ADF configuration integrity checks; no Azure credentials required.

These tests guard against broken activity dependency graphs and dangling
dataset/linked-service references before publishing JSON to Data Factory.
They do not execute pipelines or validate runtime SQL expressions.
"""

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_definitions(folder):
    definitions = {}
    for path in sorted((ROOT / folder).glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        name = document["name"]
        if name in definitions:
            raise AssertionError(f"duplicate {folder} definition: {name}")
        if path.stem != name:
            raise AssertionError(f"{path} does not match declared name {name}")
        definitions[name] = document
    return definitions


def walk_nodes(value):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from walk_nodes(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from walk_nodes(nested)


def validate_activity_scope(test, activities, scope):
    names = [activity["name"] for activity in activities]
    test.assertEqual(len(names), len(set(names)), f"{scope}: duplicate activity name")
    known = set(names)
    edges = {}
    for activity in activities:
        name = activity["name"]
        dependencies = activity.get("dependsOn", [])
        test.assertIsInstance(dependencies, list, f"{scope}/{name}: dependsOn must be a list")
        refs = []
        for dependency in dependencies:
            target = dependency["activity"]
            test.assertIn(target, known, f"{scope}/{name}: unknown dependency {target}")
            test.assertTrue(
                dependency.get("dependencyConditions"),
                f"{scope}/{name}: empty dependency conditions",
            )
            refs.append(target)
        edges[name] = refs

        props = activity.get("typeProperties", {})
        for nested_key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            if nested_key in props:
                validate_activity_scope(
                    test, props[nested_key], f"{scope}/{name}/{nested_key}"
                )

    # ADF dependencies form a DAG; cycles would leave activities unschedulable.
    visiting = set()
    visited = set()

    def visit(name):
        if name in visiting:
            raise AssertionError(f"{scope}: cyclic dependency involving {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in edges[name]:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)

    for name in names:
        visit(name)


class DataFactoryConfigurationIntegrityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pipelines = load_definitions("pipeline")
        cls.datasets = load_definitions("dataset")
        cls.services = load_definitions("linkedService")

    def test_pipeline_activity_graphs_are_resolvable_and_acyclic(self):
        self.assertTrue(self.pipelines)
        for name, document in self.pipelines.items():
            with self.subTest(pipeline=name):
                validate_activity_scope(
                    self, document["properties"]["activities"], name
                )

    def test_dataset_and_linked_service_references_exist(self):
        for folder, definitions in (
            ("pipeline", self.pipelines),
            ("dataset", self.datasets),
        ):
            for name, document in definitions.items():
                with self.subTest(folder=folder, name=name):
                    for node in walk_nodes(document):
                        if node.get("type") == "DatasetReference":
                            self.assertIn(node["referenceName"], self.datasets)
                        if node.get("type") == "LinkedServiceReference":
                            self.assertIn(node["referenceName"], self.services)

    def test_dataset_references_supply_required_parameters(self):
        for name, pipeline in self.pipelines.items():
            with self.subTest(pipeline=name):
                for node in walk_nodes(pipeline):
                    if node.get("type") != "DatasetReference":
                        continue
                    target = self.datasets[node["referenceName"]]
                    required = {
                        key for key, definition in
                        target["properties"].get("parameters", {}).items()
                        if "defaultValue" not in definition
                    }
                    supplied = set(node.get("parameters", {}))
                    self.assertFalse(
                        required - supplied,
                        f"{name}: {node['referenceName']} missing parameters "
                        f"{sorted(required - supplied)}",
                    )


if __name__ == "__main__":
    unittest.main()
