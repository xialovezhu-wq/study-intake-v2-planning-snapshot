import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PLUGIN_SCHEMAS = ROOT / "plugin" / "kaoyan-study-intake" / "schemas"
RUNTIME_SCHEMAS = ROOT / "schemas"


def load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def local_refs(value: object) -> list[str]:
    refs: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "$ref" and isinstance(item, str):
                refs.append(item)
            refs.extend(local_refs(item))
    elif isinstance(value, list):
        for item in value:
            refs.extend(local_refs(item))
    return refs


def resolve_local_ref(schema: dict[str, object], ref: str) -> object:
    if not ref.startswith("#/"):
        raise AssertionError(f"non-local schema ref: {ref}")
    current: object = schema
    for encoded in ref[2:].split("/"):
        token = encoded.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or token not in current:
            raise AssertionError(f"unresolved schema ref: {ref}")
        current = current[token]
    return current


class PackageSchemaContractTests(unittest.TestCase):
    def test_sol_authority_hardening_schemas_are_exact_runtime_contracts(self) -> None:
        for name in (
            "sol-commit-receipt-v1.json",
            "isolated-writer-artifacts-v1.json",
            "isolated-writer-execution-result-v1.json",
            "subject-authority-observation-v1.json",
            "subject-generation-authority-ack-v2.json",
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    (RUNTIME_SCHEMAS / name).read_bytes(),
                    (PLUGIN_SCHEMAS / name).read_bytes(),
                )

    def test_plugin_package_schemas_are_exact_runtime_contracts(self) -> None:
        for name in ("preprocess-package-v1.json", "preprocess-package-v2.json"):
            with self.subTest(name=name):
                self.assertEqual(
                    (RUNTIME_SCHEMAS / name).read_bytes(),
                    (PLUGIN_SCHEMAS / name).read_bytes(),
                )

    def test_package_schemas_embed_the_complete_proposal_contract(self) -> None:
        standalone = load(PLUGIN_SCHEMAS / "luna-proposal-v2.json")
        embedded_keys = (
            "type",
            "additionalProperties",
            "required",
            "allOf",
            "properties",
        )
        for name in ("preprocess-package-v1.json", "preprocess-package-v2.json"):
            package = load(RUNTIME_SCHEMAS / name)
            with self.subTest(name=name):
                self.assertEqual(
                    package["properties"]["luna_proposal"],
                    {"$ref": "#/$defs/lunaProposal"},
                )
                embedded = package["$defs"]["lunaProposal"]
                self.assertEqual(
                    {key: standalone[key] for key in embedded_keys},
                    {key: embedded[key] for key in embedded_keys},
                )
                for ref in local_refs(package):
                    resolve_local_ref(package, ref)

    def test_v1_requires_the_complete_two_stage_publication(self) -> None:
        schema = load(RUNTIME_SCHEMAS / "preprocess-package-v1.json")
        required = set(schema["required"])
        self.assertTrue(
            {
                "pipeline_status",
                "pipeline",
                "model",
                "reasoning_effort",
                "processing_contract_sha256",
                "draft_analysis",
                "critical_review",
                "stage_receipts",
                "analysis_checkpoint_sha256",
                "analysis_checkpoint_ref",
                "checkpoint_consumable",
            }.issubset(required)
        )
        provenance = schema["properties"]["model_receipt"]["properties"][
            "runtime_metadata_provenance"
        ]["enum"]
        self.assertEqual(
            provenance,
            ["codex_json_attestation_v1", "unavailable"],
        )

    def test_proposal_operations_are_nonempty_and_unique_everywhere(self) -> None:
        schemas = [
            load(PLUGIN_SCHEMAS / "luna-proposal-v2.json"),
            load(RUNTIME_SCHEMAS / "preprocess-package-v1.json")["$defs"][
                "lunaProposal"
            ],
            load(RUNTIME_SCHEMAS / "preprocess-package-v2.json")["$defs"][
                "lunaProposal"
            ],
        ]
        for schema in schemas:
            operations = schema["properties"]["operations"]
            self.assertEqual(operations["minItems"], 1)
            self.assertIs(operations["uniqueItems"], True)

    def test_math_live_business_schemas_are_release_bound_and_synced(self) -> None:
        names = (
            "math-luna-business-task-manifest-v1.json",
            "math-luna-business-task-batch-v1.json",
            "math-luna-real-business-samples-v1.json",
            "math-live-business-fixture-validation-v1.json",
            "math-live-business-golden-assertion-result-v1.json",
        )
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(
                    (RUNTIME_SCHEMAS / name).read_bytes(),
                    (PLUGIN_SCHEMAS / name).read_bytes(),
                )
                value = load(RUNTIME_SCHEMAS / name)
                self.assertEqual(
                    value["$schema"],
                    "https://json-schema.org/draft/2020-12/schema",
                )

        item = load(RUNTIME_SCHEMAS / names[0])
        solution_roles = item["properties"]["files"]["allOf"][1]["contains"][
            "properties"
        ]["role"]["enum"]
        self.assertEqual(solution_roles, ["solution_text", "solution_image"])
        self.assertEqual(
            item["properties"]["blocking_missing_artifacts"]["maxItems"], 0
        )

        batch = load(RUNTIME_SCHEMAS / names[1])
        self.assertEqual(
            batch["$defs"]["task"]["properties"]["task_kind"]["enum"],
            [
                "independent_correct_no_false_wrong_card",
                "wrong_then_corrected_weakness_proposal",
            ],
        )
        model = batch["$defs"]["model_request"]["properties"]
        self.assertEqual(model["model"]["const"], "gpt-5.6-luna")
        self.assertEqual(model["reasoning_effort"]["const"], "max")
        self.assertEqual(
            model["runtime_attestation"]["const"], "requested_unverified"
        )
        authority = load(RUNTIME_SCHEMAS / names[2])
        self.assertEqual(
            authority["$defs"]["task"]["properties"]["task_kind"]["enum"],
            [
                "independent_correct_no_false_wrong_card",
                "wrong_then_corrected_weakness_proposal",
                "wrong_then_corrected_multistage_method_proposal",
            ],
        )
        self.assertEqual(
            authority["$defs"]["model_request"]["properties"]["model"]["const"],
            "gpt-5.6-luna",
        )


if __name__ == "__main__":
    unittest.main()
