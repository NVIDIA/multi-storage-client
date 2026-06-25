import textwrap
from pathlib import Path

import multistorageclient_scripts.cli as cli
import multistorageclient_scripts.cli.check_python_api_compat as check_python_api_compat
import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions
from multistorageclient_scripts.cli.check_python_api_compat import (
    CompatibilityIssue,
    compare_manifests,
    extract_manifest,
)


def write_contract(
    root: Path, *, types_source: str = "", client_types_source: str = "", schema_source: str = ""
) -> None:
    package_root = root / "multi-storage-client" / "src" / "multistorageclient"
    client_root = package_root / "client"
    client_root.mkdir(parents=True)
    (package_root / "types.py").write_text(textwrap.dedent(types_source))
    (client_root / "types.py").write_text(textwrap.dedent(client_types_source))
    (package_root / "schema.py").write_text(textwrap.dedent(schema_source))


def issue_codes(base_root: Path, head_root: Path) -> set[str]:
    return {
        issue.code
        for issue in compare_manifests(
            extract_manifest(base_root),
            extract_manifest(head_root, allow_missing_modules=True),
        )
    }


def test_cli_uses_check_api_compat_command_name_without_revision_arguments() -> None:
    arguments = cli.PARSER.parse_args(["check-api-compat"])

    assert not hasattr(arguments, "base")
    assert not hasattr(arguments, "head")


def test_run_cli_returns_success_for_warn_only_compatibility_issues(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["msc-scripts", "check-api-compat", "--warn-only", "True"],
    )
    monkeypatch.setattr(check_python_api_compat, "_git_repo_root", lambda: Path("/repo"))
    monkeypatch.setattr(
        check_python_api_compat,
        "_revision_source_root",
        lambda *, repo_root, revision, scratch_root: Path(f"/snapshot/{revision.replace('/', '_')}"),
    )
    monkeypatch.setattr(
        check_python_api_compat,
        "extract_manifest",
        lambda source_root, **kwargs: object(),
    )
    monkeypatch.setattr(
        check_python_api_compat,
        "compare_manifests",
        lambda base, head: [CompatibilityIssue(code="breaking", path="multistorageclient.types.X", message="breakage")],
    )

    try:
        argparse_extensions.run_cli(parser=cli.PARSER)
    except SystemExit as e:
        assert e.code == 0
    else:
        raise AssertionError("run_cli should exit with the command status")


def test_func_returns_failure_for_compatibility_issues(monkeypatch) -> None:
    revisions: list[str] = []
    monkeypatch.setattr(check_python_api_compat, "_git_repo_root", lambda: Path("/repo"))

    def fake_revision_source_root(*, repo_root, revision, scratch_root) -> Path:
        revisions.append(revision)
        return Path(f"/snapshot/{revision.replace('/', '_')}")

    monkeypatch.setattr(check_python_api_compat, "_revision_source_root", fake_revision_source_root)
    monkeypatch.setattr(check_python_api_compat, "extract_manifest", lambda source_root, **kwargs: object())
    monkeypatch.setattr(
        check_python_api_compat,
        "compare_manifests",
        lambda base, head: [CompatibilityIssue(code="breaking", path="multistorageclient.types.X", message="breakage")],
    )

    assert check_python_api_compat.func(check_python_api_compat.Arguments()) == 1
    assert revisions == [
        check_python_api_compat.BASE_REVISION,
        check_python_api_compat.HEAD_REVISION,
    ]


def test_detects_enum_member_removal(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            from enum import Enum

            class StorageMode(str, Enum):
                READ = "read"
                WRITE = "write"
        """,
    )
    write_contract(
        head,
        types_source="""
            from enum import Enum

            class StorageMode(str, Enum):
                READ = "read"
        """,
    )

    assert "enum-member-removed" in issue_codes(base, head)


def test_detects_contract_module_removal(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(base, types_source="DEFAULT_RETRY_ATTEMPTS = 3\n")
    write_contract(head, types_source="DEFAULT_RETRY_ATTEMPTS = 3\n")
    (head / "multi-storage-client" / "src" / "multistorageclient" / "types.py").unlink()

    assert "module-removed" in issue_codes(base, head)


def test_detects_enum_member_value_change(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            from enum import Enum

            class StorageMode(str, Enum):
                READ = "read"
        """,
    )
    write_contract(
        head,
        types_source="""
            from enum import Enum

            class StorageMode(str, Enum):
                READ = "changed"
        """,
    )

    assert "enum-member-value-changed" in issue_codes(base, head)


def test_detects_public_class_kind_change(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            class StorageMode:
                pass
        """,
    )
    write_contract(
        head,
        types_source="""
            from enum import Enum

            class StorageMode(str, Enum):
                READ = "read"
        """,
    )

    assert "class-kind-changed" in issue_codes(base, head)


def test_detects_public_constant_removal(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(base, types_source="DEFAULT_RETRY_ATTEMPTS = 3\n")
    write_contract(head, types_source="")

    assert "constant-removed" in issue_codes(base, head)


def test_detects_public_function_removal(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            def read(path: str) -> bytes:
                return b""
        """,
    )
    write_contract(head, types_source="")

    assert "function-removed" in issue_codes(base, head)


def test_detects_public_class_removal(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            class ObjectMetadata:
                pass
        """,
    )
    write_contract(head, types_source="")

    assert "class-removed" in issue_codes(base, head)


def test_detects_dataclass_field_made_required(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            from dataclasses import dataclass

            @dataclass
            class ObjectMetadata:
                key: str
                etag: str | None = None
        """,
    )
    write_contract(
        head,
        types_source="""
            from dataclasses import dataclass

            @dataclass
            class ObjectMetadata:
                key: str
                etag: str | None
        """,
    )

    assert "dataclass-field-required" in issue_codes(base, head)


def test_detects_required_dataclass_field_addition(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            from dataclasses import dataclass

            @dataclass
            class ObjectMetadata:
                key: str
        """,
    )
    write_contract(
        head,
        types_source="""
            from dataclasses import dataclass

            @dataclass
            class ObjectMetadata:
                key: str
                content_length: int
        """,
    )

    assert "dataclass-field-added-required" in issue_codes(base, head)


def test_detects_function_parameter_made_required(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            def read(path: str, byte_range: object | None = None) -> bytes:
                return b""
        """,
    )
    write_contract(
        head,
        types_source="""
            def read(path: str, byte_range: object | None) -> bytes:
                return b""
        """,
    )

    assert "parameter-required" in issue_codes(base, head)


def test_detects_function_changed_to_async(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            def read(path: str) -> bytes:
                return b""
        """,
    )
    write_contract(
        head,
        types_source="""
            async def read(path: str) -> bytes:
                return b""
        """,
    )

    assert "function-async-changed" in issue_codes(base, head)


def test_detects_required_parameter_addition(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            def read(path: str) -> bytes:
                return b""
        """,
    )
    write_contract(
        head,
        types_source="""
            def read(path: str, byte_range: object) -> bytes:
                return b""
        """,
    )

    assert "parameter-added-required" in issue_codes(base, head)


def test_detects_positional_only_parameter_changed_to_regular_positional(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            def read(path: str, /) -> bytes:
                return b""
        """,
    )
    write_contract(
        head,
        types_source="""
            def read(path: str) -> bytes:
                return b""
        """,
    )

    assert "parameter-reordered-or-renamed" in issue_codes(base, head)


def test_allows_optional_keyword_only_parameter_inserted_before_existing_one(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            def read(path: str, *, encoding: str | None = None) -> bytes:
                return b""
        """,
    )
    write_contract(
        head,
        types_source="""
            def read(path: str, *, byte_range: object | None = None, encoding: str | None = None) -> bytes:
                return b""
        """,
    )

    assert compare_manifests(extract_manifest(base), extract_manifest(head)) == []


def test_detects_method_changed_to_property(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            class ObjectMetadata:
                def key(self) -> str:
                    return ""
        """,
    )
    write_contract(
        head,
        types_source="""
            class ObjectMetadata:
                @property
                def key(self) -> str:
                    return ""
        """,
    )

    assert "member-kind-changed" in issue_codes(base, head)


def test_detects_public_method_removal(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            class ObjectMetadata:
                def to_dict(self) -> dict:
                    return {}
        """,
    )
    write_contract(
        head,
        types_source="""
            class ObjectMetadata:
                pass
        """,
    )

    assert "method-removed" in issue_codes(base, head)


def test_detects_new_abstract_storage_client_method(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        client_types_source="""
            from abc import ABC, abstractmethod

            class AbstractStorageClient(ABC):
                @abstractmethod
                def read(self, path: str) -> bytes:
                    pass
        """,
    )
    write_contract(
        head,
        client_types_source="""
            from abc import ABC, abstractmethod

            class AbstractStorageClient(ABC):
                @abstractmethod
                def read(self, path: str) -> bytes:
                    pass

                @abstractmethod
                def stat(self, path: str) -> object:
                    pass
        """,
    )

    assert "abstract-method-added" in issue_codes(base, head)


def test_detects_existing_storage_client_method_made_abstract(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        client_types_source="""
            from abc import ABC

            class AbstractStorageClient(ABC):
                def read(self, path: str) -> bytes:
                    return b""
        """,
    )
    write_contract(
        head,
        client_types_source="""
            from abc import ABC, abstractmethod

            class AbstractStorageClient(ABC):
                @abstractmethod
                def read(self, path: str) -> bytes:
                    pass
        """,
    )

    assert "abstract-method-required" in issue_codes(base, head)


def test_detects_schema_property_removal(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                    "cache": {"type": "object"},
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                },
            }
        """,
    )

    assert "schema-property-removed" in issue_codes(base, head)


def test_detects_schema_enum_narrowing(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "provider": {"type": "string", "enum": ["s3", "gcs"]},
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "provider": {"type": "string", "enum": ["s3"]},
                },
            }
        """,
    )

    assert "schema-enum-narrowed" in issue_codes(base, head)


def test_detects_schema_required_property_addition(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {
                        "type": "object",
                        "properties": {"storage_provider": {"type": "object"}},
                    },
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {
                        "type": "object",
                        "properties": {"storage_provider": {"type": "object"}},
                        "required": ["storage_provider"],
                    },
                },
            }
        """,
    )

    assert "schema-required-added" in issue_codes(base, head)


def test_detects_schema_required_property_addition_inside_existing_items_node(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                        },
                    },
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                            "required": ["name"],
                        },
                    },
                },
            }
        """,
    )

    assert "schema-required-added" in issue_codes(base, head)


def test_detects_schema_required_property_addition_inside_existing_one_of_branch(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {
                        "type": "object",
                        "oneOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "storage_provider": {"type": "string"},
                                    "metadata_provider": {"type": "string"},
                                },
                                "required": ["storage_provider"],
                            },
                        ],
                    },
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {
                        "type": "object",
                        "oneOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "storage_provider": {"type": "string"},
                                    "metadata_provider": {"type": "string"},
                                },
                                "required": ["storage_provider", "metadata_provider"],
                            },
                        ],
                    },
                },
            }
        """,
    )

    assert "schema-required-added" in issue_codes(base, head)


def test_detects_schema_type_narrowing(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "size": {"type": ["string", "integer"]},
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "size": {"type": "string"},
                },
            }
        """,
    )

    assert "schema-type-narrowed" in issue_codes(base, head)


def test_detects_schema_additional_properties_closed_on_existing_path(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object", "additionalProperties": False},
                },
            }
        """,
    )

    assert "schema-additional-properties-closed" in issue_codes(base, head)


def test_allows_compatible_additions(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        types_source="""
            from dataclasses import dataclass
            from enum import Enum

            class StorageMode(str, Enum):
                READ = "read"

            @dataclass
            class ObjectMetadata:
                key: str

            def read(path: str) -> bytes:
                return b""
        """,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                },
            }
        """,
    )
    write_contract(
        head,
        types_source="""
            from dataclasses import dataclass
            from enum import Enum

            class StorageMode(str, Enum):
                READ = "read"
                WRITE = "write"

            @dataclass
            class ObjectMetadata:
                key: str
                etag: str | None = None

            def read(path: str, byte_range: object | None = None) -> bytes:
                return b""
        """,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                    "cache": {"type": "object"},
                },
            }
        """,
    )

    assert compare_manifests(extract_manifest(base), extract_manifest(head)) == []


def test_allows_required_fields_inside_new_optional_schema_property(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                    "plugin": {
                        "type": "object",
                        "properties": {"type": {"type": "string"}},
                        "required": ["type"],
                    },
                },
            }
        """,
    )

    assert compare_manifests(extract_manifest(base), extract_manifest(head)) == []


def test_allows_additional_properties_false_inside_new_optional_schema_property(tmp_path: Path) -> None:
    base = tmp_path / "base"
    head = tmp_path / "head"
    write_contract(
        base,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                },
            }
        """,
    )
    write_contract(
        head,
        schema_source="""
            CONFIG_SCHEMA = {
                "type": "object",
                "properties": {
                    "profiles": {"type": "object"},
                    "plugin": {
                        "type": "object",
                        "properties": {"type": {"type": "string"}},
                        "additionalProperties": False,
                    },
                },
            }
        """,
    )

    assert compare_manifests(extract_manifest(base), extract_manifest(head)) == []
