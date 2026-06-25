# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Check Python SDK public API compatibility.

This command compares a small AST-derived manifest for the public contract files listed in
``CONTRACT_FILES``. It intentionally avoids importing the package so it can compare arbitrary git
revisions without requiring cloud credentials, provider extras, or runtime side effects.

The check is conservative for SemVer 1.x releases: removals, newly-required inputs, narrowed schema
values, and subclass-breaking abstract methods are reported as compatibility issues. Additive
changes such as new optional parameters, enum members, or optional schema properties are allowed.
"""

from __future__ import annotations

import argparse
import ast
import logging
import pathlib
import shutil
import subprocess
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, cast

import multistorageclient_scripts.cli as cli
import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions

logger = logging.getLogger(__name__)


CONTRACT_FILES: Final[Mapping[str, pathlib.PurePosixPath]] = {
    "multistorageclient.types": pathlib.PurePosixPath("multi-storage-client/src/multistorageclient/types.py"),
    "multistorageclient.client.types": pathlib.PurePosixPath(
        "multi-storage-client/src/multistorageclient/client/types.py"
    ),
    "multistorageclient.schema": pathlib.PurePosixPath("multi-storage-client/src/multistorageclient/schema.py"),
}
# TODO: After MSC 1.0 is released, check compatibility against the 1.0 tag instead of origin/main.
BASE_REVISION: Final[str] = "origin/main"
HEAD_REVISION: Final[str] = "."


@dataclass(frozen=True, kw_only=True)
class ParameterManifest:
    """
    Function parameter shape that affects source compatibility for callers.
    """

    name: str
    kind: str
    required: bool


@dataclass(frozen=True, kw_only=True)
class FunctionManifest:
    """
    Public callable metadata used to detect incompatible signature and member-kind changes.
    """

    parameters: tuple[ParameterManifest, ...]
    is_abstract: bool = False
    is_async: bool = False
    is_property: bool = False


@dataclass(frozen=True, kw_only=True)
class FieldManifest:
    """
    Dataclass field metadata that affects constructor compatibility.
    """

    required: bool


@dataclass(frozen=True, kw_only=True)
class ClassManifest:
    """
    Public class metadata for enums, dataclasses, methods, and abstract client contracts.
    """

    kind: str
    methods: dict[str, FunctionManifest] = field(default_factory=dict)
    fields: dict[str, FieldManifest] = field(default_factory=dict)
    enum_members: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class SchemaManifest:
    """
    JSON-schema compatibility metadata keyed by dotted property paths.
    """

    nodes: set[str] = field(default_factory=set)
    properties: set[str] = field(default_factory=set)
    required: set[str] = field(default_factory=set)
    enums: dict[str, set[str]] = field(default_factory=dict)
    types: dict[str, set[str]] = field(default_factory=dict)
    additional_properties_false: set[str] = field(default_factory=set)


@dataclass(frozen=True, kw_only=True)
class ModuleManifest:
    """
    Public symbols extracted from one contract module.
    """

    classes: dict[str, ClassManifest] = field(default_factory=dict)
    functions: dict[str, FunctionManifest] = field(default_factory=dict)
    constants: set[str] = field(default_factory=set)
    schemas: dict[str, SchemaManifest] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class ContractManifest:
    """
    Public API contract for all files covered by this guardrail.
    """

    modules: dict[str, ModuleManifest]


@dataclass(frozen=True, kw_only=True)
class CompatibilityIssue:
    """
    One SemVer compatibility issue found while comparing contract manifests.
    """

    code: str
    path: str
    message: str


@dataclass(frozen=True, kw_only=True)
class Arguments(argparse_extensions.Arguments):
    """
    Command arguments.
    """

    #: Print issues but exit successfully.
    warn_only: Final[bool] = False


PARSER = cli.SUBPARSERS.add_parser(
    name="check-api-compat",
    help="Check Python SDK public API compatibility for semantic-versioning guardrails.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    allow_abbrev=False,
)

argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="warn_only")(
    help="Print compatibility issues but return success."
)


def extract_manifest(source_root: pathlib.Path, *, allow_missing_modules: bool = False) -> ContractManifest:
    """
    Extract the public API contract from a repository checkout.

    :param source_root: Repository root containing the paths listed in ``CONTRACT_FILES``.
    :param allow_missing_modules: Skip missing contract files so the comparator can report module removals.
    :return: AST-derived manifest for the tracked public contract modules.
    """

    modules: dict[str, ModuleManifest] = {}
    for module_name, relative_path in CONTRACT_FILES.items():
        module_path = source_root / relative_path
        if not module_path.exists():
            if allow_missing_modules:
                continue
            raise FileNotFoundError(f"Missing public API contract file: {module_path}")
        modules[module_name] = _extract_module_manifest(module_path)
    return ContractManifest(modules=modules)


def compare_manifests(base: ContractManifest, head: ContractManifest) -> list[CompatibilityIssue]:
    """
    Compare two public API contracts and return SemVer-breaking changes.
    """

    issues: list[CompatibilityIssue] = []

    for module_name, base_module in base.modules.items():
        head_module = head.modules.get(module_name)
        if head_module is None:
            issues.append(
                CompatibilityIssue(
                    code="module-removed",
                    path=module_name,
                    message=f"Public contract module {module_name} was removed.",
                )
            )
            continue
        issues.extend(_compare_modules(module_name, base_module, head_module))

    return issues


def func(arguments: Arguments) -> argparse_extensions.CommandFunction.ExitCode:
    """
    Run the CLI command against the configured base and head revisions.
    """

    repo_root = _git_repo_root()
    with tempfile.TemporaryDirectory() as scratch_directory:
        scratch_root = pathlib.Path(scratch_directory)
        base_root = _revision_source_root(repo_root=repo_root, revision=BASE_REVISION, scratch_root=scratch_root)
        head_root = _revision_source_root(repo_root=repo_root, revision=HEAD_REVISION, scratch_root=scratch_root)
        issues = compare_manifests(
            extract_manifest(base_root),
            extract_manifest(head_root, allow_missing_modules=True),
        )

    if not issues:
        logger.info("Python API compatibility check passed.")
        return 0

    for issue in issues:
        logger.error("%s %s: %s", issue.code, issue.path, issue.message)

    if arguments.warn_only:
        logger.warning("Python API compatibility issues found, but --warn-only is set.")
        return 0
    return 1


argparse_extensions.set_command_function(
    parser=PARSER,
    arguments_type=Arguments,
    func=cast(argparse_extensions.CommandFunction[Arguments], func),
)


def _extract_module_manifest(module_path: pathlib.Path) -> ModuleManifest:
    """
    Extract public symbols from a Python module without importing it.
    """

    module_ast = ast.parse(module_path.read_text(), filename=str(module_path))
    schema_environment: dict[str, Any] = {}
    classes: dict[str, ClassManifest] = {}
    functions: dict[str, FunctionManifest] = {}
    constants: set[str] = set()
    schemas: dict[str, SchemaManifest] = {}

    for statement in module_ast.body:
        if isinstance(statement, ast.ClassDef) and _is_public_name(statement.name):
            classes[statement.name] = _extract_class_manifest(statement)
        elif isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_public_name(statement.name):
            functions[statement.name] = _extract_function_manifest(statement)
        elif isinstance(statement, (ast.Assign, ast.AnnAssign)):
            for name in _assignment_names(statement):
                if not _is_public_name(name):
                    continue
                constants.add(name)
                if name.endswith("_SCHEMA"):
                    schema_environment[name] = _literal_eval_schema(_assignment_value(statement), schema_environment)
                    schemas[name] = _extract_schema_manifest(schema_environment[name])

    return ModuleManifest(classes=classes, functions=functions, constants=constants, schemas=schemas)


def _extract_class_manifest(class_def: ast.ClassDef) -> ClassManifest:
    kind = _class_kind(class_def)
    methods: dict[str, FunctionManifest] = {}
    fields: dict[str, FieldManifest] = {}
    enum_members: dict[str, str] = {}
    is_dataclass = _has_decorator(class_def.decorator_list, "dataclass")

    for statement in class_def.body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_public_name(statement.name):
            methods[statement.name] = _extract_function_manifest(statement)
        elif kind == "enum" and isinstance(statement, ast.Assign):
            for name in _assignment_names(statement):
                if _is_public_name(name):
                    enum_members[name] = _literal_source(statement.value)
        elif is_dataclass and isinstance(statement, ast.AnnAssign) and isinstance(statement.target, ast.Name):
            if _is_public_name(statement.target.id):
                fields[statement.target.id] = FieldManifest(required=_field_is_required(statement))

    return ClassManifest(kind=kind, methods=methods, fields=fields, enum_members=enum_members)


def _extract_function_manifest(function_def: ast.FunctionDef | ast.AsyncFunctionDef) -> FunctionManifest:
    return FunctionManifest(
        parameters=tuple(_iter_parameters(function_def.args)),
        is_abstract=_has_decorator(function_def.decorator_list, "abstractmethod"),
        is_async=isinstance(function_def, ast.AsyncFunctionDef),
        is_property=_has_decorator(function_def.decorator_list, "property"),
    )


def _compare_modules(module_name: str, base: ModuleManifest, head: ModuleManifest) -> Iterator[CompatibilityIssue]:
    for constant in sorted(base.constants - head.constants):
        yield CompatibilityIssue(
            code="constant-removed",
            path=f"{module_name}.{constant}",
            message=f"Public constant {constant} was removed.",
        )

    for function_name, base_function in sorted(base.functions.items()):
        head_function = head.functions.get(function_name)
        path = f"{module_name}.{function_name}"
        if head_function is None:
            yield CompatibilityIssue(code="function-removed", path=path, message=f"Public function {path} was removed.")
        else:
            yield from _compare_functions(path, base_function, head_function)

    for class_name, base_class in sorted(base.classes.items()):
        head_class = head.classes.get(class_name)
        path = f"{module_name}.{class_name}"
        if head_class is None:
            yield CompatibilityIssue(code="class-removed", path=path, message=f"Public class {path} was removed.")
            continue
        yield from _compare_classes(path, base_class, head_class)

    for schema_name, base_schema in sorted(base.schemas.items()):
        head_schema = head.schemas.get(schema_name)
        path = f"{module_name}.{schema_name}"
        if head_schema is None:
            yield CompatibilityIssue(code="schema-removed", path=path, message=f"Schema {path} was removed.")
            continue
        yield from _compare_schemas(path, base_schema, head_schema)


def _compare_classes(path: str, base: ClassManifest, head: ClassManifest) -> Iterator[CompatibilityIssue]:
    if base.kind != head.kind:
        yield CompatibilityIssue(
            code="class-kind-changed",
            path=path,
            message=f"Public class {path} changed kind from {base.kind} to {head.kind}.",
        )

    if base.kind == "enum":
        for member_name, base_value in sorted(base.enum_members.items()):
            if member_name not in head.enum_members:
                yield CompatibilityIssue(
                    code="enum-member-removed",
                    path=f"{path}.{member_name}",
                    message=f"Enum member {path}.{member_name} was removed.",
                )
            elif head.enum_members[member_name] != base_value:
                yield CompatibilityIssue(
                    code="enum-member-value-changed",
                    path=f"{path}.{member_name}",
                    message=f"Enum member {path}.{member_name} changed value from {base_value} to {head.enum_members[member_name]}.",
                )

    for field_name, base_field in sorted(base.fields.items()):
        head_field = head.fields.get(field_name)
        field_path = f"{path}.{field_name}"
        if head_field is None:
            yield CompatibilityIssue(
                code="dataclass-field-removed",
                path=field_path,
                message=f"Dataclass field {field_path} was removed.",
            )
        elif not base_field.required and head_field.required:
            yield CompatibilityIssue(
                code="dataclass-field-required",
                path=field_path,
                message=f"Dataclass field {field_path} changed from optional to required.",
            )

    for field_name, head_field in sorted(head.fields.items()):
        if field_name not in base.fields and head_field.required:
            field_path = f"{path}.{field_name}"
            yield CompatibilityIssue(
                code="dataclass-field-added-required",
                path=field_path,
                message=f"Required dataclass field {field_path} was added.",
            )

    for method_name, base_method in sorted(base.methods.items()):
        head_method = head.methods.get(method_name)
        method_path = f"{path}.{method_name}"
        if head_method is None:
            yield CompatibilityIssue(
                code="method-removed",
                path=method_path,
                message=f"Public method {method_path} was removed.",
            )
        else:
            yield from _compare_functions(method_path, base_method, head_method)

    if path.endswith(".AbstractStorageClient"):
        for method_name, base_method in sorted(base.methods.items()):
            head_method = head.methods.get(method_name)
            if head_method is not None and not base_method.is_abstract and head_method.is_abstract:
                yield CompatibilityIssue(
                    code="abstract-method-required",
                    path=f"{path}.{method_name}",
                    message=f"Method {path}.{method_name} changed from concrete to abstract and will break subclasses.",
                )

        for method_name, head_method in sorted(head.methods.items()):
            if method_name not in base.methods and head_method.is_abstract:
                yield CompatibilityIssue(
                    code="abstract-method-added",
                    path=f"{path}.{method_name}",
                    message=f"Abstract method {path}.{method_name} was added and will break subclasses.",
                )


def _compare_functions(path: str, base: FunctionManifest, head: FunctionManifest) -> Iterator[CompatibilityIssue]:
    if base.is_property != head.is_property:
        yield CompatibilityIssue(
            code="member-kind-changed",
            path=path,
            message=f"Public member {path} changed between method and property.",
        )

    if base.is_async != head.is_async:
        yield CompatibilityIssue(
            code="function-async-changed",
            path=path,
            message=f"Public callable {path} changed between sync and async.",
        )

    base_ordered_parameters = [parameter for parameter in base.parameters if parameter.kind != "keyword-only"]
    head_ordered_parameters = [parameter for parameter in head.parameters if parameter.kind != "keyword-only"]
    yield from _compare_ordered_parameters(path, base_ordered_parameters, head_ordered_parameters)
    yield from _compare_keyword_only_parameters(path, base.parameters, head.parameters)


def _compare_ordered_parameters(
    path: str, base_parameters: Sequence[ParameterManifest], head_parameters: Sequence[ParameterManifest]
) -> Iterator[CompatibilityIssue]:
    for index, base_parameter in enumerate(base_parameters):
        if index >= len(head_parameters):
            yield CompatibilityIssue(
                code="parameter-removed",
                path=f"{path}.{base_parameter.name}",
                message=f"Parameter {base_parameter.name} was removed from {path}.",
            )
            continue

        head_parameter = head_parameters[index]
        parameter_path = f"{path}.{base_parameter.name}"
        if base_parameter.name != head_parameter.name or base_parameter.kind != head_parameter.kind:
            yield CompatibilityIssue(
                code="parameter-reordered-or-renamed",
                path=parameter_path,
                message=f"Parameter {base_parameter.name} in {path} was reordered, renamed, or changed kind.",
            )
            continue

        if not base_parameter.required and head_parameter.required:
            yield CompatibilityIssue(
                code="parameter-required",
                path=parameter_path,
                message=f"Parameter {base_parameter.name} in {path} changed from optional to required.",
            )

    for head_parameter in head_parameters[len(base_parameters) :]:
        if head_parameter.required:
            yield CompatibilityIssue(
                code="parameter-added-required",
                path=f"{path}.{head_parameter.name}",
                message=f"Required parameter {head_parameter.name} was added to {path}.",
            )


def _compare_keyword_only_parameters(
    path: str, base_parameters: Sequence[ParameterManifest], head_parameters: Sequence[ParameterManifest]
) -> Iterator[CompatibilityIssue]:
    base_keyword_only_parameters = {
        parameter.name: parameter for parameter in base_parameters if parameter.kind == "keyword-only"
    }
    head_keyword_only_parameters = {
        parameter.name: parameter for parameter in head_parameters if parameter.kind == "keyword-only"
    }

    for parameter_name, base_parameter in sorted(base_keyword_only_parameters.items()):
        head_parameter = head_keyword_only_parameters.get(parameter_name)
        parameter_path = f"{path}.{parameter_name}"
        if head_parameter is None:
            yield CompatibilityIssue(
                code="parameter-removed",
                path=parameter_path,
                message=f"Parameter {parameter_name} was removed from {path}.",
            )
        elif not base_parameter.required and head_parameter.required:
            yield CompatibilityIssue(
                code="parameter-required",
                path=parameter_path,
                message=f"Parameter {parameter_name} in {path} changed from optional to required.",
            )

    for parameter_name, head_parameter in sorted(head_keyword_only_parameters.items()):
        if parameter_name not in base_keyword_only_parameters and head_parameter.required:
            yield CompatibilityIssue(
                code="parameter-added-required",
                path=f"{path}.{parameter_name}",
                message=f"Required parameter {parameter_name} was added to {path}.",
            )


def _compare_schemas(path: str, base: SchemaManifest, head: SchemaManifest) -> Iterator[CompatibilityIssue]:
    for property_path in sorted(base.properties - head.properties):
        yield CompatibilityIssue(
            code="schema-property-removed",
            path=_issue_path(path, property_path),
            message=f"Schema property {property_path} was removed.",
        )

    for required_path in sorted(head.required - base.required):
        parent_path = _parent_schema_path(required_path)
        if parent_path and parent_path not in base.nodes and parent_path not in base.properties:
            continue
        yield CompatibilityIssue(
            code="schema-required-added",
            path=_issue_path(path, required_path),
            message=f"Schema property {required_path} became required.",
        )

    for enum_path, base_values in sorted(base.enums.items()):
        head_values = head.enums.get(enum_path)
        if head_values is not None and not base_values.issubset(head_values):
            removed = sorted(base_values - head_values)
            yield CompatibilityIssue(
                code="schema-enum-narrowed",
                path=_issue_path(path, enum_path),
                message=f"Schema enum at {enum_path} removed values: {removed}.",
            )

    for type_path, base_types in sorted(base.types.items()):
        head_types = head.types.get(type_path)
        if head_types is not None and not base_types.issubset(head_types):
            removed = sorted(base_types - head_types)
            yield CompatibilityIssue(
                code="schema-type-narrowed",
                path=_issue_path(path, type_path),
                message=f"Schema type at {type_path} removed accepted types: {removed}.",
            )

    for additional_path in sorted(head.additional_properties_false - base.additional_properties_false):
        if additional_path not in base.nodes:
            continue
        yield CompatibilityIssue(
            code="schema-additional-properties-closed",
            path=_issue_path(path, additional_path),
            message=f"Schema at {additional_path} changed additionalProperties to false.",
        )


def _extract_schema_manifest(schema: Any) -> SchemaManifest:
    """
    Extract compatibility-relevant JSON-schema constraints.
    """

    manifest = SchemaManifest()
    _walk_schema(schema, path="", manifest=manifest)
    return manifest


def _walk_schema(schema: Any, *, path: str, manifest: SchemaManifest) -> None:
    """
    Record schema constraints under stable dotted paths such as ``profiles.default.provider``.
    """

    if isinstance(schema, dict):
        manifest.nodes.add(path)

        schema_type = schema.get("type")
        if schema_type is not None:
            manifest.types[path] = _schema_type_set(schema_type)

        schema_enum = schema.get("enum")
        if isinstance(schema_enum, list):
            manifest.enums[path] = {repr(value) for value in schema_enum}

        if schema.get("additionalProperties") is False:
            manifest.additional_properties_false.add(path)

        required = schema.get("required", [])
        if isinstance(required, list):
            for required_property in required:
                if isinstance(required_property, str):
                    manifest.required.add(_schema_path(path, required_property))

        properties = schema.get("properties", {})
        if isinstance(properties, dict):
            for property_name, property_schema in properties.items():
                property_path = _schema_path(path, property_name)
                manifest.properties.add(property_path)
                _walk_schema(property_schema, path=property_path, manifest=manifest)

        for key in ("items", "additionalProperties"):
            nested_schema = schema.get(key)
            if isinstance(nested_schema, dict):
                _walk_schema(nested_schema, path=_schema_path(path, key), manifest=manifest)

        for key in ("oneOf", "anyOf", "allOf"):
            nested_schemas = schema.get(key)
            if isinstance(nested_schemas, list):
                for index, nested_schema in enumerate(nested_schemas):
                    if isinstance(nested_schema, dict):
                        _walk_schema(nested_schema, path=_schema_path(path, f"{key}.{index}"), manifest=manifest)


def _schema_type_set(schema_type: Any) -> set[str]:
    if isinstance(schema_type, str):
        return {schema_type}
    if isinstance(schema_type, list):
        return {value for value in schema_type if isinstance(value, str)}
    return set()


def _schema_path(parent: str, child: str) -> str:
    return f"{parent}.{child}" if parent else child


def _parent_schema_path(path: str) -> str:
    if "." not in path:
        return ""
    return path.rsplit(".", maxsplit=1)[0]


def _issue_path(parent: str, child: str) -> str:
    return f"{parent}.{child}" if child else parent


def _iter_parameters(arguments: ast.arguments) -> Iterator[ParameterManifest]:
    positional = [
        *((parameter, "positional-only") for parameter in arguments.posonlyargs),
        *((parameter, "positional") for parameter in arguments.args),
    ]
    positional_defaults = [None] * (len(positional) - len(arguments.defaults)) + list(arguments.defaults)

    for (parameter, kind), default in zip(positional, positional_defaults, strict=True):
        yield ParameterManifest(name=parameter.arg, kind=kind, required=default is None)

    if arguments.vararg is not None:
        yield ParameterManifest(name=arguments.vararg.arg, kind="vararg", required=False)

    for parameter, default in zip(arguments.kwonlyargs, arguments.kw_defaults, strict=True):
        yield ParameterManifest(name=parameter.arg, kind="keyword-only", required=default is None)

    if arguments.kwarg is not None:
        yield ParameterManifest(name=arguments.kwarg.arg, kind="varkw", required=False)


def _class_kind(class_def: ast.ClassDef) -> str:
    base_names = {_name_from_expression(base) for base in class_def.bases}
    if "Enum" in base_names:
        return "enum"
    return "class"


def _has_decorator(decorators: Sequence[ast.expr], decorator_name: str) -> bool:
    return any(_name_from_expression(decorator) == decorator_name for decorator in decorators)


def _field_is_required(statement: ast.AnnAssign) -> bool:
    if statement.value is None:
        return True
    if isinstance(statement.value, ast.Call) and _name_from_expression(statement.value.func) == "field":
        return not any(keyword.arg in {"default", "default_factory"} for keyword in statement.value.keywords)
    return False


def _assignment_names(statement: ast.Assign | ast.AnnAssign) -> Iterator[str]:
    if isinstance(statement, ast.AnnAssign):
        if isinstance(statement.target, ast.Name):
            yield statement.target.id
        return

    for target in statement.targets:
        if isinstance(target, ast.Name):
            yield target.id


def _assignment_value(statement: ast.Assign | ast.AnnAssign) -> ast.AST:
    if isinstance(statement, ast.AnnAssign):
        if statement.value is None:
            raise ValueError("Annotated assignment has no value.")
        return statement.value
    return statement.value


def _literal_eval_schema(node: ast.AST, environment: Mapping[str, Any]) -> Any:
    if isinstance(node, ast.Name) and node.id in environment:
        return environment[node.id]
    if isinstance(node, ast.Dict):
        return {
            _literal_eval_schema(key, environment): _literal_eval_schema(value, environment)
            for key, value in zip(node.keys, node.values, strict=True)
            if key is not None
        }
    if isinstance(node, ast.List):
        return [_literal_eval_schema(element, environment) for element in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_literal_eval_schema(element, environment) for element in node.elts)
    return ast.literal_eval(node)


def _literal_source(node: ast.AST) -> str:
    try:
        return repr(ast.literal_eval(node))
    except (ValueError, TypeError):
        return ast.unparse(node)


def _name_from_expression(expression: ast.AST) -> str:
    if isinstance(expression, ast.Name):
        return expression.id
    if isinstance(expression, ast.Attribute):
        return expression.attr
    if isinstance(expression, ast.Call):
        return _name_from_expression(expression.func)
    return ""


def _is_public_name(name: str) -> bool:
    return not name.startswith("_")


def _git_repo_root() -> pathlib.Path:
    return pathlib.Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()).resolve()


def _revision_source_root(*, repo_root: pathlib.Path, revision: str, scratch_root: pathlib.Path) -> pathlib.Path:
    """
    Materialize the tracked contract files for a git revision.
    """

    if revision == ".":
        return repo_root

    revision_root = scratch_root / revision.replace("/", "_").replace(":", "_")
    for relative_path in CONTRACT_FILES.values():
        target_path = revision_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            contents = subprocess.check_output(
                ["git", "-C", str(repo_root), "show", f"{revision}:{relative_path.as_posix()}"],
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            raise FileNotFoundError(f"Unable to read {relative_path} from git revision {revision}.") from e
        target_path.write_text(contents)
    return revision_root


def copy_current_contract(source_root: pathlib.Path, target_root: pathlib.Path) -> None:
    """
    Copy the current checkout's tracked contract files into another repository-shaped tree.
    """

    for relative_path in CONTRACT_FILES.values():
        source = source_root / relative_path
        target = target_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
