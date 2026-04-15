# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import argparse
import functools
import sys
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path, PurePath
from typing import (
    Any,
    ClassVar,
    Final,
    Literal,
    NoReturn,
    Optional,
    Protocol,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

# :py:mod:`argparse` extensions for typing and CLI structuring.
#
# We don't use existing CLI frameworks since:
#
# - They may drop support for Python versions sooner than we can.
#     - Likely to happen since we have to support some Python versions far past EOL.
#     - Gates security patches behind newer versions.
# - They may raise the minimum locked dependency versions or introduce dependency conflicts in the uv workspace.


class ArgumentEnum(Enum):
    """
    :py:type:`enum.Enum` subclass for arguments.

    Use instead of :py:type:`bool` and :py:type:`typing.Literal` to rename argument choices in parser arguments + messages.
    """

    # Render argument choices as the enum name with `_` replaced with `-`.
    #
    # Some parts of `argparse` use :py:func:`repr`.
    def __repr__(self) -> str:
        return self.name.replace("_", "-")

    # Ditto `__repr__`.
    #
    # Some parts of `argparse` use :py:func:`str`.
    def __str__(self) -> str:
        return repr(self)


@dataclass(frozen=True, kw_only=True)
class Arguments:
    """
    Command arguments.
    """

    pass


# Intentionally omit the return type to make `functools.partial` type inference work.
def add_argument_partial(
    parser: argparse.ArgumentParser,
    arguments_type: type[Arguments],
    argument_key: str,
    argument_value_reader: Optional[Callable[[str], Any]] = None,
):
    """
    Partially apply :py:meth:`argparse.ArgumentParser.add_argument`.

    :param parser: Argument parser.
    :param arguments_type: Arguments dataclass.
    :param argument_key: Argument key in the arguments type. Passed to :py:meth:`argparse.ArgumentParser.add_argument` as the `name` positional argument with `--` prepended and `_` replaced with `-`.

        If the argument's type universe contains only :py:type:`enum.Enum`, :py:type:`typing.Literal`, and `None` terms, the non-`None` terms are passed to :py:meth:`argparse.ArgumentParser.add_argument` as the `choices` keyword argument.

        If the argument has a default value, the default value is passed to :py:meth:`argparse.ArgumentParser.add_argument` as the `default` keyword argument.

        If the argument's type universe contains :py:type:`typing.Any` or `None` terms, `False` is passed to :py:meth:`argparse.ArgumentParser.add_argument` as the `required` keyword argument.
    :param argument_value_reader: Argument value reader which converts a string to a value of the argument type. Passed to :py:meth:`argparse.ArgumentParser.add_argument` as the `type` keyword argument.

        Not needed for:

        - :py:type:`bool`
        - :py:type:`complex`
        - :py:type:`float`
        - :py:type:`int`
        - :py:type:`str`
        - :py:type:`pathlib.Path`
        - :py:type:`pathlib.PurePath`
        - :py:type:`enum.Enum` subclasses (the argument is used for enum name lookup, i.e. `EnumSubclass[argument]`)

    :return: :py:meth:`argparse.ArgumentParser.add_argument` partial.
    """
    argument_flag = f"--{argument_key.replace('_', '-')}"

    # :py:func:`hasattr` only returns `True` if the attribute has a value.
    #
    # :py:func:`typing.get_type_hints` omits attributes without type hints.
    #
    # ```
    # > from typing import get_type_hints
    # > class Arguments:
    # >     a: int
    # >     b = 1
    # >
    # > hasattr(Arguments, "a")
    # False
    # > hasattr(Arguments, "b")
    # True
    # > get_type_hints(obj=Arguments)
    # {"a": int}
    # ```
    arguments_type_hints = get_type_hints(obj=arguments_type, include_extras=False)
    assert hasattr(arguments_type, argument_key) or argument_key in arguments_type_hints, (
        f"{arguments_type.__name__} doesn't contain {argument_key}!"
    )

    # Default arguments without type hints to `Any`.
    argument_type_universe: set = {arguments_type_hints.get(argument_key, Any)}

    # L-system to recursively flatten the argument's type universe.
    #
    # This lets us infer :py:meth:`argparse.ArgumentParser.add_argument` arguments as described in this function's docstring.
    #
    # We want to extract terms from these types by expanding them. For example:
    #
    # - `{Optional[Union[Literal["a"], int]]}` → `{"a", int, None}`
    #
    # In some cases, these types are automatically simplified (e.g. deduplicated, flattened). For example:
    #
    # - `Literal["a", "a", Literal[1]]` → `Literal["a", 1]`
    # - `Union[str, str]`               → `str`
    #
    # In other cases, we need to simplify types. For example:
    #
    # - `NoneType` → `None`
    expandable_types = frozenset({list, tuple, set, frozenset, ClassVar, Final, Literal, Optional, Union})
    while True:
        argument_type_universe_expansion: set = set()

        for argument_type in argument_type_universe:
            if get_origin(argument_type) in expandable_types:
                # Expandable.
                argument_type_universe_expansion.update(get_args(argument_type))
            elif argument_type in expandable_types:
                # Expandable (implicit `Any` without subscript).
                argument_type_universe_expansion.add(Any)
            elif isinstance(argument_type, type) and issubclass(argument_type, Enum):
                # Expandable (`Literal`-like).
                argument_type_universe_expansion.update(argument_type)
            else:
                # Non-expandable.
                argument_type_universe_expansion.add(argument_type)

        if type(None) in argument_type_universe_expansion:
            # Simplify `NoneType` to `None`.
            #
            # - `(get_type_hints(arguments_type)[argument_key])` → `(NoneType)` (if the argument has a `None` type hint)
            # - `get_args(Final[None])`                          → `(NoneType)`
            # - `get_args(Optional[int])`                        → `(int, NoneType)`
            # - `get_args(Union[int, None])`                     → `(int, NoneType)`
            argument_type_universe_expansion.remove(type(None))
            argument_type_universe_expansion.add(None)

        if argument_type_universe == argument_type_universe_expansion:
            # Done expanding.
            break
        else:
            # Keep expanding.
            argument_type_universe = argument_type_universe_expansion

    # Argument choices should only be non-type terms.
    argument_choices: Optional[tuple] = None
    argument_type_universe_non_type_terms: set = {_ for _ in argument_type_universe if not isinstance(_, type)}
    if (
        len(argument_type_universe_non_type_terms) > 0
        and argument_type_universe_non_type_terms == argument_type_universe
    ):
        argument_choices = tuple(argument_type_universe_non_type_terms)

    # Default argument value.
    argument_default = getattr(arguments_type, argument_key, None)

    # Argument is required if it has no default and `None` doesn't inhabit its type universe.
    argument_required = (
        not hasattr(arguments_type, argument_key) and len(argument_type_universe.intersection({Any, None})) == 0
    )

    # Default argument value reader.
    if argument_value_reader is None:
        argument_type_universe_non_none_types = {
            _ if isinstance(_, type) else type(_) for _ in argument_type_universe.difference({None})
        }
        if len(argument_type_universe_non_none_types) == 1 and Any not in argument_type_universe_non_none_types:
            argument_type = argument_type_universe_non_none_types.pop()

            if argument_type is bool:
                # :py:func:`bool` returns `True` for any non-empty string.
                def argument_bool_reader(argument: str) -> Union[bool, str]:
                    # Boolean choices are rendered as `str(False)` and `str(True)` so case-insensitive match may cause confusion.
                    #
                    # Do a case-sensitive match instead.
                    if argument == str(False):
                        return False
                    elif argument == str(True):
                        return True
                    else:
                        return argument

                argument_choices = (False, True)
                argument_value_reader = argument_bool_reader
            elif argument_type in {complex, float, int, str, Path, PurePath}:
                argument_value_reader = argument_type
            elif issubclass(argument_type, Enum):
                # :py:meth:`enum.Enum.__getitem__` takes the enum name string.
                def argument_enum_reader(argument: str) -> Union[str, Enum]:
                    # :py:type:`ArgumentEnum` is rendered as the enum name with `_` replaced with `-`.
                    #
                    # Invert this to turn the argument into the enum name.
                    enum_name = argument.replace("-", "_") if issubclass(argument_type, ArgumentEnum) else argument
                    try:
                        return argument_type[enum_name]
                    except KeyError:
                        return argument

                argument_value_reader = argument_enum_reader

    return functools.partial(
        parser.add_argument,
        argument_flag,
        choices=argument_choices,
        default=argument_default,
        dest=argument_key,
        required=argument_required,
        type=argument_value_reader or (lambda _: _),
    )


T = TypeVar("T", bound=Arguments, contravariant=True)


class CommandFunction(Protocol[T]):
    """
    Command function.
    """

    ExitCode = Union[str | int | None]

    def __call__(self, arguments: T) -> ExitCode: ...


_COMMAND_FUNCTION_ARGUMENT_KEY: str = "argparse_extensions.func"


def set_command_function(parser: argparse.ArgumentParser, arguments_type: type[T], func: CommandFunction[T]) -> None:
    """
    Set the command function.

    :param parser: Argument parser.
    :param arguments_type: Arguments dataclass.
    :param func: Command function.
    """

    def _func(arguments: argparse.Namespace):
        # Translate from :py:type:`argparse.Namespace` to :py:type:`Arguments`.
        arguments_sans_func = vars(arguments)
        del arguments_sans_func[_COMMAND_FUNCTION_ARGUMENT_KEY]
        return func(arguments=arguments_type(**arguments_sans_func))

    parser.set_defaults(**{_COMMAND_FUNCTION_ARGUMENT_KEY: _func})


def run_cli(parser: argparse.ArgumentParser) -> NoReturn:
    """
    Run the CLI for the given parser.

    :param parser: Argument parser.
    """

    # Parse arguments.
    arguments = parser.parse_args(args=sys.argv[1:])

    # Check for a command function.
    assert hasattr(arguments, _COMMAND_FUNCTION_ARGUMENT_KEY), "Command has no command function!"

    # Call command function.
    sys.exit(vars(arguments)[_COMMAND_FUNCTION_ARGUMENT_KEY](arguments=arguments))
