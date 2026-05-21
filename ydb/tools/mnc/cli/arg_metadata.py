import contextlib
import io
from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class OptionMeta:
    aliases: List[str]
    dest: Optional[str]
    help: str = ""
    group: str = ""
    default: Any = None
    required: bool = False
    choices: Optional[List[Any]] = None
    expects_value: bool = True
    multivalue: bool = False
    metavar: Optional[str] = None


@dataclass
class ArgumentMeta:
    name: str
    dest: Optional[str]
    help: str = ""
    required: bool = False
    choices: Optional[List[Any]] = None
    multivalue: bool = False
    metavar: Optional[str] = None


@dataclass
class CommandMeta:
    name: str
    path: List[str]
    help: str = ""
    description: str = ""
    parser: Any = None
    children: List["CommandMeta"] = field(default_factory=list)
    options: List[OptionMeta] = field(default_factory=list)
    arguments: List[ArgumentMeta] = field(default_factory=list)

    @property
    def is_leaf(self) -> bool:
        return not self.children


def command_metadata_from_parser(parser) -> CommandMeta:
    return _command_metadata_from_parser(parser, [])


def find_command(root: CommandMeta, path: List[str]) -> Optional[CommandMeta]:
    current = root
    for name in path:
        for child in current.children:
            if child.name == name:
                current = child
                break
        else:
            return None
    return current


def command_help_text(command: CommandMeta) -> str:
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        command.parser._print_help()
    return stream.getvalue().strip()


def command_summary_text(command: CommandMeta) -> str:
    """Render a structured, TUI-friendly summary of a command.

    Includes path, description, arguments, choices, child commands, and
    options. Uses Rich markup tags (no Textual CSS variables).
    """
    lines = []
    path_text = " ".join(command.path) if command.path else "mnc"
    lines.append(f"[bold cyan]{path_text}[/bold cyan]")

    description = command.description or command.help
    if description:
        lines.append("")
        lines.append(description.strip())
    else:
        lines.append("")
        lines.append("[dim](no description)[/dim]")

    if command.arguments:
        lines.append("")
        lines.append("[bold]Arguments[/bold]")
        for argument in command.arguments:
            marker = "[red]*[/red] " if argument.required else "  "
            help_text = (argument.help or "").strip()
            choices = ""
            if argument.choices:
                choices = f"  [dim]choices: {', '.join(map(str, argument.choices))}[/dim]"
            lines.append(f"{marker}[bold]{argument.name}[/bold]{choices}")
            if help_text:
                lines.append(f"    [dim]{help_text}[/dim]")

    required_options = [option for option in command.options if option.required]
    optional_options = [
        option for option in command.options
        if not option.required and option.group not in ("common", "Common arguments")
    ]
    if required_options:
        lines.append("")
        lines.append("[bold]Required options[/bold]")
        for option in required_options:
            aliases = ", ".join(option.aliases)
            lines.append(f"  [bold]{aliases}[/bold]")
            if option.help:
                lines.append(f"    [dim]{option.help.strip()}[/dim]")
    if optional_options:
        lines.append("")
        lines.append("[bold]Options[/bold]")
        for option in optional_options:
            aliases = ", ".join(option.aliases)
            default = ""
            if option.default not in (None, "", [], False):
                default = f"  [dim](default: {option.default})[/dim]"
            lines.append(f"  {aliases}{default}")
            if option.help:
                lines.append(f"    [dim]{option.help.strip()}[/dim]")

    if command.children:
        lines.append("")
        lines.append("[bold]Subcommands[/bold]")
        for child in command.children:
            help_text = (child.help or "").strip()
            suffix = f"  [dim]{help_text}[/dim]" if help_text else ""
            lines.append(f"  [bold]{child.name}[/bold]{suffix}")

    return "\n".join(lines)


def _command_metadata_from_parser(parser, parent_path: List[str]) -> CommandMeta:
    path = parent_path + ([] if parser._parent is None else [parser.metainfo.name])
    command = CommandMeta(
        name=parser.metainfo.name,
        path=path,
        help=parser.metainfo.help or "",
        description=parser.metainfo.description or "",
        parser=parser,
    )
    command.options = _options_from_parser(parser)
    command.arguments = _arguments_from_parser(parser)
    if parser._subparsers is not None:
        command.children = [
            _command_metadata_from_parser(child, path)
            for child in parser._subparsers._subparsers
        ]
    return command


def _options_from_parser(parser) -> List[OptionMeta]:
    options = []
    seen = set()
    for group in parser._option_groups:
        for option_name in group._options:
            arg = parser._option_dict[option_name]
            if id(arg) in seen:
                continue
            seen.add(id(arg))
            if "--help" in arg.metainfo.aliases:
                continue
            value_meta = arg._value.metainfo if arg._value is not None else None
            options.append(OptionMeta(
                aliases=list(arg.metainfo.aliases),
                dest=getattr(value_meta, "name", None),
                help=arg.metainfo.help or "",
                group=getattr(group, "_title", ""),
                default=getattr(value_meta, "default", None),
                required=bool(getattr(value_meta, "required", False)),
                choices=list(value_meta.choices) if getattr(value_meta, "choices", None) is not None else None,
                expects_value=arg.metainfo.is_expecting_value,
                multivalue=arg.metainfo.multivalue,
                metavar=arg.metainfo.metavar,
            ))
    return options


def _arguments_from_parser(parser) -> List[ArgumentMeta]:
    arguments = []
    for arg in parser._free_arguments:
        value_meta = arg._value.metainfo if arg._value is not None else None
        arguments.append(ArgumentMeta(
            name=arg.metainfo.name,
            dest=getattr(value_meta, "name", None),
            help=arg.metainfo.help or "",
            required=bool(getattr(value_meta, "required", False)),
            choices=list(value_meta.choices) if getattr(value_meta, "choices", None) is not None else None,
            multivalue=arg.metainfo.multivalue,
            metavar=arg.metainfo.metavar,
        ))
    return arguments
