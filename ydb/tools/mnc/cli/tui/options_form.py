from typing import Any, Dict, List, Optional

from textual.app import App
from textual.binding import Binding
from textual.containers import Container, ScrollableContainer
from textual.screen import ModalScreen
from textual.widgets import Input, Label, ListItem, ListView, Static

from ydb.tools.mnc.cli.tui import theme
from ydb.tools.mnc.cli.tui.common import field_id, option_label, value_to_text
import ydb.tools.mnc.scheme as scheme


DEPLOY_FLAGS_DEST = "deploy_flags"


SECTION_FLAGS = "flags"
SECTION_OPTIONS = "options"
SECTION_DEPLOY_FLAGS = "deploy_flags"
SECTION_ARGUMENTS = "arguments"


SECTION_TITLES = {
    SECTION_FLAGS: "Flags",
    SECTION_OPTIONS: "Options",
    SECTION_DEPLOY_FLAGS: "Deploy flags",
    SECTION_ARGUMENTS: "Arguments",
}


class _SectionRow(ListItem):
    def __init__(self, section: str):
        super().__init__(Static(f"[bold cyan]── {SECTION_TITLES[section]} ──[/bold cyan]", markup=True))
        self.section = section
        self.can_focus = False


class _OptionsListView(ListView):
    """List view that keeps section headers out of keyboard navigation."""

    BINDINGS = [
        Binding("j,down", "cursor_down", "Down", show=False),
        Binding("k,up", "cursor_up", "Up", show=False),
        Binding("g,home", "cursor_first", "First", show=False),
        Binding("G,end", "cursor_last", "Last", show=False),
    ]

    def action_cursor_down(self):
        self._move_to_focusable(+1)

    def action_cursor_up(self):
        self._move_to_focusable(-1)

    def action_cursor_first(self):
        self.index = self._first_focusable_index()

    def action_cursor_last(self):
        self.index = self._last_focusable_index()

    def _move_to_focusable(self, delta: int):
        if not self.children:
            return
        index = self.index if self.index is not None else 0
        max_index = len(self.children) - 1
        for _ in range(max_index + 1):
            index = max(0, min(max_index, index + delta))
            child = self.children[index]
            if not isinstance(child, _SectionRow):
                self.index = index
                return

    def _first_focusable_index(self) -> Optional[int]:
        for index, child in enumerate(self.children):
            if not isinstance(child, _SectionRow):
                return index
        return None

    def _last_focusable_index(self) -> Optional[int]:
        for index in range(len(self.children) - 1, -1, -1):
            child = self.children[index]
            if not isinstance(child, _SectionRow):
                return index
        return None


class _ModalListView(ListView):
    """Modal list with explicit Vim/home/end bindings scoped to the modal."""

    BINDINGS = [
        Binding("j,down", "cursor_down", "Down", show=False),
        Binding("k,up", "cursor_up", "Up", show=False),
        Binding("g,home", "cursor_first", "First", show=False),
        Binding("G,end", "cursor_last", "Last", show=False),
    ]

    def replace_items(self, items: List[ListItem], index: int = 0):
        """Replace rows after the new rows are ready to avoid a blank frame."""
        old_items = list(self.children)
        self.mount(*items)
        for old_item in old_items:
            old_item.remove()
        if items:
            self.index = min(index, len(items) - 1)
        else:
            self.index = None

    def action_cursor_first(self):
        if self.children:
            self.index = 0

    def action_cursor_last(self):
        if self.children:
            self.index = len(self.children) - 1


class _OptionRow(ListItem):
    def __init__(self, key: str, text: str):
        super().__init__(Static(text, markup=True))
        self.key = key


class _SubmitRow(ListItem):
    def __init__(self):
        super().__init__(Static("[bold green]→ Apply and continue[/bold green]", markup=True))


class _TextInputModal(ModalScreen[Optional[str]]):
    CSS = theme.SCREEN_CSS_MODAL
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+s", "submit", "Apply", priority=True),
    ]

    def __init__(self, title: str, value: str, help_text: str = "", placeholder: str = ""):
        super().__init__()
        self._title = title
        self._initial = value
        self._help = help_text
        self._placeholder = placeholder

    def compose(self):
        with Container(id="dialog"):
            yield Static(self._title, id="title")
            if self._help:
                yield Static(self._help, classes="hint")
            yield Input(value=self._initial, placeholder=self._placeholder, id="value")
            yield Static(theme.format_footer(theme.FOOTER_MODAL_INPUT), classes="hint")

    def on_mount(self):
        self.query_one("#value", Input).focus()

    def action_cancel(self):
        self.dismiss(None)

    def action_submit(self):
        self.dismiss(self.query_one("#value", Input).value)

    def on_input_submitted(self, event: Input.Submitted):
        event.stop()
        self.dismiss(event.value)


class _ChoiceModal(ModalScreen[Optional[str]]):
    CSS = theme.SCREEN_CSS_MODAL
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
    ]

    def __init__(self, title: str, choices: List[Any], current: str, help_text: str = "", allow_clear: bool = True):
        super().__init__()
        self._title = title
        self._choices = [str(choice) for choice in choices]
        self._current = current
        self._help = help_text
        self._allow_clear = allow_clear

    def compose(self):
        with Container(id="dialog"):
            yield Static(self._title, id="title")
            if self._help:
                yield Static(self._help, classes="hint")
            items = []
            if self._allow_clear:
                items.append(ListItem(Label("(unset)"), id="choice-_clear"))
            for index, choice in enumerate(self._choices):
                items.append(ListItem(Label(choice), id=f"choice-{index}"))
            yield _ModalListView(*items, id="choices")
            yield Static(theme.format_footer(theme.FOOTER_MODAL_CHOICE), classes="hint")

    def on_mount(self):
        list_view = self.query_one("#choices", ListView)
        list_view.focus()
        if self._current and self._current in self._choices:
            list_view.index = (1 if self._allow_clear else 0) + self._choices.index(self._current)

    def action_cancel(self):
        self.dismiss(None)

    def on_list_view_selected(self, event: ListView.Selected):
        event.stop()
        if event.item is None or event.item.id is None:
            return
        if event.item.id == "choice-_clear":
            self.dismiss("")
            return
        index = int(event.item.id.removeprefix("choice-"))
        self.dismiss(self._choices[index])


class _DeployFlagsApplyRow(ListItem):
    def __init__(self):
        super().__init__(Static("[green]→ Apply selected deploy flags[/green]", markup=True))


class _DeployFlagsModal(ModalScreen[Optional[List[str]]]):
    CSS = theme.SCREEN_CSS_MODAL
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("space", "toggle", "Toggle", show=False),
        Binding("enter", "toggle", "Toggle", show=False),
    ]

    def __init__(self, title: str, selected: List[str], help_text: str = ""):
        super().__init__()
        self._title = title
        self._selected = set(selected or [])
        self._help = help_text
        self._flags = list(scheme.common.deploy_flags)

    def compose(self):
        with Container(id="dialog"):
            yield Static(self._title, id="title")
            if self._help:
                yield Static(self._help, classes="hint")
            yield _ModalListView(id="flags")
            yield Static(theme.format_footer(theme.FOOTER_MODAL_MULTI), classes="hint")

    def on_mount(self):
        self._refresh()
        list_view = self.query_one("#flags", ListView)
        list_view.focus()
        list_view.index = 0

    def action_cancel(self):
        self.dismiss(None)

    def action_submit(self):
        self.dismiss(sorted(self._selected, key=self._flags.index))

    def action_toggle(self):
        list_view = self.query_one("#flags", ListView)
        if list_view.index is None:
            return
        item = list_view.children[list_view.index]
        if isinstance(item, _DeployFlagsApplyRow):
            self.action_submit()
            return
        flag = self._flags[list_view.index]
        if flag in self._selected:
            self._selected.discard(flag)
        else:
            self._selected.add(flag)
            opposite = scheme.common.opposite_deploy_flags.get(flag)
            if opposite is not None:
                self._selected.discard(opposite)
        self._refresh(preserve_index=list_view.index)

    def _refresh(self, preserve_index: int = 0):
        list_view = self.query_one("#flags", _ModalListView)
        items = []
        for flag in self._flags:
            mark = "[green]\u2713[/green]" if flag in self._selected else "[dim]□[/dim]"
            items.append(ListItem(Static(f"{mark}  {flag}", markup=True)))
        items.append(_DeployFlagsApplyRow())
        list_view.replace_items(items, index=preserve_index)


# -- Public helpers (used in tests too) -----------------------------------


def is_value_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value == ""
    if isinstance(value, (list, tuple, set)):
        return len(value) == 0
    return False


def is_value_changed(entry, value) -> bool:
    if entry["kind"] == "flag":
        return bool(value) != bool(entry.get("default", False))
    if entry["kind"] == "deploy_flags":
        default = entry.get("default") or []
        return list(value or []) != list(default)
    default_text = value_to_text(entry.get("default"))
    return value_to_text(value) != default_text


def format_value_for_display(entry, value) -> str:
    if entry["kind"] == "flag":
        return "[green]✓[/green]" if value else "[dim]□[/dim]"
    if entry["kind"] == "deploy_flags":
        return ", ".join(value) if value else "[dim](unset)[/dim]"
    text = value_to_text(value)
    if not text:
        return "[dim](unset)[/dim]"
    return text


def format_badges(entry, value) -> str:
    badges = []
    required = entry.get("required", False)
    missing = required and is_value_missing(value) and entry["kind"] != "flag"
    if missing:
        badges.append("[bold red]missing[/bold red]")
    elif required:
        badges.append("[yellow]required[/yellow]")
    if is_value_changed(entry, value) and entry["kind"] != "flag":
        badges.append("[cyan]changed[/cyan]")
    return " ".join(badges)


def format_row(entry, value) -> str:
    label = entry["label"]
    if entry["kind"] == "flag":
        rendered_value = format_value_for_display(entry, value)
        return f"{rendered_value} {label}"
    rendered_value = format_value_for_display(entry, value)
    badges = format_badges(entry, value)
    suffix = f"  {badges}" if badges else ""
    return f"{label} = {rendered_value}{suffix}"


def collect_missing_required(entries: List[Dict[str, Any]], values: Dict[str, Any]) -> List[Dict[str, Any]]:
    missing = []
    for entry in entries:
        if not entry.get("required"):
            continue
        if entry["kind"] == "flag":
            continue
        if is_value_missing(values.get(entry["key"])):
            missing.append(entry)
    return missing


class OptionsFormApp(App[dict]):
    CSS = theme.SCREEN_CSS_FORM
    BINDINGS = [
        Binding("escape,h,left", "cancel", "Cancel"),
        Binding("q", "cancel", "Quit", show=False),
        Binding("enter,l,right", "edit", "Edit", show=False),
        Binding("space", "edit", "Edit", show=False),
    ]

    def __init__(
        self,
        title: str,
        initial_args,
        options=None,
        arguments=None,
        wizard_step: Optional[str] = None,
        argv_preview: Optional[str] = None,
    ):
        super().__init__()
        self.title = title
        self.initial_args = initial_args
        options = list(options or [])
        self.deploy_flags_options = [opt for opt in options if opt.dest == DEPLOY_FLAGS_DEST]
        self.value_options = [opt for opt in options if opt.expects_value and opt.dest != DEPLOY_FLAGS_DEST]
        self.flag_options = [opt for opt in options if not opt.expects_value]
        self.arguments = list(arguments or [])
        self.wizard_step = wizard_step
        self.argv_preview = argv_preview

        self._entries: List[Dict[str, Any]] = []
        self._values: Dict[str, Any] = {}
        self._build_entries()

    def _build_entries(self):
        for option in self.flag_options:
            key = f"flag::{option.dest}"
            value = bool(getattr(self.initial_args, option.dest, option.default))
            self._values[key] = value
            self._entries.append({
                "key": key,
                "kind": "flag",
                "section": SECTION_FLAGS,
                "option": option,
                "label": option_label(option),
                "help": option.help,
                "required": False,
                "default": bool(option.default),
            })
        for option in self.value_options:
            key = f"opt::{option.dest}"
            current = getattr(self.initial_args, option.dest, option.default)
            self._values[key] = value_to_text(current)
            self._entries.append({
                "key": key,
                "kind": "value",
                "section": SECTION_OPTIONS,
                "option": option,
                "label": option_label(option),
                "help": option.help,
                "required": bool(option.required),
                "default": option.default,
            })
        for option in self.deploy_flags_options:
            key = f"opt::{option.dest}"
            current = getattr(self.initial_args, option.dest, option.default) or []
            self._values[key] = list(current)
            self._entries.append({
                "key": key,
                "kind": "deploy_flags",
                "section": SECTION_DEPLOY_FLAGS,
                "option": option,
                "label": option_label(option),
                "help": option.help,
                "required": bool(option.required),
                "default": list(option.default or []),
            })
        for argument in self.arguments:
            key = f"arg::{argument.dest}"
            current = getattr(self.initial_args, argument.dest, None)
            self._values[key] = value_to_text(current)
            self._entries.append({
                "key": key,
                "kind": "argument",
                "section": SECTION_ARGUMENTS,
                "argument": argument,
                "label": argument.name,
                "help": argument.help,
                "required": bool(argument.required),
                "default": None,
            })

    def compose(self):
        yield Static("", id="header", markup=True)
        with ScrollableContainer(id="form"):
            yield _OptionsListView(id="options-list")
        yield Static("", id="help", classes="hint", markup=True)
        yield Static("", id="status", classes="status-info", markup=True)
        yield Static(theme.format_footer(theme.FOOTER_FORM), id="footer", markup=True)

    def on_mount(self):
        self._refresh_header()
        self._refresh_list()
        self.query_one("#options-list", ListView).focus()

    # Actions ---------------------------------------------------------

    def action_cancel(self):
        from ydb.tools.mnc.cli.tui.command_picker import _ConfirmQuitModal

        def handle(confirmed):
            if confirmed:
                self.exit(None)

        self.push_screen(_ConfirmQuitModal("Discard changes and quit?"), handle)

    def action_submit(self):
        self._submit()

    def action_edit(self):
        list_view = self.query_one("#options-list", ListView)
        if list_view.index is None:
            return
        item = list_view.children[list_view.index] if list_view.index < len(list_view.children) else None
        if isinstance(item, _SubmitRow):
            self._submit()
            return
        if isinstance(item, _OptionRow):
            for entry in self._entries:
                if entry["key"] == item.key:
                    self._open_editor(entry)
                    return

    # Events ----------------------------------------------------------

    def on_list_view_highlighted(self, event: ListView.Highlighted):
        event.stop()
        if isinstance(event.item, _SubmitRow):
            self.query_one("#help", Static).update("[dim]Validate and continue[/dim]")
            return
        if isinstance(event.item, _SectionRow):
            # Section rows are not focusable navigation targets; clear help
            # but do not auto-jump (direction is unknown from this event,
            # and auto-jumping forward breaks Up navigation).
            self.query_one("#help", Static).update("")
            return
        if not isinstance(event.item, _OptionRow):
            self.query_one("#help", Static).update("")
            return
        for entry in self._entries:
            if entry["key"] == event.item.key:
                self.query_one("#help", Static).update(entry["help"] or "")
                return

    def on_list_view_selected(self, event: ListView.Selected):
        event.stop()
        if isinstance(event.item, _SubmitRow):
            self._submit()
            return
        if not isinstance(event.item, _OptionRow):
            return
        for entry in self._entries:
            if entry["key"] == event.item.key:
                self._open_editor(entry)
                return

    # Internals -------------------------------------------------------

    def _refresh_header(self):
        self.query_one("#header", Static).update(
            theme.format_header(self.title, step=self.wizard_step, preview=self.argv_preview)
        )

    def _open_editor(self, entry):
        key = entry["key"]
        kind = entry["kind"]
        if kind == "flag":
            self._values[key] = not self._values[key]
            self._refresh_list(preserve_selection=True)
            return
        if kind == "deploy_flags":
            self.push_screen(
                _DeployFlagsModal(entry["label"], self._values[key], help_text=entry["help"] or ""),
                self._make_callback(key, list),
            )
            return
        option_or_arg = entry.get("option") or entry.get("argument")
        choices = getattr(option_or_arg, "choices", None)
        help_text = entry["help"] or ""
        if choices:
            self.push_screen(
                _ChoiceModal(entry["label"], choices, self._values[key], help_text=help_text),
                self._make_callback(key, str),
            )
            return
        placeholder = getattr(option_or_arg, "metavar", None) or (getattr(option_or_arg, "dest", None) or "")
        self.push_screen(
            _TextInputModal(entry["label"], self._values[key], help_text=help_text, placeholder=placeholder or ""),
            self._make_callback(key, str),
        )

    def _make_callback(self, key: str, expected_type):
        def callback(result):
            if result is None:
                return
            self._values[key] = list(result) if expected_type is list else result
            self._refresh_list(preserve_selection=True)
        return callback

    def _refresh_list(self, preserve_selection: bool = False):
        list_view = self.query_one("#options-list", ListView)
        previous_index = list_view.index if preserve_selection else None
        list_view.clear()

        items: List[ListItem] = []
        for section in (SECTION_FLAGS, SECTION_OPTIONS, SECTION_DEPLOY_FLAGS, SECTION_ARGUMENTS):
            section_entries = [entry for entry in self._entries if entry["section"] == section]
            if not section_entries:
                continue
            items.append(_SectionRow(section))
            for entry in section_entries:
                items.append(_OptionRow(entry["key"], format_row(entry, self._values[entry["key"]])))
        items.append(_SubmitRow())

        list_view.extend(items)
        if previous_index is None or previous_index >= len(items):
            previous_index = list_view._first_focusable_index() or 0
        list_view.index = previous_index

    def _submit(self):
        missing = collect_missing_required(self._entries, self._values)
        if missing:
            labels = ", ".join(entry["label"] for entry in missing)
            self._set_status(f"Required fields are empty: {labels}", level="error")
            self._focus_entry(missing[0]["key"])
            return

        result = {"flags": {}, "options": {}, "arguments": {}}
        for entry in self._entries:
            value = self._values[entry["key"]]
            if entry["kind"] == "flag":
                result["flags"][entry["option"].dest] = bool(value)
            elif entry["kind"] == "value":
                result["options"][entry["option"].dest] = value
            elif entry["kind"] == "deploy_flags":
                result["options"][entry["option"].dest] = list(value)
            elif entry["kind"] == "argument":
                result["arguments"][entry["argument"].dest] = value
        self.exit(result)

    def _focus_entry(self, key: str):
        list_view = self.query_one("#options-list", ListView)
        for index, child in enumerate(list_view.children):
            if isinstance(child, _OptionRow) and child.key == key:
                list_view.index = index
                return

    def _set_status(self, message: str, level: str = "info"):
        widget = self.query_one("#status", Static)
        widget.update(theme.status_line(message, level=level))
        widget.set_classes(f"status-{level}")


def _deploy_flag_field_id(deploy_flag: str) -> str:
    return field_id("deploy-flag", deploy_flag)


def _deploy_flag_from_field_id(widget_id: str):
    prefix = "deploy-flag-"
    if not widget_id.startswith(prefix):
        return None
    return widget_id[len(prefix):].replace("-", "_")
