from typing import Any, Dict, List, Optional

from textual.app import App
from textual.binding import Binding
from textual.containers import Container, ScrollableContainer
from textual.screen import ModalScreen
from textual.widgets import Checkbox, Input, Label, ListItem, ListView, Static

from ydb.tools.mnc.cli.tui.common import field_id, option_label, value_to_text
import ydb.tools.mnc.scheme as scheme


DEPLOY_FLAGS_DEST = "deploy_flags"


class _OptionRow(ListItem):
    def __init__(self, key: str, text: str):
        super().__init__(Static(text, markup=True))
        self.key = key


class _NextRow(ListItem):
    def __init__(self):
        super().__init__(Static("→ Next"))


class _TextInputModal(ModalScreen[Optional[str]]):
    CSS = """
    _TextInputModal { align: center middle; background: transparent; }
    #dialog { width: 70%; height: auto; padding: 0 1; border: solid $primary; background: transparent; }
    #title { text-style: bold; color: $accent; }
    .hint { color: $text-muted; }
    Input { background: transparent; border: solid $primary; }
    """
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
            yield Static("[dim]Enter: apply    Esc: cancel[/dim]", classes="hint")

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
    CSS = """
    _ChoiceModal { align: center middle; background: transparent; }
    #dialog { width: 60%; height: auto; max-height: 80%; padding: 0 1; border: solid $primary; background: transparent; }
    #title { text-style: bold; color: $accent; }
    .hint { color: $text-muted; }
    ListView { background: transparent; max-height: 18; border: solid $primary; }
    ListItem { background: transparent; padding: 0 1; }
    ListItem.--highlight { background: $primary; text-style: bold; }
    """
    BINDINGS = [Binding("escape", "cancel", "Cancel")]

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
            yield ListView(*items, id="choices")
            yield Static("[dim]Enter: select    Esc: cancel[/dim]", classes="hint")

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


class _DeployFlagsModal(ModalScreen[Optional[List[str]]]):
    CSS = """
    _DeployFlagsModal { align: center middle; background: transparent; }
    #dialog { width: 60%; height: auto; max-height: 80%; padding: 0 1; border: solid $primary; background: transparent; }
    #title { text-style: bold; color: $accent; }
    .hint { color: $text-muted; }
    ListView { background: transparent; max-height: 18; border: solid $primary; }
    ListItem { background: transparent; padding: 0 1; }
    ListItem.--highlight { background: $primary; text-style: bold; }
    """
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+s", "submit", "Apply", priority=True),
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
            yield ListView(id="flags")
            yield Static("[dim]Space/Enter: toggle    Ctrl+S: apply    Esc: cancel[/dim]", classes="hint")

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
        list_view = self.query_one("#flags", ListView)
        list_view.clear()
        items = []
        for flag in self._flags:
            mark = "[green]\u2713[/green]" if flag in self._selected else " "
            items.append(ListItem(Static(f"{mark}  {flag}", markup=True)))
        list_view.extend(items)
        if items:
            list_view.index = min(preserve_index, len(items) - 1)


class OptionsFormApp(App[dict]):
    CSS = """
    Screen { layout: vertical; background: transparent; }
    #title { height: 1; padding: 0 1; text-style: bold; color: $accent; }
    #form { height: 1fr; padding: 0 1; border: solid $primary; background: transparent; }
    ListView { background: transparent; }
    ListItem { background: transparent; padding: 0 1; height: 1; }
    ListItem { color: $text-muted; }
    ListItem.--highlight { background: transparent; color: $text; }
    ListView:focus > ListItem.--highlight { background: transparent; color: $accent; }
    #help { height: 1; padding: 0 1; color: $text-muted; }
    #footer { height: 1; padding: 0 1; color: $text-muted; }
    """
    BINDINGS = [
        Binding("escape", "cancel", "Cancel"),
        Binding("ctrl+s", "submit", "Apply", priority=True),
        Binding("enter", "edit", "Edit", show=False),
        Binding("space", "edit", "Edit", show=False),
    ]

    def __init__(self, title: str, initial_args, options=None, arguments=None):
        super().__init__()
        self.title = title
        self.initial_args = initial_args
        options = list(options or [])
        self.deploy_flags_options = [opt for opt in options if opt.dest == DEPLOY_FLAGS_DEST]
        self.value_options = [opt for opt in options if opt.expects_value and opt.dest != DEPLOY_FLAGS_DEST]
        self.flag_options = [opt for opt in options if not opt.expects_value]
        self.arguments = list(arguments or [])

        self._entries: List[Dict[str, Any]] = []
        self._values: Dict[str, Any] = {}
        self._build_entries()

    def _build_entries(self):
        for option in self.flag_options:
            key = f"flag::{option.dest}"
            value = bool(getattr(self.initial_args, option.dest, option.default))
            self._values[key] = value
            self._entries.append({"key": key, "kind": "flag", "option": option, "label": option_label(option), "help": option.help})
        for option in self.value_options:
            key = f"opt::{option.dest}"
            current = getattr(self.initial_args, option.dest, option.default)
            self._values[key] = value_to_text(current)
            self._entries.append({"key": key, "kind": "value", "option": option, "label": option_label(option), "help": option.help})
        for option in self.deploy_flags_options:
            key = f"opt::{option.dest}"
            current = getattr(self.initial_args, option.dest, option.default) or []
            self._values[key] = list(current)
            self._entries.append({"key": key, "kind": "deploy_flags", "option": option, "label": option_label(option), "help": option.help})
        for argument in self.arguments:
            key = f"arg::{argument.dest}"
            current = getattr(self.initial_args, argument.dest, None)
            self._values[key] = value_to_text(current)
            self._entries.append({"key": key, "kind": "argument", "argument": argument, "label": argument.name, "help": argument.help})

    def compose(self):
        yield Static(self.title, id="title")
        with ScrollableContainer(id="form"):
            yield ListView(id="options-list")
        yield Static("", id="help")
        yield Static("[dim]Enter/Space: edit    Ctrl+S: apply    Esc: cancel[/dim]", id="footer")

    def on_mount(self):
        self._refresh_list()
        self.query_one("#options-list", ListView).focus()

    def action_cancel(self):
        self.exit(None)

    def action_submit(self):
        self._submit()

    def action_edit(self):
        list_view = self.query_one("#options-list", ListView)
        if list_view.index is None:
            return
        if list_view.index >= len(self._entries):
            self._submit()
            return
        self._open_editor(self._entries[list_view.index])

    def on_list_view_highlighted(self, event: ListView.Highlighted):
        event.stop()
        if isinstance(event.item, _NextRow):
            self.query_one("#help", Static).update("[dim]Apply and continue[/dim]")
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
        if isinstance(event.item, _NextRow):
            self._submit()
            return
        if not isinstance(event.item, _OptionRow):
            return
        for entry in self._entries:
            if entry["key"] == event.item.key:
                self._open_editor(entry)
                return

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

    def _format_value(self, entry) -> str:
        value = self._values[entry["key"]]
        if entry["kind"] == "flag":
            return "on" if value else "off"
        if entry["kind"] == "deploy_flags":
            return ", ".join(value) if value else "(unset)"
        text = value_to_text(value)
        return text if text else "(unset)"

    def _format_row(self, entry) -> str:
        return f"{entry['label']} = {self._format_value(entry)}"

    def _refresh_list(self, preserve_selection: bool = False):
        list_view = self.query_one("#options-list", ListView)
        previous_index = list_view.index if preserve_selection else 0
        list_view.clear()
        items: List[ListItem] = [_OptionRow(entry["key"], self._format_row(entry)) for entry in self._entries]
        items.append(_NextRow())
        list_view.extend(items)
        if previous_index is None or previous_index >= len(items):
            previous_index = 0
        list_view.index = previous_index

    def _submit(self):
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


def _deploy_flag_field_id(deploy_flag: str) -> str:
    return field_id("deploy-flag", deploy_flag)


def _deploy_flag_from_field_id(widget_id: str):
    prefix = "deploy-flag-"
    if not widget_id.startswith(prefix):
        return None
    return widget_id[len(prefix):].replace("-", "_")
