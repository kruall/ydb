from typing import List, Optional

from textual.app import App
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, ListItem, ListView, Static

from ydb.tools.mnc.cli import arg_metadata
from ydb.tools.mnc.cli.tui import theme


class _ConfirmQuitModal(ModalScreen[bool]):
    CSS = theme.SCREEN_CSS_MODAL
    BINDINGS = [
        Binding("escape", "cancel", "No"),
        Binding("n", "cancel", "No"),
        Binding("y", "confirm", "Yes"),
        Binding("enter", "confirm", "Yes"),
    ]

    def __init__(self, message: str = "Quit the wizard?"):
        super().__init__()
        self._message = message

    def compose(self):
        with Container(id="dialog"):
            yield Static("Confirm quit", id="title")
            yield Static(self._message)
            yield Static("[bold]y[/bold]/Enter: Yes    [bold]n[/bold]/Esc: No", classes="hint", markup=True)

    def action_cancel(self):
        self.dismiss(False)

    def action_confirm(self):
        self.dismiss(True)


class _CommandListItem(ListItem):
    def __init__(self, command: arg_metadata.CommandMeta):
        super().__init__(Label(command.name))
        self.command = command


class _BackListItem(ListItem):
    def __init__(self):
        super().__init__(Label(".. back"))


class CommandPickerApp(App[arg_metadata.CommandMeta]):
    CSS = theme.SCREEN_CSS_TWO_PANE
    BINDINGS = theme.navigation_bindings(
        enter_action="select_current",
        enter_label="Select",
        back_action="back_or_cancel",
        back_label="Back",
    )

    def __init__(
        self,
        root: arg_metadata.CommandMeta,
        initial: Optional[arg_metadata.CommandMeta] = None,
        wizard_step: Optional[str] = None,
    ):
        super().__init__()
        self.root = root
        self.command_parent = initial or root
        self.stack = command_stack(root, self.command_parent)
        self.wizard_step = wizard_step

    def compose(self):
        yield Static("", id="header", markup=True)
        with Horizontal(id="body"):
            with Vertical(id="left"):
                yield ListView(id="commands")
            with Vertical(id="right"):
                yield Static("", id="details", markup=True)
        yield Static("", id="status", classes="status-info", markup=True)
        yield Static(theme.format_footer(theme.FOOTER_PICKER), id="footer", markup=True)

    def on_mount(self):
        self._refresh()
        self.query_one("#commands", ListView).focus()

    # Actions ---------------------------------------------------------

    def action_back_or_cancel(self):
        if self._pop_command_level():
            self._refresh()
            return

        def handle(confirmed):
            if confirmed:
                self.exit(None)

        self.push_screen(_ConfirmQuitModal("Quit the wizard?"), handle)

    def action_cursor_down(self):
        self.query_one("#commands", ListView).action_cursor_down()

    def action_cursor_up(self):
        self.query_one("#commands", ListView).action_cursor_up()

    def action_cursor_first(self):
        list_view = self.query_one("#commands", ListView)
        if list_view.children:
            list_view.index = 0

    def action_cursor_last(self):
        list_view = self.query_one("#commands", ListView)
        if list_view.children:
            list_view.index = len(list_view.children) - 1

    def action_select_current(self):
        list_view = self.query_one("#commands", ListView)
        if list_view.highlighted_child is not None:
            self._activate(list_view.highlighted_child)

    # Events ----------------------------------------------------------

    def on_list_view_highlighted(self, event: ListView.Highlighted):
        event.stop()
        if isinstance(event.item, _CommandListItem):
            self._show_details(event.item.command)
        elif isinstance(event.item, _BackListItem) and len(self.stack) > 1:
            self._show_details(self.stack[-2])

    def on_list_view_selected(self, event: ListView.Selected):
        event.stop()
        self._activate(event.item)

    # Internals -------------------------------------------------------

    def _activate(self, item):
        if isinstance(item, _CommandListItem):
            command = item.command
            if command.children:
                self.stack.append(command)
                self.command_parent = command
                self._refresh()
            else:
                self.exit(command)
        elif isinstance(item, _BackListItem):
            if self._pop_command_level():
                self._refresh()

    def _pop_command_level(self) -> bool:
        if len(self.stack) <= 1:
            return False
        self.stack.pop()
        self.command_parent = self.stack[-1]
        return True

    def _refresh(self):
        list_view = self.query_one("#commands", ListView)
        list_view.clear()
        items = []
        if len(self.stack) > 1:
            items.append(_BackListItem())
        items.extend(_CommandListItem(command) for command in self.command_parent.children)
        list_view.extend(items)
        list_view.index = 0 if items else None
        self.query_one("#header", Static).update(
            theme.format_header(
                "Select command: " + command_path_text(self.command_parent),
                step=self.wizard_step,
            )
        )
        if items:
            first = items[0]
            if isinstance(first, _CommandListItem):
                self._show_details(first.command)
            else:
                self._show_details(self.stack[-2])
        else:
            self.query_one("#details", Static).update("No commands")

    def _show_details(self, command: arg_metadata.CommandMeta):
        self.query_one("#details", Static).update(arg_metadata.command_summary_text(command))


def command_stack(root: arg_metadata.CommandMeta, command: arg_metadata.CommandMeta) -> List[arg_metadata.CommandMeta]:
    if command is root:
        return [root]
    stack = [root]
    for part in command.path:
        current = stack[-1]
        for child in current.children:
            if child.name == part:
                stack.append(child)
                break
        else:
            return [root]
    return stack


def command_path_text(command: arg_metadata.CommandMeta) -> str:
    return " ".join(command.path) if command.path else "mnc"
