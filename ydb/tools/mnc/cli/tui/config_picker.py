from typing import Dict, List, Optional

from textual.app import App
from textual.containers import Horizontal, Vertical
from textual.widgets import Label, ListItem, ListView, Static

from ydb.tools.mnc.cli.tui import theme
from ydb.tools.mnc.cli.tui.command_picker import _ConfirmQuitModal
from ydb.tools.mnc.cli.tui.common import (
    ConfigCandidate,
    config_preview,
    config_validation_errors,
)


COMPAT_OK = "ok"
COMPAT_BAD = "bad"
COMPAT_UNKNOWN = "unknown"


def _compat_marker(status: str) -> str:
    if status == COMPAT_OK:
        return "[green]✓[/green]"
    if status == COMPAT_BAD:
        return "[red]![/red]"
    return "[dim]?[/dim]"


class _ConfigListItem(ListItem):
    def __init__(self, candidate: ConfigCandidate, status: str = COMPAT_UNKNOWN):
        marker = _compat_marker(status)
        super().__init__(Label(f"{marker} {candidate.name}  [dim]{candidate.path}[/dim]", markup=True))
        self.candidate = candidate
        self.compat_status = status


class ConfigPickerApp(App[tuple[str, str]]):
    CSS = theme.SCREEN_CSS_TWO_PANE
    BINDINGS = theme.navigation_bindings(
        enter_action="select_current",
        enter_label="Select",
        back_action="cancel",
        back_label="Cancel",
    )

    def __init__(
        self,
        candidates: List[ConfigCandidate],
        command_scheme=None,
        wizard_step: Optional[str] = None,
    ):
        super().__init__()
        self.candidates = candidates
        self.command_scheme = command_scheme
        self.wizard_step = wizard_step
        self._compat_cache: Dict[str, List[str]] = {}
        self._compat_status: Dict[str, str] = {}
        self._compute_compatibility()

    def _compute_compatibility(self):
        for candidate in self.candidates:
            errors = config_validation_errors(candidate, self.command_scheme)
            self._compat_cache[candidate.path] = errors
            if self.command_scheme is None:
                self._compat_status[candidate.path] = COMPAT_UNKNOWN
            elif errors:
                self._compat_status[candidate.path] = COMPAT_BAD
            else:
                self._compat_status[candidate.path] = COMPAT_OK

    def compose(self):
        yield Static("", id="header", markup=True)
        with Horizontal(id="body"):
            with Vertical(id="left"):
                yield ListView(id="configs")
            with Vertical(id="right"):
                yield Static("", id="details", markup=True)
        yield Static("", id="status", classes="status-info", markup=True)
        yield Static(theme.format_footer(theme.FOOTER_PICKER), id="footer", markup=True)

    def on_mount(self):
        self._refresh()
        self.query_one("#configs", ListView).focus()

    # Actions ---------------------------------------------------------

    def action_cancel(self):
        def handle(confirmed):
            if confirmed:
                self.exit(None)

        self.push_screen(_ConfirmQuitModal("Quit the wizard?"), handle)

    def action_cursor_down(self):
        self.query_one("#configs", ListView).action_cursor_down()

    def action_cursor_up(self):
        self.query_one("#configs", ListView).action_cursor_up()

    def action_cursor_first(self):
        list_view = self.query_one("#configs", ListView)
        if list_view.children:
            list_view.index = 0

    def action_cursor_last(self):
        list_view = self.query_one("#configs", ListView)
        if list_view.children:
            list_view.index = len(list_view.children) - 1

    def action_select_current(self):
        list_view = self.query_one("#configs", ListView)
        if list_view.highlighted_child is not None:
            self._activate(list_view.highlighted_child)

    # Events ----------------------------------------------------------

    def on_list_view_highlighted(self, event: ListView.Highlighted):
        event.stop()
        if isinstance(event.item, _ConfigListItem):
            self._show_details(event.item.candidate)
            self._update_status_for(event.item)

    def on_list_view_selected(self, event: ListView.Selected):
        event.stop()
        self._activate(event.item)

    # Internals -------------------------------------------------------

    def _activate(self, item):
        if not isinstance(item, _ConfigListItem):
            return
        if item.compat_status == COMPAT_BAD:
            self._set_status(
                "Config is incompatible with the selected command. Choose a compatible config or fix this one.",
                level="error",
            )
            return
        self.exit(("--config", item.candidate.name))

    def _refresh(self):
        list_view = self.query_one("#configs", ListView)
        list_view.clear()
        items = [
            _ConfigListItem(candidate, self._compat_status.get(candidate.path, COMPAT_UNKNOWN))
            for candidate in self.candidates
        ]
        list_view.extend(items)
        list_view.index = self._first_compatible_index(items)
        self.query_one("#header", Static).update(
            theme.format_header("Select config", step=self.wizard_step)
        )
        if items and list_view.index is not None:
            current = items[list_view.index]
            self._show_details(current.candidate)
            self._update_status_for(current)
        elif not items:
            self.query_one("#details", Static).update("No configs found")
            self._set_status("No configs discovered", level="warning")

    def _first_compatible_index(self, items: List[_ConfigListItem]) -> Optional[int]:
        if not items:
            return None
        for index, item in enumerate(items):
            if item.compat_status != COMPAT_BAD:
                return index
        return 0

    def _show_details(self, candidate: ConfigCandidate):
        self.query_one("#details", Static).update(config_preview(candidate, self.command_scheme))

    def _update_status_for(self, item: _ConfigListItem):
        if item.compat_status == COMPAT_BAD:
            errors = self._compat_cache.get(item.candidate.path, [])
            summary = errors[0] if errors else "incompatible config"
            self._set_status(f"Incompatible: {summary}", level="error")
        elif item.compat_status == COMPAT_OK:
            self._set_status("Compatible with selected command", level="success")
        else:
            self._set_status("", level="info")

    def _set_status(self, message: str, level: str = "info"):
        widget = self.query_one("#status", Static)
        widget.update(theme.status_line(message, level=level))
        widget.set_classes(f"status-{level}")
