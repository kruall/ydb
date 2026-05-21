import asyncio

from textual.app import App
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.widgets import Static

from ydb.tools.mnc.cli.tui import theme
from ydb.tools.mnc.lib import output, progress
from ydb.tools.mnc.lib.exceptions import CliError, ConfigError
from ydb.tools.mnc.lib.progress_live import LiveBackend


class RuntimeProgressApp(App):
    CSS = theme.SCREEN_CSS_RUNTIME
    BINDINGS = [
        Binding("up,k", "cursor_up", "Up", show=False),
        Binding("down,j", "cursor_down", "Down", show=False),
        Binding("enter,space,l", "toggle_selected", "Toggle", show=False),
        Binding("ctrl+c", "interrupt", "Interrupt", priority=True),
        Binding("q,escape", "close_finished", "Close", show=False),
    ]

    def __init__(self, backend, action, progress_context, result_from_exception, refresh_per_second: int):
        super().__init__()
        self.backend = backend
        self.action = action
        self.progress_context = progress_context
        self.result_from_exception = result_from_exception
        self.refresh_per_second = refresh_per_second
        self.error = None
        self._finished = False
        self._action_task = None
        self._interrupt_requested = False

    def compose(self):
        yield Static(theme.format_header("Running command"), id="header")
        with Horizontal(id="body"):
            with Vertical(id="left"):
                yield Static(id="steps")
            with Vertical(id="right"):
                yield Static(id="node-details")
                yield Static(id="logs")
        yield Static("", id="status", classes="status-info")
        yield Static(theme.format_footer(theme.FOOTER_RUNTIME_RUNNING), id="footer")

    def on_mount(self):
        self._refresh()
        self.set_interval(1 / max(1, self.refresh_per_second), self._refresh)
        self._action_task = asyncio.create_task(self._run_action())

    def action_cursor_up(self):
        self.backend.move_selection(-1)
        self._refresh()

    def action_cursor_down(self):
        self.backend.move_selection(1)
        self._refresh()

    def action_toggle_selected(self):
        self.backend.toggle_selected()
        self._refresh()

    def action_interrupt(self):
        if self._finished:
            return
        if self._action_task is not None and not self._action_task.done():
            self._interrupt_requested = True
            self._action_task.cancel()
            self._set_status("Interrupting...", level="warning")

    def action_close_finished(self):
        if self._finished:
            self.exit(self.backend.result)

    async def _run_action(self):
        try:
            result = await self.action(self.progress_context)
            self.backend.result = result
        except asyncio.CancelledError:
            if self._interrupt_requested:
                self.backend.result = progress.TaskResult(
                    level=progress.TaskResultLevel.ERROR,
                    step_title="Command",
                    message="Interrupted by user",
                )
                self._mark_finished()
                return
            return
        except Exception as exc:
            self.error = exc
            self.backend.result = self.result_from_exception(exc)
        self._mark_finished()

    def _mark_finished(self):
        self._finished = True
        self._refresh()
        try:
            self.query_one("#footer", Static).update(
                theme.format_footer(theme.FOOTER_RUNTIME_FINISHED)
            )
        except NoMatches:
            pass
        result = self.backend.result
        if isinstance(result, progress.TaskResult) and result.level == progress.TaskResultLevel.ERROR:
            self._set_status("Command failed. Press q or Enter to close.", level="error")
        elif self.error is not None:
            self._set_status("Command failed. Press q or Enter to close.", level="error")
        else:
            self.exit(self.backend.result)

    def _refresh(self):
        try:
            self.query_one("#steps", Static).update(self.backend.render_tree_body())
            self.query_one("#node-details", Static).update(self.backend.render_details_body())
            self.query_one("#logs", Static).update(self.backend.render_logs_body())
        except NoMatches:
            return

    def _set_status(self, message: str, level: str = "info"):
        try:
            widget = self.query_one("#status", Static)
            widget.update(theme.status_line(message, level=level))
            widget.set_classes(f"status-{level}")
        except NoMatches:
            return


class TuiApp:
    def __init__(self, console=None, refresh_per_second: int = 4):
        self.console = console or output.get_console()
        self.backend = LiveBackend(console=self.console)
        self.refresh_per_second = refresh_per_second

    def _result_from_exception(self, error: Exception) -> progress.TaskResult:
        if isinstance(error, CliError) and isinstance(error.result, progress.TaskResult):
            return error.result
        if isinstance(error, (CliError, ConfigError)):
            return progress.TaskResult(level=progress.TaskResultLevel.ERROR, step_title="Command", message=str(error))
        return progress.TaskResult(level=progress.TaskResultLevel.ERROR, step_title="Command", exception=error)

    async def run_async(self, action):
        output.set_progress_backend_override(self.backend)
        runtime_app = None
        try:
            progress_context = progress.MyProgress(backend=self.backend)
            progress_context.__enter__()
            try:
                runtime_app = RuntimeProgressApp(
                    self.backend,
                    action,
                    progress_context,
                    self._result_from_exception,
                    self.refresh_per_second,
                )
                await runtime_app.run_async()
            finally:
                progress_context.__exit__(None, None, None)
        finally:
            output.set_progress_backend_override(None)
        if isinstance(self.backend.result, progress.TaskResult):
            self.console.print(self.backend.result.to_rich_panel())
        if runtime_app is not None and runtime_app.error is not None:
            setattr(runtime_app.error, '_mnc_tui_reported', True)
            raise runtime_app.error
        return self.backend.result
