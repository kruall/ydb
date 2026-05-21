"""Shared TUI theme, layout, and keymap building blocks.

This module is the single source of truth for visual consistency across
all MNC TUI screens. It exposes:

- CSS fragments that compose into per-screen CSS.
- Helper functions for header/footer/status text.
- Common keyboard binding sets (Vim-friendly) for navigation and dialogs.
"""

from typing import Iterable, List, Optional, Sequence, Tuple

from textual.binding import Binding


# -- CSS fragments --------------------------------------------------------

BASE_SCREEN_CSS = """
Screen { layout: vertical; background: transparent; }
"""

HEADER_CSS = """
#header { height: 1; padding: 0 1; background: $primary 20%; color: $text; }
"""

FOOTER_CSS = """
#footer { height: 1; padding: 0 1; color: $text-muted; background: transparent; }
#status { height: 1; padding: 0 1; background: transparent; }
.status-info { color: $text-muted; }
.status-warning { color: $warning; }
.status-error { color: $error; }
.status-success { color: $success; }
"""

BODY_TWO_PANE_CSS = """
#body { layout: horizontal; height: 1fr; padding: 0 1; }
#left { width: 2fr; height: 1fr; border: solid $primary; background: transparent; }
#right { width: 3fr; height: 1fr; padding: 1 2; border: solid $primary; margin-left: 1; background: transparent; }
"""

BODY_SINGLE_PANE_CSS = """
#body { layout: vertical; height: 1fr; padding: 0 1; }
#form { height: 1fr; padding: 0 1; border: solid $primary; background: transparent; }
"""

LIST_CSS = """
ListView { background: transparent; }
ListItem { background: transparent; padding: 0 1; color: $text; }
ListItem * { background: transparent; }
ListItem.--highlight { background: $accent 30%; }
ListItem.--highlight * { background: $accent 30%; }
ListView:focus > ListItem.--highlight { background: $accent; text-style: reverse; }
ListView:focus > ListItem.--highlight * { background: $accent; text-style: reverse; }
"""

DETAILS_CSS = """
#details { height: 1fr; background: transparent; }
.hint { color: $text-muted; }
"""

MODAL_CSS = """
Screen {
    align: center middle;
    background: $background 60%;
}
ModalScreen {
    align: center middle;
    background: $background 60%;
}
#dialog {
    width: 72;
    max-width: 80%;
    height: auto;
    max-height: 80%;
    padding: 1 2;
    border: round $accent;
    background: $surface;
}
#dialog ListView { height: auto; max-height: 20; min-height: 5; background: transparent; }
#dialog #title { text-style: bold; color: $accent; padding-bottom: 1; }
Input { background: transparent; border: solid $primary; }
"""


def screen_css(*fragments: str) -> str:
    """Compose CSS fragments into a single CSS string for an App."""
    return "\n".join(fragment.strip() for fragment in fragments if fragment)


SCREEN_CSS_TWO_PANE = screen_css(
    BASE_SCREEN_CSS,
    HEADER_CSS,
    BODY_TWO_PANE_CSS,
    LIST_CSS,
    DETAILS_CSS,
    FOOTER_CSS,
)


SCREEN_CSS_FORM = screen_css(
    BASE_SCREEN_CSS,
    HEADER_CSS,
    BODY_SINGLE_PANE_CSS,
    LIST_CSS,
    DETAILS_CSS,
    FOOTER_CSS,
)


SCREEN_CSS_RUNTIME = screen_css(
    BASE_SCREEN_CSS,
    HEADER_CSS,
    BODY_TWO_PANE_CSS,
    FOOTER_CSS,
    """
    #steps { height: 1fr; background: transparent; }
    #right { layout: vertical; }
    #node-details { height: auto; margin-bottom: 1; background: transparent; }
    #logs { height: 1fr; background: transparent; }
    """,
)


SCREEN_CSS_MODAL = screen_css(
    MODAL_CSS,
    LIST_CSS,
    DETAILS_CSS,
)


# -- Header/footer/status helpers ----------------------------------------


def format_header(title: str, step: Optional[str] = None, preview: Optional[str] = None) -> str:
    """Format a single-line header.

    Rendered as: `mnc tui · <step> · <title>    <preview>` with muted parts.
    Uses Rich markup tags only (no Textual CSS variables) so that the text
    renders correctly inside Static widgets with markup=True.
    """
    parts: List[str] = []
    if step:
        parts.append(f"[cyan]{step}[/cyan]")
    parts.append(f"[bold cyan]{title}[/bold cyan]")
    line = " [dim]·[/dim] ".join(parts)
    if preview:
        line = f"{line}    [dim]{preview}[/dim]"
    return line


def format_footer(bindings: Sequence[Tuple[str, str]]) -> str:
    """Format `[(key, label), ...]` as a muted single-line footer.

    Example: ``j/k: Move    Enter: Edit    Ctrl+S: Apply    Esc: Cancel``.
    """
    chunks = [f"[bold]{key}[/bold]: {label}" for key, label in bindings if key]
    return "[dim]" + "    ".join(chunks) + "[/dim]"


def status_line(message: str, level: str = "info") -> str:
    """Format a status line with a level-specific style class."""
    if not message:
        return ""
    style = {
        "info": "dim",
        "warning": "yellow",
        "error": "bold red",
        "success": "green",
    }.get(level, "dim")
    return f"[{style}]{message}[/{style}]"


# -- Keymap building blocks ----------------------------------------------


def navigation_bindings(
    *,
    enter_action: str = "select",
    enter_label: str = "Select",
    back_action: str = "cancel",
    back_label: str = "Back",
    include_jk: bool = True,
    include_gG: bool = True,
) -> List[Binding]:
    """Standard Vim-friendly navigation bindings for list-based screens."""
    bindings: List[Binding] = []
    if include_jk:
        bindings.append(Binding("j", "cursor_down", "Down", show=False))
        bindings.append(Binding("k", "cursor_up", "Up", show=False))
    bindings.append(Binding("down", "cursor_down", "Down", show=False))
    bindings.append(Binding("up", "cursor_up", "Up", show=False))
    if include_gG:
        bindings.append(Binding("g,home", "cursor_first", "First", show=False))
        bindings.append(Binding("G,end", "cursor_last", "Last", show=False))
    bindings.append(Binding("l,right", enter_action, enter_label, show=False))
    bindings.append(Binding("h,left,escape", back_action, back_label))
    bindings.append(Binding("q", back_action, "Quit", show=False))
    return bindings


FOOTER_PICKER: Sequence[Tuple[str, str]] = (
    ("j/k", "Move"),
    ("Enter", "Select"),
    ("h/Esc", "Back"),
    ("?", "Help"),
)


FOOTER_FORM: Sequence[Tuple[str, str]] = (
    ("j/k", "Move"),
    ("Enter", "Edit/Apply"),
    ("Space", "Toggle"),
    ("Esc", "Cancel"),
)


FOOTER_RUNTIME_RUNNING: Sequence[Tuple[str, str]] = (
    ("j/k", "Move"),
    ("Space/Enter", "Toggle"),
    ("Ctrl+C", "Interrupt"),
)


FOOTER_RUNTIME_FINISHED: Sequence[Tuple[str, str]] = (
    ("j/k", "Move"),
    ("Space/Enter", "Toggle"),
    ("q/Enter", "Close"),
)


FOOTER_MODAL_INPUT: Sequence[Tuple[str, str]] = (
    ("Enter", "Apply"),
    ("Esc", "Cancel"),
)


FOOTER_MODAL_CHOICE: Sequence[Tuple[str, str]] = (
    ("j/k", "Move"),
    ("Enter", "Select"),
    ("Esc", "Cancel"),
)


FOOTER_MODAL_MULTI: Sequence[Tuple[str, str]] = (
    ("j/k", "Move"),
    ("Space", "Toggle"),
    ("Enter", "Toggle/Apply"),
    ("Esc", "Cancel"),
)


def join_bindings(*groups: Iterable[Binding]) -> List[Binding]:
    """Concatenate several binding sequences into a single list."""
    bindings: List[Binding] = []
    for group in groups:
        bindings.extend(group)
    return bindings
