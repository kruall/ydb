import argparse
import os
import tempfile
import types
import unittest
from unittest import mock

import rich.console

from ydb.tools.mnc.lib import progress
from ydb.tools.mnc.lib.exceptions import CliError

from ydb.tools.mnc.cli import arg_metadata, parser_factory
from ydb.tools.mnc.cli.tui import theme
from ydb.tools.mnc.cli.tui.app import TuiApp
from ydb.tools.mnc.cli.tui.command_picker import CommandPickerApp, _BackListItem
from ydb.tools.mnc.cli.tui.common import ConfigCandidate, config_preview
from ydb.tools.mnc.cli.tui.config_picker import (
    COMPAT_BAD,
    COMPAT_OK,
    ConfigPickerApp,
)
from ydb.tools.mnc.cli.tui.launcher import (
    LauncherResult,
    TuiLauncher,
    argv_preview,
    should_route_to_launcher,
    wizard_step_label,
)
from ydb.tools.mnc.cli.tui.options_form import (
    OptionsFormApp,
    _DeployFlagsApplyRow,
    _DeployFlagsModal,
    _TextInputModal,
    collect_missing_required,
    format_row,
    is_value_changed,
    is_value_missing,
)
from ydb.tools.mnc.lib import output
from ydb.tools.mnc.lib.output import VerbosityMode
from ydb.tools.mnc.lib.progress_live import LiveBackend


def _reset_output():
    output._state = {
        "mode": VerbosityMode.NORMAL,
        "console": None,
        "stderr_console": None,
        "active_progress": None,
        "progress_backend_override": None,
    }
    output.init(VerbosityMode.NORMAL)


class LiveBackendTest(unittest.TestCase):
    def test_tree_selection_and_logs_render(self):
        backend = LiveBackend(console=rich.console.Console(record=True))
        root = backend.add_task("root", total=2)
        child = backend.add_task("child", total=1, parent=root)
        backend.update(child, completed=1)
        backend.append_log("hello", step_id="12345678-1234-1234-1234-123456789abc")

        rows = backend.visible_rows()
        self.assertEqual([state.title for _, state in rows], ["root", "child"])
        backend.move_selection(1)
        self.assertEqual(backend.selected_task().title, "child")
        self.assertIsNotNone(backend.render())
        self.assertIn("hello", str(backend.render_logs().renderable))

    def test_toggle_selected_collapses_children(self):
        backend = LiveBackend(console=rich.console.Console(record=True))
        root = backend.add_task("root")
        backend.add_task("child", parent=root)
        self.assertEqual(len(backend.visible_rows()), 2)
        backend.toggle_selected()
        self.assertEqual(len(backend.visible_rows()), 1)

    def test_log_window_width_matches_right_pane(self):
        backend = LiveBackend(console=rich.console.Console(record=True, width=120))
        self.assertEqual(backend.log_window_width(), 76)

    def test_log_payload_width_accounts_for_step_prefix(self):
        backend = LiveBackend(console=rich.console.Console(record=True, width=120))

        self.assertEqual(backend.log_prefix_width("12345678-1234-1234-1234-123456789abc"), 9)
        self.assertEqual(backend.log_payload_width("12345678-1234-1234-1234-123456789abc"), 67)

    def test_textual_body_renderables_are_available_without_panels(self):
        console = rich.console.Console(record=True)
        backend = LiveBackend(console=console)
        root = backend.add_task("root", total=2)
        backend.update(root, completed=1)
        backend.append_log("hello", step_id="12345678-1234-1234-1234-123456789abc")

        console.print(backend.render_tree_body())
        self.assertIn("root", console.export_text())
        self.assertIn("Task id", backend.render_details_body().plain)
        self.assertIn("hello", backend.render_logs_body().plain)

    def test_markup_in_task_title_uses_rich_markup_for_textual_body_renderables(self):
        console = rich.console.Console(record=True)
        backend = LiveBackend(console=console)
        backend.add_task("[bold]Install[/]", total=1)

        console.print(backend.render_tree_body())
        details = backend.render_details_body()

        self.assertIn("Install", console.export_text())
        self.assertIn("Install", details.plain)
        self.assertNotIn("[bold]", details.plain)

    def test_invalid_markup_in_task_title_is_rendered_as_plain_text(self):
        console = rich.console.Console(record=True)
        backend = LiveBackend(console=console)
        backend.add_task("Install[/]", total=1)

        console.print(backend.render_tree_body())
        details = backend.render_details_body()

        self.assertIn("Install[/]", console.export_text())
        self.assertIn("Install[/]", details.plain)


class TuiLauncherRoutingTest(unittest.TestCase):
    def test_no_verb_routes_to_launcher(self):
        args = argparse.Namespace(verb=None, tui=False)
        self.assertTrue(should_route_to_launcher(args, {}, {}))

    def test_install_without_tui_does_not_route_by_metadata(self):
        args = argparse.Namespace(verb="install", tui=False)
        self.assertFalse(should_route_to_launcher(args, {"install": object()}, {"install": True}))

    def test_complete_non_tui_command_does_not_route(self):
        args = argparse.Namespace(verb="service", tui=False, config_name="cfg", config_path=None)
        self.assertFalse(should_route_to_launcher(args, {"service": object()}, {"service": False}))

    def test_tui_missing_config_routes_to_launcher(self):
        args = argparse.Namespace(verb="service", tui=True, config_name=None, config_path=None)
        self.assertTrue(should_route_to_launcher(args, {"service": object()}, {"service": False}))

    def test_metadata_extracts_commands_and_options(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)

        install = arg_metadata.find_command(root, ["install"])

        self.assertIsNotNone(install)
        self.assertTrue(install.is_leaf)
        self.assertIn("waiting", {option.dest for option in install.options})
        self.assertIn("do_not_init", {option.dest for option in install.options})

    def test_common_options_are_detected_by_group(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        install = arg_metadata.find_command(root, ["install"])
        launcher = TuiLauncher(parser, expected_config)

        common_dests = {option.dest for option in launcher._common_options(install)}
        command_dests = {option.dest for option in launcher._command_options(install)}

        self.assertIn("work_directory", common_dests)
        self.assertIn("waiting", command_dests)
        self.assertIn("do_not_init", command_dests)
        self.assertNotIn("waiting", common_dests)

    def test_textual_apps_use_consistent_light_theme_friendly_panel_css(self):
        from ydb.tools.mnc.cli.tui.app import RuntimeProgressApp
        from ydb.tools.mnc.cli.tui.config_picker import ConfigPickerApp
        from ydb.tools.mnc.cli.tui.options_form import OptionsFormApp

        for app_cls in (CommandPickerApp, ConfigPickerApp, OptionsFormApp, RuntimeProgressApp):
            self.assertIn("border: solid $primary", app_cls.CSS)
            self.assertIn("background: transparent", app_cls.CSS)
            self.assertNotIn("background: $surface", app_cls.CSS)

    def test_command_picker_can_start_from_subcommand_level(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        service = arg_metadata.find_command(root, ["service"])

        app = CommandPickerApp(root, initial=service)

        self.assertIs(app.command_parent, service)
        self.assertEqual(app.stack, [root, service])
        self.assertTrue(service.children)

    def test_command_picker_back_does_not_cancel_at_nested_level(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        service = arg_metadata.find_command(root, ["service"])
        app = CommandPickerApp(root, initial=service)

        self.assertTrue(app._pop_command_level())

        self.assertIs(app.command_parent, root)
        self.assertEqual(app.stack, [root])
        self.assertFalse(app._pop_command_level())

    def test_command_picker_ignores_stale_back_highlight_at_root(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        app = CommandPickerApp(root)
        event = types.SimpleNamespace(
            item=_BackListItem(),
            stop=mock.Mock(),
        )

        app.on_list_view_highlighted(event)

        event.stop.assert_called_once()

    def test_command_help_text_renders_parser_help(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        service = arg_metadata.find_command(root, ["service"])

        help_text = arg_metadata.command_help_text(service)

        self.assertIn("Usage:", help_text)
        self.assertIn("Subcommands:", help_text)

    def test_config_preview_reads_config_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)

            preview = config_preview(candidate)

        self.assertIn("cluster", preview.plain)
        self.assertIn(path, preview.plain)
        self.assertIn("host1", preview.plain)

    def test_config_preview_reports_validation_errors(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)

            preview = config_preview(candidate, {"__type__": dict, "missing": str})

        self.assertIn("incompatible", preview.plain)
        self.assertIn("Validation errors", preview.plain)
        self.assertIn("missing", preview.plain)

    def test_config_preview_has_no_validation_error_for_matching_config(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)

            preview = config_preview(candidate, {"__type__": dict, "hosts": {"__type__": list, "__inner__": str}})

        self.assertNotIn("Config is not compatible", preview.plain)

    def test_launcher_values_to_argv_skips_defaults_and_keeps_changes(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        install = arg_metadata.find_command(root, ["install"])
        args = parser.parse_args(["install"])
        launcher = TuiLauncher(parser, expected_config)
        values = {
            "flags": {"do_not_init": True, "ignore_failed_stop": False},
            "options": {"waiting": "20", "bin_path": ""},
            "arguments": {},
        }

        argv = launcher._values_to_argv(install, values, args)

        self.assertIn("--do-not-init", argv)
        self.assertIn("--waiting", argv)
        self.assertIn("20", argv)
        self.assertNotIn("--ignore-failed-stop", argv)
        self.assertNotIn("--bin-path", argv)

    def test_launcher_values_to_argv_accepts_deploy_flags_list(self):
        parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        install = arg_metadata.find_command(root, ["install"])
        args = parser.parse_args(["install"])
        launcher = TuiLauncher(parser, expected_config)
        values = {
            "flags": {},
            "options": {"deploy_flags": ["do_rebuild", "secure"]},
            "arguments": {},
        }

        argv = launcher._values_to_argv(install, values, args)

        self.assertEqual(argv, ["--deploy-flags", "do_rebuild", "secure"])

class TuiAppErrorTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_output()

    @staticmethod
    async def _run_runtime_app(runtime_app):
        try:
            result = await runtime_app.action(runtime_app.progress_context)
            runtime_app.backend.result = result
        except Exception as exc:
            runtime_app.error = exc
            runtime_app.backend.result = runtime_app.result_from_exception(exc)
        return runtime_app.backend.result

    async def test_run_async_prints_rich_error_after_live_exit_and_marks_exception(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)

        async def action(pbar):
            raise CliError("boom")

        with mock.patch("ydb.tools.mnc.cli.tui.app.RuntimeProgressApp.run_async", new=self._run_runtime_app):
            with self.assertRaises(CliError) as error:
                await app.run_async(action)

        self.assertTrue(getattr(error.exception, "_mnc_tui_reported", False))
        rendered = console.export_text()
        self.assertIn("ERROR: Command", rendered)
        self.assertIn("boom", rendered)

    async def test_run_async_prints_cli_error_task_result(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)
        command_result = progress.TaskResult(
            level=progress.TaskResultLevel.ERROR,
            step_title="failed-step",
            message="actionable failure",
        )

        async def action(pbar):
            raise CliError("generic failure", result=command_result)

        with mock.patch("ydb.tools.mnc.cli.tui.app.RuntimeProgressApp.run_async", new=self._run_runtime_app):
            with self.assertRaises(CliError) as error:
                await app.run_async(action)

        self.assertTrue(getattr(error.exception, "_mnc_tui_reported", False))
        rendered = console.export_text()
        self.assertIn("ERROR: failed-step", rendered)
        self.assertIn("actionable failure", rendered)
        self.assertNotIn("generic failure", rendered)

    async def test_run_async_prints_compact_task_result(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)

        async def action(parent_task=None):
            return progress.TaskResult(
                level=progress.TaskResultLevel.ERROR,
                step_title="root",
                subresults=[
                    progress.TaskResult(
                        level=progress.TaskResultLevel.OK,
                        step_title="successful_subtree",
                        subresults=[
                            progress.TaskResult(
                                level=progress.TaskResultLevel.ERROR,
                                step_title="recovered_error",
                                message="recovered",
                            ),
                        ],
                    ),
                    progress.TaskResult(
                        level=progress.TaskResultLevel.ERROR,
                        step_title="failed_child",
                        message="failed",
                    ),
                ],
            )

        with mock.patch("ydb.tools.mnc.cli.tui.app.RuntimeProgressApp.run_async", new=self._run_runtime_app):
            await app.run_async(action)
        rendered = console.export_text()

        self.assertIn("failed_child", rendered)
        self.assertNotIn("successful_subtree", rendered)
        self.assertNotIn("recovered_error", rendered)

    async def test_run_async_prints_task_result_error(self):
        console = rich.console.Console(record=True, width=120)
        app = TuiApp(console=console)

        async def action(pbar):
            return progress.TaskResult(level=progress.TaskResultLevel.ERROR, message="failed step")

        with mock.patch("ydb.tools.mnc.cli.tui.app.RuntimeProgressApp.run_async", new=self._run_runtime_app):
            result = await app.run_async(action)

        self.assertFalse(result)
        self.assertIn("failed step", console.export_text())


class TuiMainRoutingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_output()

    async def _run_tui_action(self, action):
        return await action(None)

    async def test_async_main_no_args_runs_launcher(self):
        from ydb.tools.mnc.cli import main

        calls = []

        async def do(args):
            calls.append(args)
            return True

        module = types.SimpleNamespace(
            __name__="ydb.tools.mnc.cli.commands.install",
            expected_config=None,
            prefer_tui_launcher=True,
            add_arguments=lambda parser: None,
            do=do,
        )
        parser, actions, expected_config, prefer_launcher = parser_factory.build_parser([module])
        launcher_result = LauncherResult(args=parser.parse_args(["install"]), argv=["install"])

        with mock.patch("sys.argv", ["mnc"]), \
             mock.patch("ydb.tools.mnc.cli.parser_factory.build_parser", return_value=(parser, actions, expected_config, prefer_launcher)), \
             mock.patch("ydb.tools.mnc.cli.main.should_route_to_launcher", return_value=True) as route_to_launcher, \
             mock.patch("ydb.tools.mnc.cli.main.TuiLauncher.run_async", return_value=launcher_result) as run_launcher, \
             mock.patch("ydb.tools.mnc.cli.main.TuiApp.run_async", side_effect=self._run_tui_action) as run_tui:
            await main.async_main()

        route_to_launcher.assert_called_once()
        run_launcher.assert_called_once()
        run_tui.assert_called_once()
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].verb, "install")

    async def test_async_main_install_does_not_run_launcher_without_tui_flag(self):
        from ydb.tools.mnc.cli import main

        async def do(args):
            return True

        module = types.SimpleNamespace(
            __name__="ydb.tools.mnc.cli.commands.install",
            expected_config=None,
            prefer_tui_launcher=True,
            add_arguments=lambda parser: None,
            do=do,
        )
        parser, actions, expected_config, prefer_launcher = parser_factory.build_parser([module])

        with mock.patch("sys.argv", ["mnc", "install"]), \
             mock.patch("ydb.tools.mnc.cli.parser_factory.build_parser", return_value=(parser, actions, expected_config, prefer_launcher)), \
             mock.patch("ydb.tools.mnc.cli.main.TuiLauncher.run_async") as run_launcher, \
             mock.patch("ydb.tools.mnc.cli.main.TuiApp.run_async", side_effect=self._run_tui_action) as run_tui:
            await main.async_main()

        run_launcher.assert_not_called()
        run_tui.assert_not_called()


class ThemeHelpersTest(unittest.TestCase):
    def test_format_header_includes_expected_context(self):
        cases = [
            ("Select command", {}, ["Select command"]),
            ("Common options", {"step": "1/4", "preview": "mnc install"}, ["1/4", "Common options", "mnc install"]),
        ]

        for title, kwargs, expected_fragments in cases:
            with self.subTest(title=title):
                rendered = theme.format_header(title, **kwargs)
                for fragment in expected_fragments:
                    self.assertIn(fragment, rendered)

    def test_format_footer_includes_all_bindings(self):
        rendered = theme.format_footer([("j/k", "Move"), ("Enter", "Edit"), ("Esc", "Cancel")])
        self.assertIn("j/k", rendered)
        self.assertIn("Enter", rendered)
        self.assertIn("Esc", rendered)
        self.assertIn("Move", rendered)

    def test_status_line_levels(self):
        self.assertIn("dim", theme.status_line("hello", level="info"))
        self.assertIn("red", theme.status_line("oops", level="error"))
        self.assertIn("yellow", theme.status_line("careful", level="warning"))
        self.assertIn("green", theme.status_line("done", level="success"))

    def test_status_line_returns_empty_for_no_message(self):
        self.assertEqual(theme.status_line(""), "")

    def test_apps_use_shared_screen_css(self):
        from ydb.tools.mnc.cli.tui.app import RuntimeProgressApp
        from ydb.tools.mnc.cli.tui.options_form import OptionsFormApp

        self.assertEqual(CommandPickerApp.CSS, theme.SCREEN_CSS_TWO_PANE)
        self.assertEqual(ConfigPickerApp.CSS, theme.SCREEN_CSS_TWO_PANE)
        self.assertEqual(OptionsFormApp.CSS, theme.SCREEN_CSS_FORM)
        self.assertEqual(RuntimeProgressApp.CSS, theme.SCREEN_CSS_RUNTIME)

    def test_modal_css_centers_dialogs_through_shared_root(self):
        self.assertIn("#modal-root", theme.SCREEN_CSS_MODAL)
        self.assertIn("align: center middle", theme.SCREEN_CSS_MODAL)
        self.assertIn("border: round $primary", theme.SCREEN_CSS_MODAL)
        self.assertNotIn("Screen {\n    align: center middle", theme.SCREEN_CSS_MODAL)


class WizardPreviewTest(unittest.TestCase):
    def test_wizard_step_label_formats_progress(self):
        cases = [
            (wizard_step_label(1, "Select command"), "mnc tui · 1/4 · Select command"),
            (wizard_step_label(2, "Select config", total=5), "mnc tui · 2/5 · Select config"),
        ]

        for actual, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(actual, expected)

    def test_argv_preview_quotes_special_characters(self):
        preview = argv_preview(["install", "--config", "my cfg", "--host", "h1"])
        self.assertIn("mnc", preview)
        self.assertIn("install", preview)
        self.assertIn("'my cfg'", preview)

    def test_argv_preview_empty(self):
        self.assertEqual(argv_preview([]), "")


class OptionsFormHelpersTest(unittest.TestCase):
    def test_is_value_missing(self):
        self.assertTrue(is_value_missing(None))
        self.assertTrue(is_value_missing(""))
        self.assertTrue(is_value_missing([]))
        self.assertFalse(is_value_missing("value"))
        self.assertFalse(is_value_missing(["v"]))
        self.assertFalse(is_value_missing(0))

    def test_is_value_changed_detects_default_and_changed_values(self):
        cases = [
            ({"kind": "value", "default": "default-value"}, "default-value", False),
            ({"kind": "value", "default": "default-value"}, "new-value", True),
            ({"kind": "flag", "default": False}, False, False),
            ({"kind": "flag", "default": False}, True, True),
            ({"kind": "deploy_flags", "default": []}, [], False),
            ({"kind": "deploy_flags", "default": []}, ["secure"], True),
        ]

        for entry, value, expected in cases:
            with self.subTest(kind=entry["kind"], value=value):
                self.assertEqual(is_value_changed(entry, value), expected)

    def test_format_row_shows_required_missing_badge(self):
        entry = {
            "kind": "value",
            "label": "--host",
            "required": True,
            "default": None,
        }
        rendered = format_row(entry, "")
        self.assertIn("--host", rendered)
        self.assertIn("missing", rendered)

    def test_format_row_shows_changed_badge(self):
        entry = {
            "kind": "value",
            "label": "--waiting",
            "required": False,
            "default": "10",
        }
        rendered = format_row(entry, "20")
        self.assertIn("changed", rendered)
        self.assertIn("20", rendered)

    def test_format_row_flag_uses_checkbox(self):
        entry = {
            "kind": "flag",
            "label": "--do-not-init",
            "required": False,
            "default": False,
        }
        on = format_row(entry, True)
        off = format_row(entry, False)
        self.assertIn("✓", on)
        self.assertIn("□", off)

    def test_collect_missing_required_returns_only_blocked_entries(self):
        entries = [
            {"key": "arg::host", "kind": "argument", "label": "host", "required": True, "default": None},
            {"key": "arg::port", "kind": "argument", "label": "port", "required": True, "default": None},
            {"key": "opt::waiting", "kind": "value", "label": "--waiting", "required": False, "default": "10"},
            {"key": "flag::quiet", "kind": "flag", "label": "--quiet", "required": True, "default": False},
        ]
        values = {
            "arg::host": "h1",
            "arg::port": "",
            "opt::waiting": "",
            "flag::quiet": False,
        }
        missing = collect_missing_required(entries, values)

        self.assertEqual([entry["key"] for entry in missing], ["arg::port"])

    def test_form_footer_has_no_ctrl_s_apply_shortcut(self):
        footer = theme.format_footer(theme.FOOTER_FORM)

        self.assertNotIn("Ctrl+S", footer)
        self.assertIn("Edit/Apply", footer)

    def test_deploy_flags_footer_has_no_ctrl_s_apply_shortcut(self):
        footer = theme.format_footer(theme.FOOTER_MODAL_MULTI)

        self.assertNotIn("Ctrl+S", footer)
        self.assertIn("Toggle/Apply", footer)

    def test_text_input_modal_has_no_ctrl_s_apply_shortcut(self):
        key_bindings = [binding.key for binding in _TextInputModal.BINDINGS]

        self.assertNotIn("ctrl+s", key_bindings)

    def test_deploy_flags_modal_has_explicit_apply_row(self):
        modal = _DeployFlagsModal("--deploy-flags", [])
        list_view = mock.Mock()
        list_view.index = 0
        list_view.children = [_DeployFlagsApplyRow()]

        with mock.patch.object(modal, "query_one", return_value=list_view), \
             mock.patch.object(modal, "action_submit") as submit:
            modal.action_toggle()

        submit.assert_called_once_with()


class OptionsFormAppTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _reset_output()
        self.parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(self.parser)
        self.install = arg_metadata.find_command(root, ["install"])

    def test_options_form_builds_entries_with_sections(self):
        args = self.parser.parse_args(["install"])
        app = OptionsFormApp(
            "Command options",
            args,
            options=self.install.options,
            arguments=self.install.arguments,
        )

        sections = {entry["section"] for entry in app._entries}
        self.assertIn("flags", sections)
        self.assertIn("options", sections)

    def test_options_form_submit_blocks_when_required_value_is_missing(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        nbs_create_disk = arg_metadata.find_command(root, ["nbs", "create-disk"])
        args = argparse.Namespace()
        for option in nbs_create_disk.options:
            setattr(args, option.dest, option.default)
        for argument in nbs_create_disk.arguments:
            setattr(args, argument.dest, None)

        app = OptionsFormApp(
            "Command options",
            args,
            options=nbs_create_disk.options,
            arguments=nbs_create_disk.arguments,
        )

        required_entries = [entry for entry in app._entries if entry.get("required") and entry["kind"] != "flag"]
        if not required_entries:
            self.skipTest("no required options/arguments found for this command")

        for entry in required_entries:
            app._values[entry["key"]] = ""

        missing = collect_missing_required(app._entries, app._values)
        self.assertTrue(missing)
        self.assertEqual({entry["key"] for entry in missing}, {entry["key"] for entry in required_entries})


class ConfigPickerCompatibilityTest(unittest.TestCase):
    def setUp(self):
        _reset_output()

    def _make_candidates(self):
        tmp_dir = tempfile.mkdtemp()
        compatible_path = os.path.join(tmp_dir, "good.yaml")
        with open(compatible_path, "w") as file:
            file.write("hosts:\n  - host1\n")
        incompatible_path = os.path.join(tmp_dir, "bad.yaml")
        with open(incompatible_path, "w") as file:
            file.write("foo: bar\n")
        return [
            ConfigCandidate("good", compatible_path),
            ConfigCandidate("bad", incompatible_path),
        ]

    def test_compatibility_status_for_each_candidate(self):
        candidates = self._make_candidates()
        scheme = {"__type__": dict, "hosts": {"__type__": list, "__inner__": str}}

        app = ConfigPickerApp(candidates, command_scheme=scheme)

        self.assertEqual(app._compat_status[candidates[0].path], COMPAT_OK)
        self.assertEqual(app._compat_status[candidates[1].path], COMPAT_BAD)

    def test_activate_does_not_exit_on_incompatible(self):
        candidates = self._make_candidates()
        scheme = {"__type__": dict, "hosts": {"__type__": list, "__inner__": str}}
        app = ConfigPickerApp(candidates, command_scheme=scheme)

        item_factory = mock.Mock()
        from ydb.tools.mnc.cli.tui.config_picker import _ConfigListItem
        item = _ConfigListItem(candidates[1], COMPAT_BAD)
        item_factory.return_value = item

        exit_mock = mock.Mock()
        status_mock = mock.Mock()
        with mock.patch.object(app, "exit", exit_mock), \
             mock.patch.object(app, "_set_status", status_mock):
            app._activate(item)

        exit_mock.assert_not_called()
        status_mock.assert_called()


class StructuredDetailsTest(unittest.TestCase):
    def test_command_summary_includes_path_and_subcommands(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        service = arg_metadata.find_command(root, ["service"])

        summary = arg_metadata.command_summary_text(service)

        self.assertIn("service", summary)
        self.assertIn("Subcommands", summary)

    def test_command_summary_for_leaf_lists_arguments_section(self):
        parser, _, _, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(parser)
        nbs_create_disk = arg_metadata.find_command(root, ["nbs", "create-disk"])

        summary = arg_metadata.command_summary_text(nbs_create_disk)

        self.assertIn("nbs create-disk", summary)
        if nbs_create_disk.arguments:
            self.assertIn("Arguments", summary)

    def test_config_preview_shows_status_compatible_first(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)
            preview = config_preview(candidate, {"__type__": dict, "hosts": {"__type__": list, "__inner__": str}})

        plain = preview.plain
        status_index = plain.find("Status")
        contents_index = plain.find("Contents")
        self.assertGreaterEqual(status_index, 0)
        self.assertGreaterEqual(contents_index, 0)
        self.assertLess(status_index, contents_index)
        self.assertIn("compatible", plain)

    def test_config_preview_shows_validation_errors_before_contents(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "cluster.yaml")
            with open(path, "w") as file:
                file.write("hosts:\n  - host1\n")
            candidate = ConfigCandidate("cluster", path)
            preview = config_preview(candidate, {"__type__": dict, "missing": str})

        plain = preview.plain
        errors_index = plain.find("Validation errors")
        contents_index = plain.find("Contents")
        self.assertGreaterEqual(errors_index, 0)
        self.assertGreaterEqual(contents_index, 0)
        self.assertLess(errors_index, contents_index)


class RuntimeAppFooterTest(unittest.TestCase):
    def test_runtime_footer_includes_available_actions(self):
        cases = [
            (theme.FOOTER_RUNTIME_RUNNING, ["Ctrl+C", "Interrupt"]),
            (theme.FOOTER_RUNTIME_FINISHED, ["Close"]),
        ]

        for footer_spec, expected_fragments in cases:
            with self.subTest(expected_fragments=expected_fragments):
                footer = theme.format_footer(footer_spec)
                for fragment in expected_fragments:
                    self.assertIn(fragment, footer)


if __name__ == '__main__':
    unittest.main()
