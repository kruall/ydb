import os
import tempfile
import unittest
from unittest import mock

from ydb.tools.mnc.cli import arg_metadata, command_options, parser_factory
from ydb.tools.mnc.cli.tui.launcher import TuiLauncher


class TuiLauncherCacheTest(unittest.TestCase):
    def setUp(self):
        self.parser, _, expected_config, _ = parser_factory.build_parser()
        root = arg_metadata.command_metadata_from_parser(self.parser)
        self.command = arg_metadata.find_command(root, ["nbs", "create-disk"])
        self.launcher = TuiLauncher(self.parser, expected_config)
        self.initial_args = self.parser.parse_args([])

    def _with_cache(self, cache):
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        patcher = mock.patch.dict(os.environ, {"HOME": tmp_dir.name})
        patcher.start()
        self.addCleanup(patcher.stop)
        command_options.save_cache(cache)

    def test_prefills_selected_command_from_matching_config_cache(self):
        self._with_cache({
            "nbs/create-disk|config:cfg1": {
                "tokens": ["--disk-id", "disk1", "--blocks-count", "42"],
            },
        })

        args = self.launcher._initial_args_with_cached_options(
            self.command,
            self.initial_args,
            ["--config", "cfg1"],
        )

        self.assertEqual(args.verb, "nbs")
        self.assertEqual(args.cmd, "create-disk")
        self.assertEqual(args.config_name, "cfg1")
        self.assertEqual(args.disk_id, "disk1")
        self.assertEqual(args.blocks_count, 42)

    def test_ignores_cache_from_another_config(self):
        self._with_cache({
            "nbs/create-disk|config:cfg1": {
                "tokens": ["--disk-id", "disk1", "--blocks-count", "42"],
            },
        })

        args = self.launcher._initial_args_with_cached_options(
            self.command,
            self.initial_args,
            ["--config", "cfg2"],
        )

        self.assertIs(args, self.initial_args)

    def test_explicit_initial_argv_values_override_cached_tokens(self):
        self._with_cache({
            "nbs/create-disk|config:cfg1": {
                "tokens": ["--disk-id", "old-disk", "--blocks-count", "42"],
            },
        })

        args = self.launcher._initial_args_with_cached_options(
            self.command,
            self.initial_args,
            ["--config", "cfg1"],
            ["nbs", "create-disk", "--disk-id", "new-disk"],
        )

        self.assertEqual(args.config_name, "cfg1")
        self.assertEqual(args.disk_id, "new-disk")
        self.assertEqual(args.blocks_count, 42)

    def test_invalid_cached_tokens_fall_back_to_initial_args(self):
        self._with_cache({
            "nbs/create-disk|config:cfg1": {"tokens": ["--unknown-option"]},
        })

        args = self.launcher._initial_args_with_cached_options(
            self.command,
            self.initial_args,
            ["--config", "cfg1"],
        )

        self.assertIs(args, self.initial_args)


if __name__ == '__main__':
    unittest.main()
