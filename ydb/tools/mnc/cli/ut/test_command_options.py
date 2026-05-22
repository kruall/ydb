import os
import tempfile
import unittest
from unittest import mock

import ydb.apps.dstool.lib.arg_parser as argparse

from ydb.tools.mnc.cli import command_options
from ydb.tools.mnc.lib import common


def make_parser():
    parser = argparse.ArgumentParser(description='test parser')
    parser.add_argument('--verbose', '-V', dest='verbose', action='store_true', default=False)
    subparsers = parser.add_subparsers(dest='verb', required=True)

    nbs_parser = subparsers.add_parser('nbs')
    nbs_subparsers = nbs_parser.add_subparsers(dest='cmd', required=True)

    create_disk_parser = nbs_subparsers.add_parser('create-disk')
    common.add_common_options(create_disk_parser)
    create_disk_parser.add_argument('--endpoint', default=None)
    create_disk_parser.add_argument('--disk-id', required=True)
    create_disk_parser.add_argument('--blocks-count', required=True, type=int)
    create_disk_parser.add_argument('--block-size', type=int, default=4096)

    service_parser = subparsers.add_parser('service')
    service_subparsers = service_parser.add_subparsers(dest='cmd', required=True)
    hosts_parser = service_subparsers.add_parser('hosts')
    common.add_common_options(hosts_parser)
    hosts_parser.add_argument('operation', choices=('start', 'stop'))
    hosts_parser.add_argument('--node-type', dest='node_type', default=None)

    return parser


class CommandOptionsTest(unittest.TestCase):
    def test_save_command_options_writes_effective_leaf_options(self):
        parser = make_parser()
        with tempfile.TemporaryDirectory() as home:
            args = parser.parse_args([
                'nbs',
                'create-disk',
                '--config',
                'cfg1',
                '--endpoint',
                'grpc://host1:2135',
                '--disk-id',
                'disk1',
                '--blocks-count',
                '42',
            ])

            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_command_options(parser, args)
                data = command_options.load_cache()

        self.assertEqual(
            data['nbs/create-disk']['tokens'],
            [
                '--config',
                'cfg1',
                '--endpoint',
                'grpc://host1:2135',
                '--disk-id',
                'disk1',
                '--blocks-count',
                '42',
                '--block-size',
                '4096',
            ],
        )

    def test_save_cache_uses_restrictive_permissions(self):
        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_cache({'nbs/create-disk': {'tokens': ['--endpoint', 'grpc://host1:2135']}})
                path = command_options.cache_path()

            cache_dir = os.path.dirname(path)
            mnc_dir = os.path.dirname(cache_dir)
            self.assertEqual(os.stat(mnc_dir).st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(cache_dir).st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)

    def test_cache_is_disabled_without_home(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(command_options.cache_path())
            command_options.save_cache({'nbs/create-disk': {'tokens': ['--endpoint', 'grpc://host1:2135']}})
            self.assertEqual(command_options.load_cache(), {})

    def test_apply_cached_options_injects_missing_required_options(self):
        parser = make_parser()
        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_cache({
                    'nbs/create-disk': {
                        'tokens': [
                            '--config',
                            'cfg1',
                            '--endpoint',
                            'grpc://host1:2135',
                            '--disk-id',
                            'old-disk',
                            '--blocks-count',
                            '42',
                        ],
                    },
                })

                argv = command_options.apply_cached_options(parser, ['nbs', 'create-disk', '--disk-id', 'new-disk'])

        args = parser.parse_args(argv)

        self.assertEqual(args.config_name, 'cfg1')
        self.assertEqual(args.endpoint, 'grpc://host1:2135')
        self.assertEqual(args.disk_id, 'new-disk')
        self.assertEqual(args.blocks_count, 42)
        self.assertNotIn('old-disk', argv)

    def test_global_options_are_not_cached(self):
        parser = make_parser()
        args = parser.parse_args([
            '--verbose',
            'nbs',
            'create-disk',
            '--config',
            'cfg1',
            '--disk-id',
            'disk1',
            '--blocks-count',
            '42',
        ])

        tokens = command_options.tokens_from_namespace(
            command_options.command_from_namespace(parser, args)[1],
            args,
        )

        self.assertNotIn('--verbose', tokens)

    def test_position_arguments_are_cached_and_can_be_overridden(self):
        parser = make_parser()
        with tempfile.TemporaryDirectory() as home:
            args = parser.parse_args(['service', 'hosts', '--config', 'cfg1', 'start', '--node-type', 'dynamic'])
            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_command_options(parser, args)
                argv = command_options.apply_cached_options(parser, ['service', 'hosts', 'stop'])

        self.assertEqual(argv, ['service', 'hosts', '--config', 'cfg1', '--node-type', 'dynamic', 'stop'])
        parsed = make_parser().parse_args(argv)

        self.assertEqual(parsed.config_name, 'cfg1')
        self.assertEqual(parsed.operation, 'stop')
        self.assertEqual(parsed.node_type, 'dynamic')
        self.assertNotIn('start', argv)

    def test_reset_parser_allows_repeated_parse_args(self):
        parser = make_parser()
        argv = ['nbs', 'create-disk', '--config', 'cfg1', '--disk-id', 'd', '--blocks-count', '42']
        parser.parse_args(argv)

        # Without reset, a second parse_args() on the same parser would crash
        # with "Value verb already assigned by nbs" because the dstool parser
        # is stateful.
        command_options.reset_parser(parser)
        args = parser.parse_args(argv)
        self.assertEqual(args.verb, 'nbs')
        self.assertEqual(args.cmd, 'create-disk')

        command_options.reset_parser(parser)
        args = parser.parse_args(['service', 'hosts', '--config', 'cfg1', 'start'])
        self.assertEqual(args.verb, 'service')
        self.assertEqual(args.operation, 'start')


if __name__ == '__main__':
    unittest.main()
