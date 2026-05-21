import logging

from ydb.tools.mnc.lib import common, configs, output
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)


expected_config = multinode.scheme


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    generate_parser = subparsers.add_parser('generate')
    common.add_common_options(generate_parser)


async def do_generate(args):
    hosts = await common.get_machines(args.config)
    result = await configs.act_generate(hosts, args.config)
    output.get_console().print(result.to_rich_panel(verbose=getattr(args, 'verbose', False)))
    return result


async def do(args):
    actions = {
        'generate': do_generate,
    }
    return await actions[args.cmd](args)
