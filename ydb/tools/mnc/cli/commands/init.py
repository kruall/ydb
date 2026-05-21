import logging

from ydb.tools.mnc.lib import common, init, output
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)


expected_config = multinode.scheme


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    static_parser = subparsers.add_parser('static')
    common.add_common_options(static_parser)

    dynamic_parser = subparsers.add_parser('dynamic')
    common.add_common_options(dynamic_parser)


async def do_static(args):
    result = await init.act_static(args.config)
    output.get_console().print(result.to_rich_panel(verbose=getattr(args, 'verbose', False)))
    return result


async def do_dynamic(args):
    result = await init.act_dynamic(args.config)
    output.get_console().print(result.to_rich_panel(verbose=getattr(args, 'verbose', False)))
    return result


async def do(args):
    actions = {
        'static': do_static,
        'dynamic': do_dynamic,
    }
    return await actions[args.cmd](args)
