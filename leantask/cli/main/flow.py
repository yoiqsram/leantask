import argparse
import subprocess
import sys
from pathlib import Path
from typing import Callable

from ...database import FlowModel
from ...utils.string import quote


def add_flow_parser(subparsers) -> Callable:
    parser: argparse.ArgumentParser = subparsers.add_parser(
        'flow',
        help='Interact with flow.',
        description='Interact with flow.'
    )
    parser.add_argument(
        'flow_name',
        help='Flow name. It should be already indexed.'
    )

    return execute_flow_command


def execute_flow_command(args: argparse.Namespace):
    flow_model = (
        FlowModel.select()
        .where(FlowModel.name == args.flow_name)
        .limit(1)
        [0]
    )
    flow_path = Path(flow_model.path).resolve()

    flow_args = sys.argv[1:]
    flow_command = (
        f'{quote(sys.executable)} {quote(flow_path)} '
        + ' '.join(flow_args[2:])
    )
    subprocess.run(flow_command, shell=True)
