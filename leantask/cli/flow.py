import inspect
import sys
from argparse import ArgumentParser
from pathlib import Path


def run_flow_cli(flow):
    main_script_path = Path(sys.argv[0]).resolve()
    main_caller_path = Path(inspect.stack()[-1].filename).resolve()
    if main_caller_path != main_script_path:
        return

    parser = ArgumentParser(description=flow.description)
    parser.add_argument(
        'COMMAND',
        help='Command to run.',
        choices=['run', 'queue']
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose flow and task run log.'
    )
    args = parser.parse_args()
    if args.COMMAND == 'run':
        flow.run()
        return

    flow.queue()
