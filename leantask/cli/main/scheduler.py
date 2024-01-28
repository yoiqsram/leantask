def add_scheduler_parser(subparsers) -> None:
    scheduler_parser = subparsers.add_parser(
        'scheduler',
        help='Leantask scheduler',
        description='Leantask scheduler'
    )
    scheduler_subparsers = scheduler_parser.add_subparsers(
        dest='scheduler_command',
        required=True,
        help='Command to run.'
    )
    scheduler_run_parser = scheduler_subparsers.add_parser(
        'run',
        help='Run scheduler.',
        description='Run scheduler.'
    )
    scheduler_run_parser.add_argument(
        '--project-dir', '-P',
        help='Project directory. Default to current directory.'
    )
    scheduler_run_parser.add_argument(
        '--worker', '-W',
        default=1,
        help='Number of worker.'
    )


def run_scheduler() -> None:
    ...
