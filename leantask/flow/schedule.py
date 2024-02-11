from datetime import datetime
from typing import List, Union


class Schedule:
    def __init__(
            self,
            cron_schedules: [str, List[str]],
            start_datetime: datetime = None,
            end_datetime: datetime = None,
        ) -> None:
        if not isinstance(cron_schedules, list):
            cron_schedules = [cron_schedules]
        self.cron_schedules = cron_schedules

        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

    def next_datetime(
            self,
            anchor_datetime: datetime = None
        ) -> Union[datetime, None]:
        import croniter

        if anchor_datetime is None:
            anchor_datetime = datetime.now()

        min_next_datetime = None
        for cron_schedule in self.cron_schedules:
            cron_iter = croniter.croniter(cron_schedule, anchor_datetime)
            cron_next_datetime = cron_iter.get_next(datetime)

            if self.start_datetime is not None \
                    and cron_next_datetime < self.start_datetime:
                continue

            if self.end_datetime is not None \
                    and cron_next_datetime > self.end_datetime:
                continue

            if min_next_datetime is None:
                min_next_datetime = cron_next_datetime
            elif cron_next_datetime < min_next_datetime:
                min_next_datetime = cron_next_datetime

        return min_next_datetime
