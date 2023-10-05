from datetime import datetime
from typing import Optional, Union

from dagster import TimeWindowPartitionsDefinition


class QuarterlyPartitionsDefinition(TimeWindowPartitionsDefinition):
    def __new__(
        cls,
        start_date: Union[datetime, str],
        cron_schedule: str = "0 0 1 */3 *",
        fmt: Optional[str] = None,
        end_offset: Optional[int] = 0,
    ):
        _fmt = fmt or "%Y-%m-%d"

        return super(QuarterlyPartitionsDefinition, cls).__new__(
            cls,
            start=start_date,
            cron_schedule=cron_schedule,
            fmt=_fmt,
            end_offset=end_offset,
        )

    def __str__(self) -> str:
        return f"Quarterly, starting {self.start.strftime(self.fmt)} {self.timezone}."
