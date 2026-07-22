use std::time::Duration as StdDuration;

use jiff::{SignedDuration, Timestamp, civil::Weekday, tz::TimeZone};
use serde_json::Value;

use crate::EnqueueOptions;

#[derive(Clone, Copy, Debug)]
pub struct DailyJobSchedule {
    hour: u8,
    minute: u8,
    weekdays: Option<&'static [Weekday]>,
}

impl DailyJobSchedule {
    pub fn new(hour: u8, minute: u8) -> Self {
        assert!(hour < 24, "daily job schedule hour must be less than 24");
        assert!(
            minute < 60,
            "daily job schedule minute must be less than 60"
        );
        Self {
            hour,
            minute,
            weekdays: None,
        }
    }

    pub fn weekdays(mut self, weekdays: &'static [Weekday]) -> Self {
        assert!(
            !weekdays.is_empty(),
            "daily job schedule weekdays must not be empty"
        );
        self.weekdays = Some(weekdays);
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MonthlyJobSchedule {
    day: u8,
    hour: u8,
    minute: u8,
}

impl MonthlyJobSchedule {
    pub fn new(day: u8, hour: u8, minute: u8) -> Self {
        assert!(
            (1..=28).contains(&day),
            "monthly job schedule day must be between 1 and 28"
        );
        assert!(hour < 24, "monthly job schedule hour must be less than 24");
        assert!(
            minute < 60,
            "monthly job schedule minute must be less than 60"
        );
        Self { day, hour, minute }
    }
}

#[derive(Clone, Debug)]
pub struct CronJob {
    pub identifier: String,
    pub name: String,
    pub payload: Value,
    pub schedule: CronSchedule,
    pub options: EnqueueOptions,
    pub catch_up_on_start: bool,
}

impl CronJob {
    pub fn daily_utc(
        identifier: impl Into<String>,
        name: impl Into<String>,
        schedule: DailyJobSchedule,
    ) -> Self {
        Self::daily(identifier, name, schedule, TimeZone::UTC)
    }

    pub fn daily(
        identifier: impl Into<String>,
        name: impl Into<String>,
        schedule: DailyJobSchedule,
        time_zone: TimeZone,
    ) -> Self {
        Self {
            identifier: identifier.into(),
            name: name.into(),
            payload: Value::Object(Default::default()),
            schedule: CronSchedule::Daily {
                schedule,
                time_zone,
            },
            options: EnqueueOptions::default(),
            catch_up_on_start: false,
        }
    }

    pub fn daily_in_timezone(
        identifier: impl Into<String>,
        name: impl Into<String>,
        schedule: DailyJobSchedule,
        time_zone_name: &str,
    ) -> Result<Self, jiff::Error> {
        Ok(Self::daily(
            identifier,
            name,
            schedule,
            TimeZone::get(time_zone_name)?,
        ))
    }

    pub fn monthly_utc(
        identifier: impl Into<String>,
        name: impl Into<String>,
        schedule: MonthlyJobSchedule,
    ) -> Self {
        Self::monthly(identifier, name, schedule, TimeZone::UTC)
    }

    pub fn monthly(
        identifier: impl Into<String>,
        name: impl Into<String>,
        schedule: MonthlyJobSchedule,
        time_zone: TimeZone,
    ) -> Self {
        Self {
            identifier: identifier.into(),
            name: name.into(),
            payload: Value::Object(Default::default()),
            schedule: CronSchedule::Monthly {
                schedule,
                time_zone,
            },
            options: EnqueueOptions::default(),
            catch_up_on_start: false,
        }
    }

    pub fn monthly_in_timezone(
        identifier: impl Into<String>,
        name: impl Into<String>,
        schedule: MonthlyJobSchedule,
        time_zone_name: &str,
    ) -> Result<Self, jiff::Error> {
        Ok(Self::monthly(
            identifier,
            name,
            schedule,
            TimeZone::get(time_zone_name)?,
        ))
    }

    /// Runs on Unix-epoch-aligned boundaries of `every`, independent of any time zone. The interval must be a whole number of seconds and at least one second.
    pub fn interval(
        identifier: impl Into<String>,
        name: impl Into<String>,
        every: StdDuration,
    ) -> Self {
        assert!(
            every >= StdDuration::from_secs(1),
            "cron interval must be at least one second"
        );
        assert!(
            every.subsec_nanos() == 0,
            "cron interval must be a whole number of seconds"
        );
        Self {
            identifier: identifier.into(),
            name: name.into(),
            payload: Value::Object(Default::default()),
            schedule: CronSchedule::Interval { every },
            options: EnqueueOptions::default(),
            catch_up_on_start: false,
        }
    }

    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    pub fn options(mut self, options: EnqueueOptions) -> Self {
        self.options = options;
        self
    }

    pub fn catch_up_on_start(mut self) -> Self {
        self.catch_up_on_start = true;
        self
    }
}

#[derive(Clone, Debug)]
pub enum CronSchedule {
    Daily {
        schedule: DailyJobSchedule,
        time_zone: TimeZone,
    },
    Monthly {
        schedule: MonthlyJobSchedule,
        time_zone: TimeZone,
    },
    Interval {
        every: StdDuration,
    },
}

impl CronSchedule {
    pub(crate) fn latest_due_at(&self, now: Timestamp) -> Option<Timestamp> {
        match self {
            Self::Daily {
                schedule,
                time_zone,
            } => latest_daily_due_at(now, *schedule, time_zone),
            Self::Monthly {
                schedule,
                time_zone,
            } => latest_monthly_due_at(now, *schedule, time_zone),
            Self::Interval { every } => latest_interval_due_at(now, *every),
        }
    }

    pub(crate) fn next_after(&self, after: Timestamp) -> Timestamp {
        match self {
            Self::Daily {
                schedule,
                time_zone,
            } => next_daily_after(after, *schedule, time_zone),
            Self::Monthly {
                schedule,
                time_zone,
            } => next_monthly_after(after, *schedule, time_zone),
            Self::Interval { every } => next_interval_after(after, *every),
        }
    }
}

pub(crate) fn latest_daily_due_at(
    now: Timestamp,
    schedule: DailyJobSchedule,
    time_zone: &TimeZone,
) -> Option<Timestamp> {
    for day_offset in 0..=8 {
        let date = local_date(time_zone, now) - days(day_offset);
        if schedule
            .weekdays
            .is_some_and(|days| !days.contains(&date.weekday()))
        {
            continue;
        }

        let candidate = at_time_zone(time_zone, date, schedule.hour, schedule.minute);
        if candidate <= now {
            return Some(candidate);
        }
    }

    None
}

pub(crate) fn next_daily_after(
    after: Timestamp,
    schedule: DailyJobSchedule,
    time_zone: &TimeZone,
) -> Timestamp {
    for day_offset in 0..=8 {
        let date = local_date(time_zone, after) + days(day_offset);
        if schedule
            .weekdays
            .is_some_and(|days| !days.contains(&date.weekday()))
        {
            continue;
        }

        let candidate = at_time_zone(time_zone, date, schedule.hour, schedule.minute);
        if candidate > after {
            return candidate;
        }
    }

    after + days(1)
}

pub(crate) fn latest_monthly_due_at(
    now: Timestamp,
    schedule: MonthlyJobSchedule,
    time_zone: &TimeZone,
) -> Option<Timestamp> {
    for day_offset in 0..=370 {
        let date = local_date(time_zone, now) - days(day_offset);
        if date.day() != schedule.day as i8 {
            continue;
        }

        let candidate = at_time_zone(time_zone, date, schedule.hour, schedule.minute);
        if candidate <= now {
            return Some(candidate);
        }
    }

    None
}

pub(crate) fn next_monthly_after(
    after: Timestamp,
    schedule: MonthlyJobSchedule,
    time_zone: &TimeZone,
) -> Timestamp {
    for day_offset in 0..=370 {
        let date = local_date(time_zone, after) + days(day_offset);
        if date.day() != schedule.day as i8 {
            continue;
        }

        let candidate = at_time_zone(time_zone, date, schedule.hour, schedule.minute);
        if candidate > after {
            return candidate;
        }
    }

    after + days(31)
}

pub(crate) fn latest_interval_due_at(now: Timestamp, interval: StdDuration) -> Option<Timestamp> {
    let interval_secs = interval.as_secs().max(1) as i64;
    let latest = now.as_second().div_euclid(interval_secs) * interval_secs;
    Timestamp::from_second(latest).ok()
}

pub(crate) fn next_interval_after(after: Timestamp, interval: StdDuration) -> Timestamp {
    let interval_secs = interval.as_secs().max(1) as i64;
    let next = (after.as_second().div_euclid(interval_secs) + 1) * interval_secs;
    Timestamp::from_second(next).unwrap_or(after + SignedDuration::from_secs(interval_secs))
}

fn local_date(time_zone: &TimeZone, timestamp: Timestamp) -> jiff::civil::Date {
    time_zone.to_datetime(timestamp).date()
}

fn at_time_zone(time_zone: &TimeZone, date: jiff::civil::Date, hour: u8, minute: u8) -> Timestamp {
    let datetime = date.at(hour as i8, minute as i8, 0, 0);
    time_zone
        .to_timestamp(datetime)
        .expect("valid time zone job schedule")
}

pub(crate) fn days(count: i64) -> SignedDuration {
    SignedDuration::from_hours(count * 24)
}
