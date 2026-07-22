use std::{future::pending, time::Duration as StdDuration};

use jiff::{Timestamp, civil::Weekday, tz::TimeZone};
use serde_json::Value;
use uuid::Uuid;

use super::*;
use crate::schedule::{
    latest_daily_due_at, latest_interval_due_at, latest_monthly_due_at, next_daily_after,
    next_interval_after, next_monthly_after,
};
use crate::worker::AbortOnDrop;
use serde::{Deserialize, Serialize};

fn timestamp(value: &str) -> Timestamp {
    value.parse().expect("valid test timestamp")
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct TestPayload {
    value: String,
}

fn test_job(payload: Value) -> Job {
    let now = timestamp("2026-06-13T10:00:00Z");
    Job {
        id: Uuid::nil(),
        name: "test".to_owned(),
        status: JobStatus::Queued,
        payload,
        queue_name: None,
        priority: 0,
        run_at: now,
        job_key: None,
        attempt: 0,
        max_attempts: 25,
        locked_by: None,
        locked_at: None,
        error: None,
        queued_at: now,
        started_at: None,
        finished_at: None,
        created_at: now,
        updated_at: now,
    }
}

#[test]
fn daily_schedule_rejects_invalid_hour() {
    let result = std::panic::catch_unwind(|| DailyJobSchedule::new(24, 0));
    assert!(result.is_err());
}

#[test]
fn daily_schedule_rejects_invalid_minute() {
    let result = std::panic::catch_unwind(|| DailyJobSchedule::new(23, 60));
    assert!(result.is_err());
}

#[test]
fn monthly_schedule_rejects_invalid_day() {
    let result = std::panic::catch_unwind(|| MonthlyJobSchedule::new(0, 0, 0));
    assert!(result.is_err());

    let result = std::panic::catch_unwind(|| MonthlyJobSchedule::new(29, 0, 0));
    assert!(result.is_err());
}

#[test]
fn cron_schedule_rejects_zero_interval() {
    let result = std::panic::catch_unwind(|| CronJob::interval("test", "test", StdDuration::ZERO));

    assert!(result.is_err());
}

#[test]
fn worker_config_defaults_renew_locks_before_stale_recovery() {
    let config = WorkerConfig::default();

    assert!(config.lock_renew_interval < config.stale_after);
    assert!(!config.shutdown_grace_period.is_zero());
}

#[test]
fn worker_config_rejects_zero_concurrency() {
    let config = WorkerConfig {
        concurrency: 0,
        ..WorkerConfig::default()
    };

    assert!(matches!(
        config.validate(),
        Err(WorkerError::InvalidConfig(
            "concurrency must be greater than zero"
        ))
    ));
}

#[test]
fn job_deserializes_typed_payload() {
    let job = test_job(serde_json::json!({ "value": "ready" }));

    assert_eq!(
        job.payload_as::<TestPayload>().unwrap(),
        TestPayload {
            value: "ready".to_owned()
        }
    );
}

#[tokio::test]
async fn registry_runs_typed_handler() {
    let registry = Registry::builder()
        .register_typed("test", |_job, payload: TestPayload| async move {
            assert_eq!(payload.value, "ready");
            Ok::<(), &'static str>(())
        })
        .build();
    let handler = registry.get("test").unwrap();

    handler(test_job(serde_json::json!({ "value": "ready" })))
        .await
        .unwrap();
}

#[tokio::test]
async fn typed_handler_rejects_invalid_payload() {
    let registry = Registry::builder()
        .register_typed("test", |_job, _payload: TestPayload| async move {
            Ok::<(), &'static str>(())
        })
        .build();
    let handler = registry.get("test").unwrap();

    let error = handler(test_job(serde_json::json!({ "wrong": true })))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("missing field `value`"));
}

#[tokio::test]
async fn abort_on_drop_cancels_handler_task() {
    let handle = tokio::spawn(pending::<()>());
    let abort_handle = handle.abort_handle();

    drop(AbortOnDrop::new(handle));
    tokio::task::yield_now().await;

    assert!(abort_handle.is_finished());
}

#[test]
fn latest_interval_due_at_aligns_to_epoch_boundary() {
    let now = timestamp("2026-06-13T10:07:42Z");
    let due_at = latest_interval_due_at(now, StdDuration::from_secs(300)).unwrap();

    assert_eq!(due_at, timestamp("2026-06-13T10:05:00Z"));
    assert_eq!(
        next_interval_after(due_at, StdDuration::from_secs(300)),
        timestamp("2026-06-13T10:10:00Z")
    );
}

#[test]
fn latest_daily_due_at_uses_previous_valid_weekday_after_todays_slot_passed() {
    let schedule = DailyJobSchedule::new(9, 30).weekdays(&[Weekday::Monday]);
    let now = timestamp("2026-06-16T10:00:00Z");

    assert_eq!(
        latest_daily_due_at(now, schedule, &TimeZone::UTC),
        Some(timestamp("2026-06-15T09:30:00Z"))
    );
    assert_eq!(
        next_daily_after(now, schedule, &TimeZone::UTC),
        timestamp("2026-06-22T09:30:00Z")
    );
}

#[test]
fn daily_schedule_rejects_empty_weekdays() {
    let result = std::panic::catch_unwind(|| DailyJobSchedule::new(9, 30).weekdays(&[]));
    assert!(result.is_err());
}

#[test]
fn cron_schedule_rejects_subsecond_interval() {
    let result = std::panic::catch_unwind(|| {
        CronJob::interval("test", "test", StdDuration::from_millis(500))
    });

    assert!(result.is_err());
}

#[test]
fn cron_schedule_rejects_fractional_second_interval() {
    let result = std::panic::catch_unwind(|| {
        CronJob::interval("test", "test", StdDuration::from_millis(1_500))
    });

    assert!(result.is_err());
}

#[test]
fn latest_monthly_due_at_uses_current_month_after_slot_passed() {
    let schedule = MonthlyJobSchedule::new(1, 3, 30);
    let now = timestamp("2026-06-01T04:00:00Z");

    assert_eq!(
        latest_monthly_due_at(now, schedule, &TimeZone::UTC),
        Some(timestamp("2026-06-01T03:30:00Z"))
    );
    assert_eq!(
        next_monthly_after(now, schedule, &TimeZone::UTC),
        timestamp("2026-07-01T03:30:00Z")
    );
}

#[test]
fn latest_monthly_due_at_uses_previous_month_before_slot_passed() {
    let schedule = MonthlyJobSchedule::new(1, 3, 30);
    let now = timestamp("2026-06-01T03:00:00Z");

    assert_eq!(
        latest_monthly_due_at(now, schedule, &TimeZone::UTC),
        Some(timestamp("2026-05-01T03:30:00Z"))
    );
    assert_eq!(
        next_monthly_after(now, schedule, &TimeZone::UTC),
        timestamp("2026-06-01T03:30:00Z")
    );
}

#[test]
fn cron_rejects_unknown_time_zone() {
    let result = CronJob::daily_in_timezone(
        "test",
        "test",
        DailyJobSchedule::new(9, 30),
        "Mars/Olympus_Mons",
    );

    assert!(result.is_err());
}

#[test]
fn daily_schedule_uses_named_time_zone_offset() {
    let time_zone = TimeZone::get("America/New_York").unwrap();
    let schedule = DailyJobSchedule::new(9, 30);
    let now = timestamp("2026-06-13T14:00:00Z");

    assert_eq!(
        latest_daily_due_at(now, schedule, &time_zone),
        Some(timestamp("2026-06-13T13:30:00Z"))
    );
    assert_eq!(
        next_daily_after(now, schedule, &time_zone),
        timestamp("2026-06-14T13:30:00Z")
    );
}

#[test]
fn daily_schedule_moves_forward_through_dst_gap() {
    let time_zone = TimeZone::get("America/New_York").unwrap();
    let schedule = DailyJobSchedule::new(2, 30);
    let now = timestamp("2026-03-08T08:00:00Z");

    assert_eq!(
        latest_daily_due_at(now, schedule, &time_zone),
        Some(timestamp("2026-03-08T07:30:00Z"))
    );
}

#[test]
fn daily_schedule_runs_once_at_earlier_dst_fold_time() {
    let time_zone = TimeZone::get("America/New_York").unwrap();
    let schedule = DailyJobSchedule::new(1, 30);
    let between_repetitions = timestamp("2026-11-01T06:00:00Z");

    assert_eq!(
        latest_daily_due_at(between_repetitions, schedule, &time_zone),
        Some(timestamp("2026-11-01T05:30:00Z"))
    );
    assert_eq!(
        next_daily_after(between_repetitions, schedule, &time_zone),
        timestamp("2026-11-02T06:30:00Z")
    );
}
