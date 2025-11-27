#!/usr/bin/env python3
"""
C86 DDA Alerts – Python migration of SAS job
--------------------------------------------

- Trino used in place of Hive/Hadoop
- teradatasql used for Teradata
- pandas for data-step / proc-sql logic

This file mirrors the SAS sections 1–12 as closely as possible.
"""

import os
import sys
import logging
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import trino
import teradatasql

# --------------------------------------------------------------------------------------
# SECTION 0: LOGGING / CONFIG BASICS
# --------------------------------------------------------------------------------------

logger = logging.getLogger("c86_dda_alerts")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)


@dataclass
class Paths:
    regpath: Path
    logpath: Path
    outpath: Path
    alrtdata: Path


@dataclass
class DateContext:
    today: date
    tday: date
    tday2: date
    week_start: date
    week_end: date
    launch_dt: date
    ongoing_start: date
    medate: date
    par_dt: date
    ymd2: str
    snap_dt_hive: str
    snap_dt_hive2: str
    date_com: str
    me_dt: str
    sasdt: date
    sasdt2: date


# --------------------------------------------------------------------------------------
# SECTION 1: SETUP & CONNECTIONS (Python equivalent)
# --------------------------------------------------------------------------------------

def get_paths(env: str = "PROD", regpath_override: str | None = None) -> Paths:
    """
    Rough equivalent of SAS regpath/logpath/outpath/alrtdata libname setup.
    """
    if regpath_override:
        reg_root = Path(regpath_override)
    else:
        if env.upper() == "PROD":
            reg_root = Path("/sas/RSD/REG")
        else:
            reg_root = Path("/sas/RSD/REG_DEV")

    logpath = reg_root / "C86" / "log" / "alert" / "dda"
    outpath = reg_root / "C86" / "output" / "alert" / "dda"
    alrtdata = outpath  # same as SAS libname

    for p in (logpath, outpath, alrtdata):
        p.mkdir(parents=True, exist_ok=True)

    return Paths(regpath=reg_root, logpath=logpath, outpath=outpath, alrtdata=alrtdata)


def init_logging_file(paths: Paths) -> Path:
    """
    Implements the SAS proc printto > log file behaviour.
    """
    today_ymd = date.today().strftime("%Y%m%d")
    logfile = paths.logpath / f"C86_Alert_DDA_{today_ymd}.log"

    file_handler = logging.FileHandler(logfile, mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Log file: {logfile}")
    return logfile


def get_trino_connection() -> trino.dbapi.Connection:
    """
    Create a Trino connection that replaces the Hive/Hadoop SAS connect.
    """
    host = os.environ.get("TRINO_HOST", "strplpaed12007.fg.rbc.com")
    port = int(os.environ.get("TRINO_PORT", "8443"))
    user = os.environ.get("TRINO_USER", "REPLACE_ME")
    catalog = os.environ.get("TRINO_CATALOG", "hive")
    schema = "prod_brt0_ess"

    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme="https",
        # auth=trino.auth.BasicAuthentication(user, "PASSWORD_OR_TOKEN"),
    )
    return conn


def get_teradata_connection() -> teradatasql.TeradataConnection:
    """
    Equivalent of %ConnectSql macro using teradatasql.
    """
    host = os.environ.get("TD_HOST", "uasasp11.fg.rbc.com")
    user = os.environ.get("TD_USER", "REPLACE_ME")
    password = os.environ.get("TD_PASSWORD", "REPLACE_ME")

    conn = teradatasql.connect(
        host=host,
        user=user,
        password=password,
    )
    return conn


# --------------------------------------------------------------------------------------
# SECTION 2: DATE & MACRO SETUP
# --------------------------------------------------------------------------------------

def compute_week_anchor_wed(today_: date | None = None) -> date:
    """
    SAS: tday = intnx('week.4', today(), 0);
    week.4 => weeks starting on Wednesday.
    """
    if today_ is None:
        today_ = date.today()
    days_since_wed = (today_.weekday() - 2) % 7  # Wednesday=2
    return today_ - timedelta(days=days_since_wed)


def compute_dates(ini_run: str) -> DateContext:
    """
    Port of the DATA _NULL_ date block.
    """
    tday2 = date.today()
    tday = compute_week_anchor_wed(tday2)

    launch_dt = date(2022, 6, 30)
    ongoing_start = max(tday - timedelta(days=180), launch_dt)

    week_end = tday - timedelta(days=2)  # Monday
    if ini_run == "N":
        week_start = week_end - timedelta(days=6)  # last Tuesday
    else:
        week_start = launch_dt

    first_of_curr_month = date(tday2.year, tday2.month, 1)
    todayd = first_of_curr_month + timedelta(days=14)
    first_of_todayd_month = date(todayd.year, todayd.month, 1)
    medate = first_of_todayd_month - timedelta(days=1)

    par_dt = week_start - timedelta(days=7)

    snap_dt_hive = week_start.strftime("%Y-%m-%d")
    snap_dt_hive2 = week_end.strftime("%Y-%m-%d")
    me_dt_str = medate.strftime("%Y-%m-%d")
    ymd2 = week_end.strftime("%Y%m%d")
    date_com = tday2.strftime("%Y-%m-%d")

    return DateContext(
        today=tday2,
        tday=tday,
        tday2=tday2,
        week_start=week_start,
        week_end=week_end,
        launch_dt=launch_dt,
        ongoing_start=ongoing_start,
        medate=medate,
        par_dt=par_dt,
        ymd2=ymd2,
        snap_dt_hive=snap_dt_hive,
        snap_dt_hive2=snap_dt_hive2,
        date_com=date_com,
        me_dt=me_dt_str,
        sasdt=week_start,
        sasdt2=week_end,
    )


def ini_check(paths: Paths) -> str:
    """
    SAS ini_run check; here we look for a parquet 'current' dataset.
    """
    curr = paths.outpath / "dda_alert_ac_curr.parquet"
    backup = paths.outpath / "dda_alert_ac_curr_backup.parquet"

    if curr.exists():
        logger.info(f"Found existing current file: {curr}")
        if not backup.exists():
            curr.replace(backup)
            logger.info(f"Backed up to: {backup}")
        ini_run = "N"
    else:
        ini_run = "Y"

    return ini_run


# --------------------------------------------------------------------------------------
# SECTION 3: HIVE → TRINO DATA EXTRACTION
# --------------------------------------------------------------------------------------

def fetch_pref_init(conn_trino: trino.dbapi.Connection) -> pd.DataFrame:
    sql = """
    WITH base AS (
      SELECT
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_process_timestamp')) AS timestamp))
          AS ess_process_timestamp_p,
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_src_event_timestamp')) AS timestamp))
          AS ess_src_event_timestamp_p,
        try(json_parse(element_at(eventattributes, 'SourceEventHeader'))) AS hdr,
        try(json_parse(element_at(eventattributes, 'eventPayload')))      AS payload
      FROM prod_brt0_ess.ffs0___client_alert_preferences_dep___initial_load
      WHERE partition_date = DATE '2022-03-24'
    )
    SELECT
      ess_process_timestamp_p,
      ess_src_event_timestamp_p,
      try(CAST(from_iso8601_timestamp(json_extract_scalar(hdr, '$.eventTimestamp')) AS timestamp))
        AS eventtimestamp_p,

      json_extract_scalar(payload, '$.preferenceType')      AS preferencetype_p,
      json_extract_scalar(payload, '$.clientId')            AS clientid_p,
      json_extract_scalar(payload, '$.sendAlertEligible')   AS sendalerteligible_p,
      json_extract_scalar(payload, '$.active')              AS active_p,
      try(CAST(json_extract_scalar(payload, '$.threshold') AS double)) AS threshold_p,
      json_extract_scalar(payload, '$.optOutDate')          AS optoutdate_p,
      json_extract_scalar(payload, '$.account')             AS account,
      json_extract_scalar(payload, '$.productType')         AS producttype_p
    FROM base
    WHERE json_extract_scalar(payload, '$.preferenceType') = 'DDA_BALANCE_ALERT'
    """
    logger.info("Running Trino PREF_INIT query...")
    return pd.read_sql(sql, conn_trino)


# def fetch_pref_init(conn_trino: trino.dbapi.Connection) -> pd.DataFrame:
#     """
#     SAS PREF_INIT on Hive → Trino.
#     """
#     sql = """
#     SELECT
#         CAST(regexp_replace(eventattributes['ess_process_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_process_timestamp_p,
#         CAST(regexp_replace(eventattributes['ess_src_event_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_src_event_timestamp_p,
#         CAST(regexp_replace(
#                 json_extract_scalar(eventattributes['SourceEventHeader'], '$.eventTimestamp'),
#                 'T|Z', ' '
#             ) AS timestamp) AS eventTimestamp_p,

#         json_extract_scalar(eventattributes['eventPayload'], '$.preferenceType') AS preferenceType_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.clientId')        AS clientId_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.sendAlertEligible') AS sendAlertEligible_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.active')         AS active_p,
#         CAST(json_extract_scalar(eventattributes['eventPayload'], '$.threshold') AS double) AS threshold_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.optOutDate')     AS optOutDate_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.account')        AS account,
#         json_extract_scalar(eventattributes['eventPayload'], '$.productType')    AS productType_p
#     FROM prod_brt0_ess.ffs0___client_alert_preferences_dep___initial_load
#     WHERE json_extract_scalar(eventattributes['eventPayload'], '$.preferenceType')
#               = 'DDA_BALANCE_ALERT'
#       AND partition_date = DATE '2022-03-24'
#     """
#     logger.info("Running Trino PREF_INIT query...")
#     return pd.read_sql(sql, conn_trino)


# def fetch_pref_new(conn_trino: trino.dbapi.Connection) -> pd.DataFrame:
#     """
#     SAS PREF_NEW on Hive → Trino.
#     """
#     sql = """
#     SELECT
#         CAST(regexp_replace(eventattributes['ess_process_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_process_timestamp_p,
#         CAST(regexp_replace(eventattributes['ess_src_event_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_src_event_timestamp_p,
#         CAST(regexp_replace(
#                 json_extract_scalar(eventattributes['SourceEventHeader'], '$.eventTimestamp'),
#                 'T|Z', ' '
#             ) AS timestamp) AS eventTimestamp_p,

#         json_extract_scalar(eventattributes['eventPayload'], '$.preferenceType') AS preferenceType_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.clientId')        AS clientId_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.sendAlertEligible') AS sendAlertEligible_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.active')         AS active_p,
#         CAST(json_extract_scalar(eventattributes['eventPayload'], '$.threshold') AS double) AS threshold_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.optOutDate')     AS optOutDate_p,
#         json_extract_scalar(eventattributes['eventPayload'], '$.accountId')      AS accountId,
#         json_extract_scalar(eventattributes['eventPayload'], '$.productType')    AS productType_p
#     FROM prod_brt0_ess.ffs0___client_alert_preferences_dep
#     WHERE json_extract_scalar(eventattributes['eventPayload'], '$.preferenceType')
#               = 'DDA_BALANCE_ALERT'
#     """
#     logger.info("Running Trino PREF_NEW query...")
#     return pd.read_sql(sql, conn_trino)

def fetch_pref_new(conn_trino: trino.dbapi.Connection) -> pd.DataFrame:
    sql = """
    WITH base AS (
      SELECT
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_process_timestamp')) AS timestamp))
          AS ess_process_timestamp_p,
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_src_event_timestamp')) AS timestamp))
          AS ess_src_event_timestamp_p,
        try(json_parse(element_at(eventattributes, 'SourceEventHeader'))) AS hdr,
        try(json_parse(element_at(eventattributes, 'eventPayload')))      AS payload
      FROM prod_brt0_ess.ffs0___client_alert_preferences_dep
    )
    SELECT
      ess_process_timestamp_p,
      ess_src_event_timestamp_p,
      try(CAST(from_iso8601_timestamp(json_extract_scalar(hdr, '$.eventTimestamp')) AS timestamp))
        AS eventtimestamp_p,

      json_extract_scalar(payload, '$.preferenceType')      AS preferencetype_p,
      json_extract_scalar(payload, '$.clientId')            AS clientid_p,
      json_extract_scalar(payload, '$.sendAlertEligible')   AS sendalerteligible_p,
      json_extract_scalar(payload, '$.active')              AS active_p,
      try(CAST(json_extract_scalar(payload, '$.threshold') AS double)) AS threshold_p,
      json_extract_scalar(payload, '$.optOutDate')          AS optoutdate_p,

      -- some records store accountId, some store account; keep both safely
      coalesce(
        json_extract_scalar(payload, '$.accountId'),
        json_extract_scalar(payload, '$.account')
      ) AS accountid,

      json_extract_scalar(payload, '$.productType')         AS producttype_p
    FROM base
    WHERE json_extract_scalar(payload, '$.preferenceType') = 'DDA_BALANCE_ALERT'
    """
    logger.info("Running Trino PREF_NEW query...")
    return pd.read_sql(sql, conn_trino)

def fetch_colt_start(conn_trino: trino.dbapi.Connection, dc: DateContext) -> pd.DataFrame:
    sql = f"""
    WITH base AS (
      SELECT
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_process_timestamp')) AS timestamp))
          AS ess_process_timestamp_c,
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_src_event_timestamp')) AS timestamp))
          AS ess_src_event_timestamp_c,
        try(json_parse(element_at(eventattributes, 'SourceEventHeader'))) AS hdr,
        try(json_parse(element_at(eventattributes, 'eventPayload')))      AS payload,
        partition_date
      FROM prod_brt0_ess.zgv0___colt_front_end_system
      WHERE partition_date > DATE '{dc.par_dt.strftime("%Y-%m-%d")}'
    )
    SELECT
      ess_process_timestamp_c,
      ess_src_event_timestamp_c,
      try(CAST(from_iso8601_timestamp(json_extract_scalar(hdr, '$.eventTimestamp')) AS timestamp))
        AS eventtimestamp_c,
      json_extract_scalar(hdr, '$.eventActivityName') AS eventactivityname_c,

      json_extract_scalar(payload, '$.alertType') AS alerttype_c,
      json_extract_scalar(payload, '$.clientId')  AS clientid_c,

      try(CAST(json_extract_scalar(payload, '$.thresholdAmount') AS double)) AS thresholdamount_c,

      try(CAST(from_iso8601_timestamp(json_extract_scalar(payload, '$.transactionTimestamp')) AS timestamp))
        AS transactiontimestamp_c,

      try(CAST(json_extract_scalar(payload, '$.alertAmount') AS double))       AS alertamount_c,
      try(CAST(json_extract_scalar(payload, '$.previousBalance') AS double))   AS previousbalance_c,

      json_extract_scalar(payload, '$.accountStatus')    AS accountstatus_c,
      json_extract_scalar(payload, '$.accountId')        AS accountid,
      json_extract_scalar(payload, '$.processingCentre') AS processingcentre_c,
      json_extract_scalar(payload, '$.accountCloseInd')  AS accountcloseind_c,
      json_extract_scalar(payload, '$.decisionId')       AS decisionid,
      json_extract_scalar(payload, '$.reasonCodes')      AS reasoncodes_c
    FROM base
    WHERE json_extract_scalar(payload, '$.alertType') = 'DDA_BALANCE_ALERT'
      AND transactiontimestamp_c BETWEEN TIMESTAMP '{dc.snap_dt_hive} 00:00:00'
                                    AND TIMESTAMP '{dc.today.strftime("%Y-%m-%d")} 23:59:59'
    """
    logger.info("Running Trino COLT_START query...")
    return pd.read_sql(sql, conn_trino)


# def fetch_colt_start(conn_trino: trino.dbapi.Connection, dc: DateContext) -> pd.DataFrame:
#     """
#     SAS COLT_START on Hive → Trino.
#     """
#     sql = f"""
#     SELECT
#         CAST(regexp_replace(eventattributes['ess_process_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_process_timestamp_c,
#         CAST(regexp_replace(eventattributes['ess_src_event_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_src_event_timestamp_c,
#         CAST(regexp_replace(
#                 json_extract_scalar(eventattributes['SourceEventHeader'], '$.eventTimestamp'),
#                 'T|Z', ' '
#             ) AS timestamp) AS eventTimestamp_c,
#         json_extract_scalar(eventattributes['SourceEventHeader'], '$.eventActivityName')
#             AS eventActivityName_c,

#         json_extract_scalar(eventattributes['eventPayload'], '$.alertType')        AS alertType_c,
#         json_extract_scalar(eventattributes['eventPayload'], '$.clientId')        AS clientId_c,
#         CAST(json_extract_scalar(eventattributes['eventPayload'], '$.thresholdAmount') AS double)
#             AS thresholdAmount_c,
#         CAST(regexp_replace(
#                 json_extract_scalar(eventattributes['eventPayload'], '$.transactionTimestamp'),
#                 'T|Z', ' '
#             ) AS timestamp) AS transactionTimestamp_c,
#         CAST(json_extract_scalar(eventattributes['eventPayload'], '$.alertAmount') AS double)
#             AS alertAmount_c,
#         CAST(json_extract_scalar(eventattributes['eventPayload'], '$.previousBalance') AS double)
#             AS previousBalance_c,
#         json_extract_scalar(eventattributes['eventPayload'], '$.accountStatus')   AS accountStatus_c,
#         json_extract_scalar(eventattributes['eventPayload'], '$.accountId')       AS accountId,
#         json_extract_scalar(eventattributes['eventPayload'], '$.processingCentre') AS processingCentre_c,
#         json_extract_scalar(eventattributes['eventPayload'], '$.accountCloseInd') AS accountCloseInd_c,
#         json_extract_scalar(eventattributes['eventPayload'], '$.decisionId')      AS decisionId,
#         json_extract_scalar(eventattributes['eventPayload'], '$.reasonCodes')     AS reasonCodes_c
#     FROM prod_brt0_ess.zgv0___colt_front_end_system
#     WHERE partition_date > DATE '{dc.par_dt.strftime("%Y-%m-%d")}'
#       AND json_extract_scalar(eventattributes['eventPayload'], '$.alertType')
#               = 'DDA_BALANCE_ALERT'
#       AND CAST(regexp_replace(
#                 json_extract_scalar(eventattributes['eventPayload'], '$.transactionTimestamp'),
#                 'T|Z', ' '
#           ) AS timestamp)
#           BETWEEN TIMESTAMP '{dc.snap_dt_hive} 00:00:00'
#               AND TIMESTAMP '{dc.today.strftime("%Y-%m-%d")} 23:59:59'
#     """
#     logger.info("Running Trino COLT_START query...")
#     return pd.read_sql(sql, conn_trino)

def fetch_alert_inbox(conn_trino: trino.dbapi.Connection, dc: DateContext) -> pd.DataFrame:
    sql = f"""
    WITH base AS (
      SELECT
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_process_timestamp')) AS timestamp))
          AS ess_process_timestamp_a,
        try(CAST(from_iso8601_timestamp(element_at(eventattributes, 'ess_src_event_timestamp')) AS timestamp))
          AS ess_src_event_timestamp_a,
        try(json_parse(element_at(eventattributes, 'SourceEventHeader'))) AS hdr,
        try(json_parse(element_at(eventattributes, 'eventPayload')))      AS payload,
        partition_date
      FROM prod_brt0_ess.fft0___alert_inbox_dep
      WHERE partition_date > DATE '{dc.par_dt.strftime("%Y-%m-%d")}'
    )
    SELECT
      ess_process_timestamp_a,
      ess_src_event_timestamp_a,
      try(CAST(from_iso8601_timestamp(json_extract_scalar(hdr, '$.eventTimestamp')) AS timestamp))
        AS eventtimestamp_a,

      json_extract_scalar(payload, '$.alertType')       AS alerttype_a,
      json_extract_scalar(payload, '$.decisionId')      AS decisionid,
      json_extract_scalar(payload, '$.accountId')       AS accountid,
      try(CAST(json_extract_scalar(payload, '$.alertAmount') AS double))     AS alertamount_a,
      try(CAST(json_extract_scalar(payload, '$.thresholdAmount') AS double)) AS thresholdamount_a,
      json_extract_scalar(payload, '$.alertSent')       AS alertsent_a,
      json_extract_scalar(payload, '$.reasonCode')      AS reasoncode_a
    FROM base
    WHERE ess_src_event_timestamp_a >= TIMESTAMP '{dc.snap_dt_hive} 00:00:00'
      AND json_extract_scalar(payload, '$.alertType') = 'DDA_BALANCE_ALERT'
    """
    logger.info("Running Trino ALERT_INBOX query...")
    return pd.read_sql(sql, conn_trino)


# def fetch_alert_inbox(conn_trino: trino.dbapi.Connection, dc: DateContext) -> pd.DataFrame:
#     """
#     SAS ALERT_INBOX on Hive → Trino.
#     """
#     sql = f"""
#     SELECT
#         CAST(regexp_replace(eventattributes['ess_process_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_process_timestamp_a,
#         CAST(regexp_replace(eventattributes['ess_src_event_timestamp'], 'T|Z', ' ') AS timestamp)
#             AS ess_src_event_timestamp_a,
#         CAST(regexp_replace(
#                 json_extract_scalar(eventattributes['SourceEventHeader'], '$.eventTimestamp'),
#                 'T|Z', ' '
#             ) AS timestamp) AS eventTimestamp_a,

#         json_extract_scalar(eventattributes['eventPayload'], '$.alertType')       AS alertType_a,
#         json_extract_scalar(eventattributes['eventPayload'], '$.decisionId')     AS decisionId,
#         json_extract_scalar(eventattributes['eventPayload'], '$.accountId')      AS accountId,
#         CAST(json_extract_scalar(eventattributes['eventPayload'], '$.alertAmount') AS double)
#             AS alertAmount_a,
#         CAST(json_extract_scalar(eventattributes['eventPayload'], '$.thresholdAmount') AS double)
#             AS thresholdAmount_a,
#         json_extract_scalar(eventattributes['eventPayload'], '$.alertSent')      AS alertSent_a,
#         json_extract_scalar(eventattributes['eventPayload'], '$.reasonCode')     AS reasonCode_a
#     FROM prod_brt0_ess.fft0___alert_inbox_dep
#     WHERE partition_date > DATE '{dc.par_dt.strftime("%Y-%m-%d")}'
#       AND CAST(regexp_replace(eventattributes['ess_src_event_timestamp'], 'T|Z', ' ') AS timestamp)
#               >= TIMESTAMP '{dc.snap_dt_hive} 00:00:00'
#       AND json_extract_scalar(eventattributes['eventPayload'], '$.alertType')
#               = 'DDA_BALANCE_ALERT'
#     """
#     logger.info("Running Trino ALERT_INBOX query...")
#     return pd.read_sql(sql, conn_trino)


# --------------------------------------------------------------------------------------
# SECTION 7 SUPPORT: TIMELINESS SPLIT
# --------------------------------------------------------------------------------------

def split_timeliness_sets(
    alert_inbox_utc: pd.DataFrame,
    colt_decisioned: pd.DataFrame,
    dc: DateContext,
) -> dict[str, pd.DataFrame]:
    """
    Python equivalent of SAS data-step that produces:
      inboth_met_time, inboth_did_not_meet_time, incolt_only, inbox_only,
      transaction_utc_issue, alert_inbox_buffer, remaining_inbox.
    """

    left = alert_inbox_utc.copy()
    right = colt_decisioned.copy()

    merged = left.merge(
        right,
        on="decisionid",
        how="outer",
        suffixes=("_a", "_c"),
        indicator=True,
    )

    merged["inbox"] = merged["ess_process_timestamp_a"].notna()
    merged["incolt"] = merged["transactiontimestamp_c"].notna()

    merged["transactiontimestamp_c"] = pd.to_datetime(
        merged["transactiontimestamp_c"], utc=True
    )
    merged["ess_src_event_timestamp_a"] = pd.to_datetime(
        merged["ess_src_event_timestamp_a"], utc=True
    )

    merged["timecheck"] = (
        merged["ess_src_event_timestamp_a"] - merged["transactiontimestamp_c"]
    ).dt.total_seconds()

    merged["ess_src_event_a"] = merged["ess_src_event_timestamp_a"].dt.date
    merged["transaction_date"] = pd.to_datetime(
        merged["transactionTimestamp_c2"]
    ).dt.date

    def compute_newdate(d: date) -> date | None:
        if pd.isna(d):
            return None
        if d.weekday() == 0:  # Monday
            return d
        days_until_next_monday = (7 - d.weekday()) % 7
        if days_until_next_monday == 0:
            days_until_next_monday = 7
        return d + timedelta(days=days_until_next_monday)

    merged["newdate"] = merged["transaction_date"].apply(compute_newdate)

    merged["RegulatoryName"] = "C86"
    merged["LOB"] = "Payments & Banking"
    merged["ReportName"] = "C86 Alerts"
    merged["ProductType"] = "Personal Deposit Accounts"

    sasdt = dc.sasdt
    sasdt2 = dc.sasdt2

    inbox = merged["inbox"]
    incolt = merged["incolt"]
    t_a2 = pd.to_datetime(merged["ess_src_event_timestamp_a2"]).dt.date

    inboth_met_time = merged[inbox & incolt & (merged["timecheck"] <= 1800)].copy()
    inboth_did_not_meet_time = merged[
        inbox & incolt & (merged["timecheck"] > 1800)
    ].copy()
    incolt_only = merged[~inbox & incolt].copy()
    inbox_only = merged[
        inbox & ~incolt & t_a2.le(sasdt2) & t_a2.gt(sasdt)
    ].copy()
    transaction_utc_issue = merged[inbox & ~incolt & t_a2.le(sasdt)].copy()
    alert_inbox_buffer = merged[inbox & ~incolt & t_a2.gt(sasdt2)].copy()
    remaining_inbox = merged[inbox & ~incolt & t_a2.isna()].copy()

    return {
        "inboth_met_time": inboth_met_time,
        "inboth_did_not_meet_time": inboth_did_not_meet_time,
        "incolt_only": incolt_only,
        "inbox_only": inbox_only,
        "transaction_utc_issue": transaction_utc_issue,
        "alert_inbox_buffer": alert_inbox_buffer,
        "remaining_inbox": remaining_inbox,
    }


# --------------------------------------------------------------------------------------
# SECTION 4: TERADATA PULLS & HOLIDAY CALENDAR
# --------------------------------------------------------------------------------------

def fetch_calendar(conn_td, dc: DateContext) -> pd.DataFrame:
    """
    SAS Calendar query from DDWV01.HOLIDY.
    """
    sql = f"""
    SELECT DISTINCT
        holidy_dt        AS DATE_T,
        holidy_dt + INTERVAL '1' DAY AS DATE_T2,
        Holidy_TYP,
        subcntry_cd,
        cntry_Cd,
        snap_dt
    FROM ddwv01.HOLIDY
    WHERE SNAP_DT = DATE '{dc.me_dt}'
      AND (
            Holidy_TYP = 'F'
         OR (Holidy_TYP = 'P' AND subcntry_cd = 'ON')
         OR (Holidy_TYP = 'P' AND subcntry_cd = 'QC')
      )
      AND holidy_dt BETWEEN DATE '2021-11-01' AND CURRENT_DATE
    ORDER BY 1
    """
    logger.info("Running Teradata HOLIDY query...")
    return pd.read_sql(sql, conn_td)


# --------------------------------------------------------------------------------------
# SECTION 5: COLT PROCESSING & SAMPLING (pandas)
# --------------------------------------------------------------------------------------

def adjust_transaction_date_timezone(colt_start: pd.DataFrame) -> pd.DataFrame:
    """
    Convert transactionTimestamp_c (UTC) to local tz and derive transaction_date.
    """
    df = colt_start.copy()
    df["transactionTimestamp_c"] = pd.to_datetime(df["transactiontimestamp_c"], utc=True)

    pacific = "America/Vancouver"
    eastern = "America/Toronto"

    cond_b = df["processingcentre_c"] == "B"

    df["transactionTimestamp_c_local"] = np.where(
        cond_b,
        df["transactionTimestamp_c"].dt.tz_convert(pacific),
        df["transactionTimestamp_c"].dt.tz_convert(eastern),
    )
    df["transactionTimestamp_c2"] = df["transactionTimestamp_c_local"].dt.date
    df["transaction_date"] = df["transactionTimestamp_c2"]

    return df


def build_holiday_calendar(calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge DATE_T and DATE_T2 into a single holiday transaction_date column.
    """
    cal1 = calendar_df[["date_t"]].rename(columns={"date_t": "transaction_date"})
    cal2 = calendar_df[["date_t2"]].rename(columns={"date_t2": "transaction_date"})
    holiday = pd.concat([cal1, cal2], ignore_index=True).drop_duplicates()
    holiday = holiday.sort_values("transaction_date")
    return holiday


def filter_colt_window(colt: pd.DataFrame, dc: DateContext) -> pd.DataFrame:
    """
    Restrict COLT to sasdt–sasdt2 window.
    """
    df = colt.copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    mask = (df["transaction_date"] >= dc.sasdt) & (df["transaction_date"] <= dc.sasdt2)
    return df.loc[mask].copy()


def merge_colt_holiday(colt2: pd.DataFrame, holiday: pd.DataFrame) -> pd.DataFrame:
    """
    Merge COLT with holiday calendar and compute newdate (next Monday).
    """
    holiday["transaction_date"] = pd.to_datetime(holiday["transaction_date"]).dt.date
    colt2["transaction_date"] = pd.to_datetime(colt2["transaction_date"]).dt.date

    merged = colt2.merge(
        holiday,
        on="transaction_date",
        how="left",
        indicator=False,
    )

    def compute_newdate(d: date) -> date | None:
        if d is None:
            return None
        if d.weekday() == 0:
            return d
        days_until_next_monday = (7 - d.weekday()) % 7
        if days_until_next_monday == 0:
            days_until_next_monday = 7
        return d + timedelta(days=days_until_next_monday)

    merged["newdate"] = merged["transaction_date"].apply(compute_newdate)
    return merged


def sample_dda(indata: pd.DataFrame) -> pd.DataFrame:
    """
    Approximate %sample_dda:
      - samp_size = min(total size, 10)
      - SRS within each transaction_date stratum, up to samp_size per stratum.
    """
    if indata.empty:
        return indata.copy()

    df = indata.copy()
    df = df[df["transaction_date"].notna()]
    size = len(df)
    samp_size = min(size, 10)
    if samp_size <= 0:
        return df

    def sample_group(g: pd.DataFrame) -> pd.DataFrame:
        n = min(len(g), samp_size)
        return g.sample(n=n, replace=False, random_state=42)

    sampled = (
        df.groupby("transaction_date", group_keys=False)
        .apply(sample_group)
        .reset_index(drop=True)
    )
    return sampled


def add_ar_id(dda_colt: pd.DataFrame) -> pd.DataFrame:
    """
    ar_id = '00000000' || accountid
    """
    df = dda_colt.copy()
    df["accountId_str"] = df["accountid"].astype(str).str.zfill(7)
    df["ar_id"] = "00000000" + df["accountId_str"]
    return df


def build_in_lists_for_teradata(dda_colt: pd.DataFrame) -> tuple[list[str], list[int]]:
    """
    Build Python equivalents of macro lists dda_alert_list and dda_alert_list_clnt.
    """
    ar_ids = sorted(dda_colt["ar_id"].dropna().unique().tolist())
    client_ids = sorted(
        pd.to_numeric(dda_colt["clientid_c"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    return ar_ids, client_ids


# --------------------------------------------------------------------------------------
# SECTION 8: MACRO DDA_WEEK & TERADATA LOOPS
# --------------------------------------------------------------------------------------

def compute_snap_date_for_n(dc: DateContext, n: int) -> date:
    """
    SAS RELTN_DLY = intnx('day', week_end, -n, 'B');
    """
    return dc.week_end - timedelta(days=n)


def build_in_clause_list_str(values: list[str | int]) -> str:
    """
    Turn Python list into a Teradata IN() list with proper quoting.
    """
    formatted = []
    for v in values:
        if isinstance(v, str):
            formatted.append("'" + v.replace("'", "''") + "'")
        else:
            formatted.append(str(v))
    return ", ".join(formatted)


def fetch_pers_pda_population_for_n(
    conn_td,
    snap_dt: date,
    ar_ids: list[str],
    client_ids: list[int],
) -> pd.DataFrame:
    """
    Direct translation of the %dda_week Teradata query.
    """
    if not ar_ids or not client_ids:
        return pd.DataFrame()

    ar_in = build_in_clause_list_str(ar_ids)
    clnt_in = build_in_clause_list_str(client_ids)

    snap_dt_str = snap_dt.strftime("%Y-%m-%d")

    sql = f"""
    SELECT
        CLNT_AR_RELTN_DLY.clnt_no,
        CLNT_AR_RELTN_DLY.PRMRY_CLNT_IND,
        CLNT_AR_RELTN_DLY.CLNT_AR_RELTN_TYP,
        CLNT_AR_RELTN_DLY.CLNT_TYP,
        AR_STATIC_DLY.AR_ID,
        AR_STATIC_DLY.SRVC_ID,
        AR_STATIC_DLY.SNAP_DT,
        AR_STATIC_DLY.CAPTR_DT,
        AR_STATIC_DLY.SPCFC_FND_IND,
        AR_STATIC_DLY.ACCT_NO,
        AR_STATIC_DLY.OPEN_CLS_STS,
        AR_STATIC_DLY.DT_OPEN,
        AR_STATIC_DLY.DT_CLS,
        AR_STATIC_DLY.CURR_CD,
        AR_STATIC_DLY.SHRT_NM,
        AR_STATIC_DLY.CHG_DT,
        AR_STATIC_DLY.LST_UPDT_DT_TM,
        AR_STATIC_DLY.ACCT_CATG_CD,
        AR_STATIC_DLY.ACCT_TYP,
        AR_STATIC_DLY.ACCT_CLS,
        DEPOSIT_ACCOUNT_DLY.RESTRAINT_DATE,
        DEPOSIT_ACCOUNT_DLY.RESTRAINT_OVRRD_DT,
        DEPOSIT_ACCOUNT_DLY.RESTRNT_TYP_R_SPCL,
        DEPOSIT_ACCOUNT_DLY.DEP_ACCT_STS,
        DEPOSIT_ACCOUNT_DLY.MSG_CD1,
        DEPOSIT_ACCOUNT_DLY.MSG_CD2,
        DEPOSIT_ACCOUNT_DLY.MSG_CD3,
        DEPOSIT_ACCOUNT_DLY.MSG_CD4,
        DEPOSIT_ACCOUNT_DLY.MSG_CD5,
        AR_BAL_DLY.BAL,
        AR_BAL_DLY2.BAL AS bal_yesterday,

        CASE WHEN DEPOSIT_ACCOUNT_DLY.DEP_ACCT_STS BETWEEN 0 AND 6
             THEN 'Valid Status   '
             ELSE 'Invalid Status' END AS acct_status,

        CASE WHEN CLNT_AR_RELTN_DLY.CLNT_AR_RELTN_TYP IN (4,5,6)
             THEN 'Invalid Relationship type'
             ELSE 'Valid Relationship type' END AS Relationship_type,

        CASE WHEN  51 IN (
                        DEPOSIT_ACCOUNT_DLY.MSG_CD1,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD2,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD3,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD4,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD5
                    )
             THEN 'Invalid - deceased restraint'
             ELSE 'Valid - non-deceased' END AS deceased_status,

        CASE WHEN 65 IN (
                        DEPOSIT_ACCOUNT_DLY.MSG_CD1,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD2,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD3,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD4,
                        DEPOSIT_ACCOUNT_DLY.MSG_CD5
                    )
             THEN 'Invalid - Closed restraint'
             ELSE 'Valid - non-closed' END AS closed_status

    FROM DDWV01.CLNT_AR_RELTN_DLY      AS CLNT_AR_RELTN_DLY
         INNER JOIN DDWV01.AR_STATIC_DLY    AS AR_STATIC_DLY
           ON CLNT_AR_RELTN_DLY.ar_id    = AR_STATIC_DLY.ar_id
          AND CLNT_AR_RELTN_DLY.snap_dt  = DATE '{snap_dt_str}'
          AND CLNT_AR_RELTN_DLY.snap_dt  = AR_STATIC_DLY.snap_dt

         LEFT JOIN DDWV01.DEPOSIT_ACCOUNT_DLY AS DEPOSIT_ACCOUNT_DLY
           ON CLNT_AR_RELTN_DLY.ar_id    = DEPOSIT_ACCOUNT_DLY.ar_id
          AND DEPOSIT_ACCOUNT_DLY.snap_dt= DATE '{snap_dt_str}'

         LEFT JOIN DDWV01.AR_BAL_DLY AS AR_BAL_DLY
           ON CLNT_AR_RELTN_DLY.ar_id    = AR_BAL_DLY.ar_id
          AND AR_BAL_DLY.snap_dt         = DATE '{snap_dt_str}'

         LEFT JOIN
         (
            SELECT
                AR_BAL_DLY_pre1.ar_id,
                AR_BAL_DLY_pre1.bal
            FROM DDWV01.AR_BAL_DLY AS AR_BAL_DLY_pre1
            INNER JOIN
            (
                SELECT
                    AR_BAL_DLY_inner.ar_id,
                    MAX(AR_BAL_DLY_inner.snap_dt) AS max_snap
                FROM DDWV01.AR_BAL_DLY AS AR_BAL_DLY_inner
                WHERE AR_BAL_DLY_inner.snap_dt < DATE '{snap_dt_str}'
                  AND AR_BAL_DLY_inner.ar_id IN ({ar_in})
                GROUP BY 1
            ) AS AR_BAL_DLY_pre2
            ON AR_BAL_DLY_pre1.ar_id   = AR_BAL_DLY_pre2.ar_id
           AND AR_BAL_DLY_pre1.snap_dt = AR_BAL_DLY_pre2.max_snap
         ) AS AR_BAL_DLY2
           ON CLNT_AR_RELTN_DLY.ar_id = AR_BAL_DLY2.ar_id

    WHERE
        CLNT_AR_RELTN_DLY.ar_id   IN ({ar_in})
      AND CLNT_AR_RELTN_DLY.clnt_no IN ({clnt_in})
    """
    logger.info(f"Running Teradata pers_pda_population for snap_dt={snap_dt_str} ...")
    return pd.read_sql(sql, conn_td)


def build_pers_pda_population(
    conn_td,
    dc: DateContext,
    ar_ids: list[str],
    client_ids: list[int],
    max_n: int = 13,
) -> pd.DataFrame:
    """
    Equivalent of %dda_week(0–13) concatenation + bal_yesterday fill.
    """
    all_parts = []
    for n in range(max_n + 1):
        snap_dt = compute_snap_date_for_n(dc, n)
        df_n = fetch_pers_pda_population_for_n(conn_td, snap_dt, ar_ids, client_ids)
        if not df_n.empty:
            all_parts.append(df_n)

    if not all_parts:
        return pd.DataFrame()

    pers = pd.concat(all_parts, ignore_index=True)
    pers["bal_yesterday"] = pers["bal_yesterday"].fillna(0)
    return pers


# --------------------------------------------------------------------------------------
# SECTION 9–11: LOGIC & FLAGS / TIMELINESS / REPORTING (core pieces)
# --------------------------------------------------------------------------------------

def merge_pref(pref_init: pd.DataFrame, pref_new: pd.DataFrame) -> pd.DataFrame:
    """
    SAS pref = pref_init + pref_new2 (account substr(accountId,6,7)).
    """
    pn = pref_new.copy()
    pn["account"] = pn["accountid"].astype(str).str[-7:]
    pn = pn.drop(columns=["accountid"])
    pref = pd.concat([pref_init, pn], ignore_index=True)
    return pref


def build_calendar_holiday(calendar_df: pd.DataFrame) -> pd.DataFrame:
    cal = calendar_df.copy()
    cal["DATE_T"] = pd.to_datetime(cal["date_t"]).dt.date
    cal["DATE_T2"] = pd.to_datetime(cal["date_t2"]).dt.date
    holiday = build_holiday_calendar(
        cal.rename(columns={"DATE_T": "date_t", "DATE_T2": "date_t2"})
    )
    return holiday


def build_colt_pipeline(colt_start: pd.DataFrame, holiday: pd.DataFrame, dc: DateContext) -> pd.DataFrame:
    colt = adjust_transaction_date_timezone(colt_start)
    colt2 = filter_colt_window(colt, dc)
    colt3 = merge_colt_holiday(colt2, holiday)

    colt3 = colt3.sort_values(
        ["clientid_c", "accountid", "transaction_date", "transactiontimestamp_c"],
        ascending=[True, True, True, False],
    )
    colt4 = colt3.drop_duplicates(
        subset=["clientid_c", "accountid", "transaction_date"], keep="first"
    )
    colt4 = colt4.sort_values("transaction_date").reset_index(drop=True)
    return colt4


def build_dda_colt_sample(colt4: pd.DataFrame) -> pd.DataFrame:
    dda_colt = sample_dda(colt4)
    dda_colt = add_ar_id(dda_colt)
    return dda_colt


def join_dda_colt_inbox(dda_colt: pd.DataFrame, alert_inbox: pd.DataFrame) -> pd.DataFrame:
    """
    merge dda_colt (in=inbase) alert_inbox; by decisionid; if inbase;
    """
    df = dda_colt.merge(alert_inbox, on="decisionid", how="left", suffixes=("", "_a"))
    df["account"] = df["accountid"].astype(str).str[-7:]
    return df


def compute_pref_summary(dda_colt_inbox: pd.DataFrame, pref: pd.DataFrame) -> pd.DataFrame:
    """
    SAS pref_sum: last pref record before transaction.
    """
    pref_ = pref.copy()
    pref_["ess_src_event_timestamp_p"] = pd.to_datetime(pref_["ess_src_event_timestamp_p"])

    c = dda_colt_inbox.copy()
    c["transactiontimestamp_c"] = pd.to_datetime(c["transactiontimestamp_c"])

    merged = c.merge(
        pref_[
            [
                "account",
                "clientid_p",
                "preferencetype_p",
                "ess_src_event_timestamp_p",
            ]
        ],
        left_on=["account", "clientid_c", "alerttype_c"],
        right_on=["account", "clientid_p", "preferencetype_p"],
        how="left",
        suffixes=("", "_pref"),
    )

    cond = merged["ess_src_event_timestamp_p"] <= merged["transactiontimestamp_c"]
    merged = merged[cond].copy()

    grp_cols = ["account", "clientid_c", "alerttype_c", "transactiontimestamp_c"]
    agg = (
        merged.groupby(grp_cols)["ess_src_event_timestamp_p"]
        .max()
        .reset_index()
        .rename(columns={"ess_src_event_timestamp_p": "pref_time"})
    )
    return agg


def build_dda_total_hive(
    dda_colt_inbox: pd.DataFrame,
    pref: pd.DataFrame,
    pref_sum: pd.DataFrame,
) -> pd.DataFrame:
    """
    Implement the big SAS join into dda_total_hive.
    """
    pref_ = pref.copy()
    pref_["ess_src_event_timestamp_p"] = pd.to_datetime(pref_["ess_src_event_timestamp_p"])

    pp = pref_.merge(
        pref_sum,
        left_on=["account", "clientid_p", "preferencetype_p", "ess_src_event_timestamp_p"],
        right_on=["account", "clientid_c", "alerttype_c", "pref_time"],
        how="inner",
        suffixes=("", "_sum"),
    )

    dda = dda_colt_inbox.merge(
        pp,
        left_on=["account", "clientid_c", "alerttype_c", "transactiontimestamp_c"],
        right_on=["account", "clientid_c", "alerttype_c", "transactiontimestamp_c"],
        how="left",
        suffixes=("", "_pref"),
    )

    dda = dda.drop_duplicates(subset=["clientid_c", "ar_id"])
    dda["clnt_no"] = pd.to_numeric(dda["clientid_c"], errors="coerce")
    return dda


def build_edw_hive_full_with_new_accounts(
    pers_pda_population: pd.DataFrame,
    dda_total_hive: pd.DataFrame,
    edw_hive_full_init: pd.DataFrame,
) -> pd.DataFrame:
    """
    Implements the SAS 'new_acct' + 'edw_hive_full_new_acct*' logic.
    """
    keys = ["clnt_no", "ar_id"]

    existing_keys = edw_hive_full_init[keys].drop_duplicates()
    hive_keys = dda_total_hive[keys].drop_duplicates()

    new_keys = hive_keys.merge(existing_keys, on=keys, how="left", indicator=True)
    new_keys = new_keys[new_keys["_merge"] == "left_only"][keys]

    if new_keys.empty:
        return edw_hive_full_init.copy()

    new_acct = dda_total_hive.merge(new_keys, on=keys, how="inner")

    edw_new = pers_pda_population.merge(
        new_acct,
        on=keys,
        how="inner",
        suffixes=("", "_hive"),
    )

    edw_new = apply_pass_logic(edw_new)

    edw_new["snap_dt"] = pd.to_datetime(edw_new["snap_dt"]).dt.date
    idx_oldest = edw_new.groupby(["clnt_no", "ar_id"])["snap_dt"].idxmin()
    edw_new2 = edw_new.loc[idx_oldest].reset_index(drop=True)

    edw_full = pd.concat([edw_hive_full_init, edw_new2], ignore_index=True)
    return edw_full


def apply_pass_logic(edw: pd.DataFrame) -> pd.DataFrame:
    """
    Port of the SAS Pass_flag + account_status logic.
    """
    df = edw.copy()

    df["snap_dt"] = pd.to_datetime(df["snap_dt"]).dt.date
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    df["correct_date"] = np.where(df["snap_dt"] == df["transaction_date"], "Y", "N")

    df["Pass_flag"] = "N"
    df.loc[df["correct_date"] == "N", "Pass_flag"] = "X"

    df["threshold_p"] = df.get("threshold_p", np.nan)
    df["threshold_p"] = df["threshold_p"].fillna(100)

    mask_decision = df["eventactivityname_c"] == "AlertDecision"
    cond_decision = (
        (df["acct_status"] == "Valid Status   ")
        & (df["Relationship_type"] == "Valid Relationship type")
        & (df["deceased_status"] == "Valid - non-deceased")
        & (df["closed_status"] == "Valid - non-closed")
        & (df["bal_yesterday"] > df["threshold_p"])
        & (df["bal"] < df["threshold_p"])
        & df["optoutdate_p"].isna()
        & (~df["accountcloseind_c"].fillna("").eq("true"))
    )
    df.loc[mask_decision & cond_decision, "Pass_flag"] = "Y"

    mask_supp = df["eventactivityname_c"] == "AlertSuppression"
    cond_supp = (
        (df["acct_status"] != "Valid Status   ")
        | (df["Relationship_type"] != "Valid Relationship type")
        | (df["deceased_status"] != "Valid - non-deceased")
        | (df["closed_status"] != "Valid - non-closed")
        | (df["bal_yesterday"] <= df["threshold_p"])
        | df["optoutdate_p"].notna()
        | df["accountcloseind_c"].fillna("").eq("true")
    )
    df.loc[mask_supp & cond_supp, "Pass_flag"] = "Y"

    dep = df["dep_acct_sts"]
    account_status = np.full(len(df), "Unknown", dtype=object)
    account_status[dep == 0] = "Active"
    account_status[dep == 2] = "Dormant"
    account_status[dep.isin([3, 4, 5])] = "Opened Today"
    account_status[dep == 6] = "Conversion Pending"
    account_status[dep.isin([7, 8, 9])] = "Closed"
    account_status[dep == 10] = "OATS transferred 1st day non financial"
    account_status[dep == 11] = "OATS transferred end of 1st day after financials are processed"
    account_status[dep == 12] = "OATS transfer complete"
    account_status[dep == 13] = "OATS transfer non zero balance"
    df["account_status"] = account_status

    df["RegulatoryName"] = "C86"
    df["LOB"] = "Payments & Banking"
    df["ReportName"] = "C86 Alerts"
    df["ProductType"] = "Personal Deposit Accounts"

    return df


def build_edw_hive_full_txn(edw: pd.DataFrame, dc: DateContext) -> pd.DataFrame:
    """
    Equivalent of SAS data edw_hive_full_txn.
    Adds Accuracy control columns and CommentCode/Comments.
    """
    df = edw.copy()

    df["alertamount"] = pd.to_numeric(df["alertamount_c"], errors="coerce")
    df["thresholdamount"] = pd.to_numeric(df["thresholdamount_c"], errors="coerce")
    df["previousbalance"] = pd.to_numeric(df["previousbalance_c"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date

    def compute_newdate(d: date) -> date | None:
        if d is None:
            return None
        if d.weekday() == 0:
            return d
        days_until_next_monday = (7 - d.weekday()) % 7
        if days_until_next_monday == 0:
            days_until_next_monday = 7
        return d + timedelta(days=days_until_next_monday)

    df["newdate"] = df["transaction_date"].apply(compute_newdate)
    df["ControlRisk"] = "Accuracy"

    df["CommentCode"] = "COM19"
    df["Comments"] = "Potential Fail"

    m_dec = df["eventactivityname_c"] == "AlertDecision"
    cond_dec = (
        (df["Pass_flag"] == "Y")
        & (df["previousbalance"] >= df["thresholdamount"])
        & (df["thresholdamount"] > df["alertamount"])
    )
    df.loc[m_dec & cond_dec, ["CommentCode", "Comments"]] = ["COM16", "Pass"]

    m_sup = df["eventactivityname_c"] == "AlertSuppression"
    cond_sup1 = df["Pass_flag"] == "Y"
    cond_sup2 = (df["Pass_flag"] == "N") & (df["previousbalance"] < df["thresholdamount"])
    cond_sup3 = (
        (df["Pass_flag"] == "N")
        & (df["previousbalance"] > df["thresholdamount"])
        & (df["thresholdamount"] > df["alertamount"])
    )
    df.loc[m_sup & (cond_sup1 | cond_sup2 | cond_sup3), ["CommentCode", "Comments"]] = [
        "COM16",
        "Pass",
    ]

    return df


def build_accuracy_dataset(edw_txn: pd.DataFrame, dc: DateContext) -> pd.DataFrame:
    """
    Aggregate edw_hive_full_txn to ac_accu_dda_alert.
    """
    df_valid = edw_txn[edw_txn["transaction_date"].notna()].copy()
    df_valid["TestType"] = "Sample"
    df_valid["TestPeriod"] = "Portfolio"
    df_valid["ProductType"] = "Personal Deposit Accounts"
    df_valid["RDE"] = "Alert001_Accuracy_Balance"
    df_valid["HoldoutFlag"] = "N"
    df_valid["segment10"] = df_valid["transaction_date"].apply(
        lambda d: d.strftime("%Y%m")
    )
    df_valid["DateCompleted"] = pd.to_datetime(dc.date_com).date()
    df_valid["SnapDate"] = df_valid["newdate"]

    group_cols = [
        "RegulatoryName",
        "LOB",
        "ReportName",
        "ControlRisk",
        "TestType",
        "TestPeriod",
        "ProductType",
        "RDE",
        "segment10",
        "HoldoutFlag",
        "CommentCode",
        "Comments",
        "DateCompleted",
        "SnapDate",
    ]

    grouped = (
        df_valid.groupby(group_cols)
        .agg(
            Volume=("accountid", "count"),
            Bal=("alertamount", "sum"),
            Amount=("thresholdamount", "sum"),
        )
        .reset_index()
    )
    return grouped


def build_timeliness_and_completeness_aggregates(
    timeliness_sets: dict[str, pd.DataFrame],
    dc: DateContext,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Recreates:
      - total_timeliness + ac_time_dda_alert
      - total_completeness + ac_comp_dda_alert
    Returns:
      ac_time_dda_alert, ac_comp_dda_alert, total_timeliness, total_completeness
    """

    inboth_met_time = timeliness_sets["inboth_met_time"]
    inboth_did_not_meet_time = timeliness_sets["inboth_did_not_meet_time"]
    incolt_only = timeliness_sets["incolt_only"]

    def to_numeric(series):
        return pd.to_numeric(series, errors="coerce")

    total_timeliness = pd.concat(
        [
            inboth_met_time.assign(_src="intime"),
            inboth_did_not_meet_time.assign(_src="notime"),
            incolt_only.assign(_src="inmiss"),
        ],
        ignore_index=True,
    )

    total_timeliness["ControlRisk"] = "Timeliness"
    total_timeliness["transaction_date"] = pd.to_datetime(
        total_timeliness["transactionTimestamp_c2"]
    ).dt.date

    total_timeliness["alertamount"] = to_numeric(total_timeliness["alertamount_c"])
    total_timeliness["thresholdamount"] = to_numeric(
        total_timeliness["thresholdamount_c"]
    )

    total_timeliness["CommentCode"] = "COM19"
    total_timeliness["Comments"] = "Potential Fail"

    total_timeliness.loc[
        total_timeliness["_src"] == "intime", ["CommentCode", "Comments"]
    ] = ["COM16", "Pass"]

    total_completeness = total_timeliness.copy()
    total_completeness["ControlRisk"] = "Completeness"

    total_completeness["CommentCode"] = np.where(
        total_completeness["_src"] == "inmiss", "COM19", "COM16"
    )
    total_completeness["Comments"] = np.where(
        total_completeness["_src"] == "inmiss", "Potential Fail", "Pass"
    )

    def aggregate_to_rde(df: pd.DataFrame, control_risk: str, rde_code: str) -> pd.DataFrame:
        d = df.copy()
        d = d[d["transaction_date"].notna()]

        d["RegulatoryName"] = "C86"
        d["LOB"] = "Payments & Banking"
        d["ReportName"] = "C86 Alerts"
        d["ProductType"] = "Personal Deposit Accounts"
        d["ControlRisk"] = control_risk
        d["TestType"] = "Anomaly" if control_risk == "Timeliness" else "Reconciliation"
        d["TestPeriod"] = "Portfolio"
        d["RDE"] = rde_code
        d["HoldoutFlag"] = "N"
        d["segment10"] = d["transaction_date"].apply(lambda x: x.strftime("%Y%m"))
        d["DateCompleted"] = pd.to_datetime(dc.date_com).date()
        d["SnapDate"] = d["newdate"]

        group_cols = [
            "RegulatoryName",
            "LOB",
            "ReportName",
            "ControlRisk",
            "TestType",
            "TestPeriod",
            "ProductType",
            "RDE",
            "segment10",
            "HoldoutFlag",
            "CommentCode",
            "Comments",
            "DateCompleted",
            "SnapDate",
        ]

        grouped = (
            d.groupby(group_cols)
            .agg(
                Volume=("accountid", "count"),
                Bal=("alertamount", "sum"),
                Amount=("thresholdamount", "sum"),
            )
            .reset_index()
        )
        return grouped

    ac_time_dda_alert = aggregate_to_rde(
        total_timeliness, "Timeliness", "Alert002_Timeliness_SLA"
    )
    ac_comp_dda_alert = aggregate_to_rde(
        total_completeness, "Completeness", "Alert003_Completeness_All_Clients"
    )

    return ac_time_dda_alert, ac_comp_dda_alert, total_timeliness, total_completeness


def build_colt_decisioned(colt_start: pd.DataFrame, dc: DateContext) -> pd.DataFrame:
    """
    Build colt_decisioned windowed on sasdt–sasdt2 and AlertDecision.
    """
    colt = adjust_transaction_date_timezone(colt_start)
    colt["transaction_date"] = colt["transactionTimestamp_c2"]
    colt["transaction_date"] = pd.to_datetime(colt["transaction_date"]).dt.date

    mask_window = (colt["transaction_date"] >= dc.sasdt) & (colt["transaction_date"] <= dc.sasdt2)
    mask_event = colt["eventactivityname_c"] == "AlertDecision"

    colt_decisioned = colt.loc[mask_window & mask_event].copy()
    return colt_decisioned


def build_alert_inbox_utc(
    alert_inbox: pd.DataFrame,
    colt_decisioned: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build alert_inbox_utc with ess_src_event_timestamp_a2 local date.
    """
    ai = alert_inbox.copy()
    cd = colt_decisioned[["decisionid", "processingCentre_c"]].drop_duplicates()

    ai = ai.merge(cd, on="decisionid", how="left")

    ai["ess_src_event_timestamp_a"] = pd.to_datetime(
        ai["ess_src_event_timestamp_a"], utc=True
    )

    pacific = "America/Vancouver"
    eastern = "America/Toronto"
    cond_b = ai["processingCentre_c"] == "B"

    ai_loc = ai.copy()
    ai_loc["ess_src_event_timestamp_a_local"] = np.where(
        cond_b,
        ai["ess_src_event_timestamp_a"].dt.tz_convert(pacific),
        ai["ess_src_event_timestamp_a"].dt.tz_convert(eastern),
    )
    ai_loc["ess_src_event_timestamp_a2"] = ai_loc[
        "ess_src_event_timestamp_a_local"
    ].dt.date

    return ai_loc


# --------------------------------------------------------------------------------------
# SECTION 10–11: DETAIL FAIL TABLES & TIMELINESS BUCKETS
# --------------------------------------------------------------------------------------

def build_accuracy_fail_detail(edw_txn: pd.DataFrame, dc: DateContext) -> pd.DataFrame:
    """
    Equivalent of data dda_alrt_ac_fail_wk_&ymd2.
    """
    df = edw_txn.copy()
    df = df[df["Comments"] == "Potential Fail"].copy()

    df["fail_reason"] = ""

    mask_dec = df["eventactivityname_c"] == "AlertDecision"
    cond_not_threshold = ~( (df["bal_yesterday"] > df["threshold_p"]) & (df["bal"] < df["threshold_p"]) )
    df.loc[mask_dec & cond_not_threshold, "fail_reason"] = "Threshold not breached"

    df.loc[
        mask_dec & (df["fail_reason"] == "") & (df["acct_status"] != "Valid Status   "),
        "fail_reason",
    ] = "Invalid status"
    df.loc[
        mask_dec & (df["fail_reason"] == "") & (df["Relationship_type"] != "Valid Relationship type"),
        "fail_reason",
    ] = "Invalid Relationship Type"
    df.loc[
        mask_dec & (df["fail_reason"] == "") & (df["deceased_status"] != "Valid - non-deceased"),
        "fail_reason",
    ] = "Deceased"
    df.loc[
        mask_dec & (df["fail_reason"] == "") & (df["closed_status"] != "Valid - non-closed"),
        "fail_reason",
    ] = "Closed account"
    df.loc[
        mask_dec & (df["fail_reason"] == "") & df["optoutdate_p"].notna(),
        "fail_reason",
    ] = "Opted out"
    df.loc[
        mask_dec & (df["fail_reason"] == "") & (df["accountcloseind_c"].fillna("") != ""),
        "fail_reason",
    ] = "Closing account (Hive)"

    df.loc[df["eventactivityname_c"] == "AlertSuppression", "fail_reason"] = "Required Alert"

    df["account_number"] = df["ar_id"]
    df["decision"] = df["eventactivityname_c"]
    df["balance_after_transaction"] = df["alertamount_c"]
    df["clientid_mask"] = (
        "******" + df["clientid_c"].astype(str).str[-3:].fillna("")
    )
    df["thresholdamount"] = df["thresholdamount_c"]
    df["product"] = df.get("producttype_p", df.get("ProductType", "Personal Deposit Accounts"))
    df["event_month"] = df["transaction_date"].apply(lambda d: d.strftime("%Y%m") if pd.notna(d) else "")
    df["reporting_date"] = pd.to_datetime(dc.date_com).date()
    df["event_week_ending"] = pd.to_datetime(dc.snap_dt_hive2).date()

    cols = [
        "event_month",
        "event_week_ending",
        "LOB",
        "product",
        "account_number",
        "account_status",
        "balance_after_transaction",
        "transaction_date",
        "decision",
        "clientid_mask",
        "thresholdamount",
        "decisionid",
        "reporting_date",
        "previousbalance",
        "fail_reason",
    ]

    # previousbalance from numeric column
    df["previousbalance"] = df["previousbalance"]
    return df[cols].copy()


def build_timeliness_fail_details(
    timeliness_sets: dict[str, pd.DataFrame],
    dc: DateContext,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build:
      - dda_alrt_time_fail_wk (timeliness fails)
      - dda_alrt_comp_fail_wk (completeness fails)
    """
    notime = timeliness_sets["inboth_did_not_meet_time"].copy()
    incolt_only = timeliness_sets["incolt_only"].copy()

    # Timeliness fails: inboth_did_not_meet_time + incolt_only
    notime["sent_timestamp"] = notime.get("eventtimestamp_a")
    notime["transactiontimestamp"] = notime.get("transactiontimestamp_c")
    incolt_only["sent_timestamp"] = np.nan
    incolt_only["transactiontimestamp"] = incolt_only.get("transactiontimestamp_c")

    time_fail = pd.concat([notime, incolt_only], ignore_index=True)

    time_fail["transaction_date"] = pd.to_datetime(
        time_fail["transactionTimestamp_c2"]
    ).dt.date
    time_fail["total_minutes"] = np.round(time_fail["timecheck"] / 60.0, 2)
    time_fail["balance_after_transaction"] = time_fail["alertamount_c"]

    acct_status = np.full(len(time_fail), "Other", dtype=object)
    acct_status[time_fail["accountstatus_c"] == "A"] = "Active"
    acct_status[time_fail["accountstatus_c"] == "N"] = "New"
    acct_status[time_fail["accountstatus_c"] == "D"] = "Dormant"
    time_fail["account_status"] = acct_status

    time_fail["clientid_mask"] = (
        "******" + time_fail["clientid_c"].astype(str).str[-3:].fillna("")
    )
    time_fail["product"] = time_fail.get("ProductType", "Personal Deposit Accounts")
    time_fail["thresholdamount"] = time_fail["thresholdamount_c"]
    time_fail["event_month"] = time_fail["transaction_date"].apply(
        lambda d: d.strftime("%Y%m") if pd.notna(d) else ""
    )
    time_fail["reporting_date"] = pd.to_datetime(dc.date_com).date()
    time_fail["event_week_ending"] = pd.to_datetime(dc.snap_dt_hive2).date()
    time_fail["account_number"] = time_fail["accountid"]

    cols_time = [
        "event_month",
        "event_week_ending",
        "LOB",
        "product",
        "account_number",
        "account_status",
        "balance_after_transaction",
        "transaction_date",
        "transactiontimestamp",
        "sent_timestamp",
        "total_minutes",
        "clientid_mask",
        "thresholdamount",
        "decisionid",
        "reporting_date",
    ]

    dda_alrt_time_fail_wk = time_fail[cols_time].copy()

    # Completeness fails: incolt_only only
    comp = incolt_only.copy()
    comp["transaction_date"] = pd.to_datetime(
        comp["transactionTimestamp_c2"]
    ).dt.date
    comp["balance_after_transaction"] = comp["alertamount_c"]
    acct_status2 = np.full(len(comp), "Other", dtype=object)
    acct_status2[comp["accountstatus_c"] == "A"] = "Active"
    acct_status2[comp["accountstatus_c"] == "N"] = "New"
    acct_status2[comp["accountstatus_c"] == "D"] = "Dormant"
    comp["account_status"] = acct_status2

    comp["clientid_mask"] = (
        "******" + comp["clientid_c"].astype(str).str[-3:].fillna("")
    )
    comp["product"] = comp.get("ProductType", "Personal Deposit Accounts")
    comp["thresholdamount"] = comp["thresholdamount_c"]
    comp["event_month"] = comp["transaction_date"].apply(
        lambda d: d.strftime("%Y%m") if pd.notna(d) else ""
    )
    comp["reporting_date"] = pd.to_datetime(dc.date_com).date()
    comp["event_week_ending"] = pd.to_datetime(dc.snap_dt_hive2).date()
    comp["account_number"] = comp["accountid"]

    cols_comp = [
        "event_month",
        "event_week_ending",
        "LOB",
        "product",
        "account_number",
        "account_status",
        "balance_after_transaction",
        "transaction_date",
        "clientid_mask",
        "thresholdamount",
        "decisionid",
        "reporting_date",
    ]

    dda_alrt_comp_fail_wk = comp[cols_comp].copy()
    return dda_alrt_time_fail_wk, dda_alrt_comp_fail_wk


def build_timeliness_buckets(total_timeliness: pd.DataFrame) -> pd.DataFrame:
    """
    Equivalent of totaltime + alert_time_dda.
    NOTE: buckets 03–14 are approximated as 'Other range' because detailed SAS ranges
    are not present in the snippet.
    """
    df = total_timeliness.copy()

    def bucket(sec: float | None) -> str:
        if pd.isna(sec):
            return "99 - Timestamp is missing"
        if sec > -1 and sec <= 1800:
            return "01 - Less than or equal to 30 minutes"
        if sec < 0:
            return "15 - Less than 0 seconds"
        if sec > 1800 and sec <= 3600:
            return "02 - Greater than 30 mins and less than or equal to 60 mins"
        if sec > 259200:
            return "15 - Greater than 3 days"
        return "03-14 - Other range"

    df["alert_time"] = df["timecheck"].apply(bucket)
    df["result"] = np.where(df["_src"] == "intime", "P", "F")

    alert_time_dda = (
        df.groupby("alert_time", as_index=False)["decisionid"]
        .nunique()
        .rename(columns={"decisionid": "decision_count"})
        .sort_values("alert_time")
    )
    return alert_time_dda


# --------------------------------------------------------------------------------------
# MAIN ORCHESTRATION
# --------------------------------------------------------------------------------------

def main(env: str = "PROD", regpath_override: str | None = None):
    paths = get_paths(env, regpath_override)
    logfile = init_logging_file(paths)

    ini_run = ini_check(paths)
    logger.info(f"ini_run = {ini_run}")

    dc = compute_dates(ini_run)
    logger.info(f"Date context: {dc}")

    conn_trino = get_trino_connection()
    conn_td = get_teradata_connection()

    # --- Hive→Trino pulls ---
    pref_init = fetch_pref_init(conn_trino)
    pref_new = fetch_pref_new(conn_trino)
    colt_start = fetch_colt_start(conn_trino, dc)
    alert_inbox = fetch_alert_inbox(conn_trino, dc)

    # --- Calendar from Teradata ---
    calendar = fetch_calendar(conn_td, dc)
    holiday = build_calendar_holiday(calendar)

    # --- Core COLT pipeline (Sections 5–6) ---
    colt4 = build_colt_pipeline(colt_start, holiday, dc)
    dda_colt = build_dda_colt_sample(colt4)

    # COLT branch for timeliness (colt_decisioned)
    colt_decisioned = build_colt_decisioned(colt_start, dc)

    # Alert_inbox with local date (alert_inbox_utc)
    alert_inbox_utc = build_alert_inbox_utc(alert_inbox, colt_decisioned)

    ar_ids, client_ids = build_in_lists_for_teradata(dda_colt)

    # --- Teradata snapshot population (Section 8) ---
    pers_pda_population = build_pers_pda_population(conn_td, dc, ar_ids, client_ids)

    # --- Preference joins (Section 6) ---
    pref = merge_pref(pref_init, pref_new)
    dda_colt_inbox = join_dda_colt_inbox(dda_colt, alert_inbox)
    pref_sum = compute_pref_summary(dda_colt_inbox, pref)
    dda_total_hive = build_dda_total_hive(dda_colt_inbox, pref, pref_sum)

    # --- Merge EDW & Hive (Section 9 initial) ---
    edw_hive_full_init = pers_pda_population.merge(
        dda_total_hive,
        on=["clnt_no", "ar_id"],
        how="inner",
        suffixes=("", "_hive"),
    )
    edw_hive_full_init = apply_pass_logic(edw_hive_full_init)

    # Weekend/new-account branch
    edw_hive_full = build_edw_hive_full_with_new_accounts(
        pers_pda_population,
        dda_total_hive,
        edw_hive_full_init,
    )

    # --- Timeliness / Completeness splits (Section 7) ---
    timeliness_sets = split_timeliness_sets(alert_inbox_utc, colt_decisioned, dc)

    # --- Timeliness & Completeness RDE aggregates ---
    (
        ac_time_dda_alert,
        ac_comp_dda_alert,
        total_timeliness,
        total_completeness,
    ) = build_timeliness_and_completeness_aggregates(
        timeliness_sets,
        dc,
    )

    # --- Accuracy transactional dataset & aggregate (Section 9) ---
    edw_hive_full_txn = build_edw_hive_full_txn(edw_hive_full, dc)
    ac_accu_dda_alert = build_accuracy_dataset(edw_hive_full_txn, dc)

    # --- Final weekly combined table (Section 10) ---
    dda_alert_ac_wk = pd.concat(
        [ac_accu_dda_alert, ac_time_dda_alert, ac_comp_dda_alert],
        ignore_index=True,
    )

    # Persist "current" version if ongoing run (ini_run = 'N')
    if ini_run == "N":
        dda_alert_ac_curr_path = paths.outpath / "dda_alert_ac_curr.parquet"
        dda_alert_ac_wk.to_parquet(dda_alert_ac_curr_path, index=False)
        logger.info(f"Updated current dda_alert_ac_curr at {dda_alert_ac_curr_path}")

    # Combined AC Excel (dda_alert_ac)
    out_alert_ac_path = paths.outpath / "dda_alert_ac.xlsx"
    with pd.ExcelWriter(out_alert_ac_path, engine="xlsxwriter") as writer:
        dda_alert_ac_wk.to_excel(writer, sheet_name="dda_alert_ac", index=False)
    logger.info(f"Wrote combined (accuracy + timeliness + completeness) to {out_alert_ac_path}")

    # --- Detail fail tables (Section 10) ---
    dda_alrt_ac_fail_wk = build_accuracy_fail_detail(edw_hive_full_txn, dc)
    dda_alrt_time_fail_wk, dda_alrt_comp_fail_wk = build_timeliness_fail_details(
        timeliness_sets, dc
    )

    # --- Timeliness buckets (Section 11) ---
    alert_time_dda = build_timeliness_buckets(total_timeliness)

    # --- Exports mirroring PROC EXPORT naming (Section 11) ---
    # 1) alert_time_dda
    with pd.ExcelWriter(paths.outpath / "alert_time_dda.xlsx", engine="xlsxwriter") as writer:
        alert_time_dda.to_excel(writer, sheet_name="alert_time_dda", index=False)

    # 2) Completeness detail
    with pd.ExcelWriter(
        paths.outpath / "Alert_DDA_Completeness_Detail.xlsx", engine="xlsxwriter"
    ) as writer:
        dda_alrt_comp_fail_wk.to_excel(writer, sheet_name="alert_comp_dda", index=False)

    # 3) Timeliness detail
    with pd.ExcelWriter(
        paths.outpath / "Alert_DDA_Timeliness_Detail.xlsx", engine="xlsxwriter"
    ) as writer:
        dda_alrt_time_fail_wk.to_excel(writer, sheet_name="alert_time_dda", index=False)

    # 4) Accuracy detail
    with pd.ExcelWriter(
        paths.outpath / "Alert_DDA_Accuracy_Detail.xlsx", engine="xlsxwriter"
    ) as writer:
        dda_alrt_ac_fail_wk.to_excel(writer, sheet_name="alert_accuracy_dda", index=False)

    logger.info("Wrote alert_time_dda and fail-detail Excel outputs.")

    logger.info("C86 DDA Alerts Python pipeline completed successfully.")


if __name__ == "__main__":
    env = os.environ.get("C86_ENV", "PROD")
    reg_override = os.environ.get("C86_REGPATH_OVERRIDE")
    main(env=env, regpath_override=reg_override)
