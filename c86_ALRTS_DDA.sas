/*-----------------------------------------------------------------------------------------
  SECTION 1: SETUP & CONNECTIONS
  (Images 1-2)
-----------------------------------------------------------------------------------------*/
/*sign-in script*/
/*kinit -f PRYU0SRVWIN@MAPLE.FG.RBC.COM -t PRYU0SRVWIN_Prod.kt*/
/*x 'cd; kinit -f PRYU0SRVWIN@MAPLE.FG.RBC.COM -t PRYU0SRVWIN_Prod.kt';*/

/*for prod*/
/*%let _server_ = uasasp11.fg.rbc.com 7551;*/
/*options comamid=tcp remote=_server_ fullstimer;*/

/*filename rlink "c:\Program Files\SASHome94\SASFoundation\9.4\connect\saslink\tcpunix.scr";*/
/*signon rlink;*/

/*libname rmtwork slibref=work server=_server_;*/

/*rsubmit;*/
x 'umask 000';
options nosymbolgen;
%global cdepath logpath outpath trackerpath logfile lstfile;
%let cdepath = %sysfunc(ifc("&sysparm" = "", /sas/broc/cde, &sysparm));
%include "&cdepath/common/security/pswd.sas";

options sasautos = (sasautos, "&cdepath/common/macros/volatile", "&cdepath/common/macros/general");
options pagesize=max;
%let syscc = 0; /*System macro variable, */

/*%macro ConnectSql;*/
/* %local init_mprint init_symbolgen ;*/
/*--- Save initial options ---*/
/* %let init_mprint = %sysfunc(getoption(mprint)) ;*/
/* %let init_symbolgen = %sysfunc(getoption(symbolgen)) ;*/
/* %let tdpid_option = %sysfunc(ifc(&SYSHOSTNAME=uasast11,%str(tdpid=sdf),));*/
/* options nomprint nosymbolgen;*/
/* connect to teradata (user=&user password=&pswd mode=teradata connection=global database=&user &tdpid_option) ;*/
/*--- Reset options ---*/
/* options &init_mprint &init_symbolgen ;*/
/*%mend ConnectSql;*/

x 'cd; kinit -f PRYU0SRVWIN@MAPLE.FG.RBC.COM -t PRYU0SRVWIN_Prod.kt';

/*endrsubmit;*/

/*rsubmit;*/

%global env regpath logpath outpath sasfile logfile lstfile;
%let env = PROD; /*Change to PROD when running production manually*/

%let env = %sysfunc(ifc("&env"="", %sysfunc(ifc(%upcase(%substr(%sysget(USER),1,1)) = U and &SYSHOSTNAME ^= uasast11, PROD, DEV)), &env));
%let regpath = %sysfunc(ifc("&sysparm" ^+ "", &sysparm, %sysfunc(ifc(&env=PROD, /sas/RSD/REG, /sas/RSD/REG_DEV))));

%include "&cdepath/common/security/pswd.sas";

options sasautos = (sasautos, "&regpath/script/common/macros/volatile", "&regpath/script/common/macros/general");
options pagesize=max;
%let syscc = 0; /*System macro variable, */
%let logpath = &regpath/C86/log/alert/dda;
%let outpath = &regpath/C86/output/alert/dda;

%let ymd = %sysfunc(today(), yymmddn8.);
%let logfile = &logpath/C86_Alert_DDA_&ymd..log;
%let lstfile = &logpath/C86_Alert_DDA_&ymd..lst;

%put &regpath &logfile &lstfile;
%CreateDirectory(&logpath);
proc printto log="&logfile" print="&lstfile" new;
run;

%GLOBAL _SASPROGRAMFILE;
%let sasfile = %sysfunc(ifc("&_SASPROGRAMFILE"="", %scan(&SYSPROCESSNAME,-1,%str( )), %scan(&_SASPROGRAMFILE,-1,%str(/))));
%put >>>>>>>>>> Name of This Program - &sasfile <<<<<<<<<<;
%put >>>>>>>>>> Starting Running Time - %left(%sysfunc(datetime(), datetime20.)) <<<<<<<<<<;
%put >>>>>>>>>> User ID - %sysget(USER) <<<<<<<<<<;
%put >>>>>>>>>> Platform - &SYSHOSTNAME <<<<<<<<<<;
%put >>>>>>>>>> Prod/Dev? - &env <<<<<<<<<<;

libname alrtdata "&regpath./C86/output/alert/dda/";

/*-----------------------------------------------------------------------------------------
  SECTION 2: DATE & MACRO SETUP
  (Images 3-4)
-----------------------------------------------------------------------------------------*/
%let ini_run = 'I';
%macro ini_check;
%if %sysfunc(fileexist(&outpath/dda_alert_ac_curr.sas7bdat)) %then %do;
    %let ini_run = 'N';
    x cp "&outpath/dda_alert_ac_curr.sas7bdat" "&outpath/dda_alert_ac_curr_backup.sas7bdat";
%end;
%else
    %let ini_run = 'Y';
%mend;
%ini_check;
%put &ini_run;

data _null_;
tday2=intnx('day',today(),-0);
tday=intnx('week.4',today(),-0);    /* Every Wednesday run */
launch_dt='30JUN2022'd;             /*need to change*/
ongoing_start = max(intnx('day',tday,-180), launch_dt);

week_end = tday - 2;                /* Ends on every monday */
if &ini_run = 'N' then 
week_start = week_end - 6;          /* Start on last Tuesday */
else week_start = launch_dt;

todayd = intnx('month',today(),0,'B')+14; /*Change 0 to -1 for last month*/
medate = intnx('month',todayd,-1,'E');
par_dt = week_start -7;

call symput('snap_dt_hive',"'"||put(week_start,YYMMDD10.)||"'");
call symput('snap_dt_hive2',"'"||put(week_end,YYMMDD10.)||"'");
call symput('today',"'"||put(tday,yymmdd10.)||"'");
call symput('sasdt', "'"||put(week_start,date.)||"'d");
call symput('sasdt2', "'"||put(week_end,date.)||"'d");
call symput ('me_dt',"'"||put(medate,yymmdd10.)||"'");
call symput ('ymd2',put(week_end, yymmddn8.));
call symput('date_com', "'"||put(tday2,YYMMDD10.)||"'");
call symput('pardt', "'"||put(par_dt,YYMMDD10.)||"'");

run;

%put &=snap_dt_hive. ;
%put &=snap_dt_hive2. ;
%put &=today. ;
%put &=sasdt. ;
%put &=sasdt2. ;
%put &=me_dt. ;
%put &=ymd2. ;
%put &=date_com. ;

/*-----------------------------------------------------------------------------------------
  SECTION 3: HIVE DATA EXTRACTION
  (Images 5-9, 15-17)
-----------------------------------------------------------------------------------------*/
proc sql noprint;
connect to hadoop as sql1 (
URI="jdbc:hive2://strplpaed12007.fg.rbc.com:2181,
              strplpaed12009.fg.rbc.com:2181,
              strplpaed12010.fg.rbc.com:2181,
              strplpaed12013.fg.rbc.com:2181/;serviceDiscoveryMode=zooKeeper;ssl=true;zooKeeperNamespace=hiveserver2"
schema=prod_brt0_ess
DBMAX_TEXT=256
LOGIN_TIMEOUT=600
);
execute(SET tez.queue.name=PRYU0) by sql1;
execute(SET hive.execution.engine=tez) by sql1;
execute(SET hive.compute.query.using.stats=true) by sql1;

/* Create Table PREF_INIT */
create table pref_init as 
select * from connection to sql1
(
select 
cast(regexp_replace(eventattributes ['ess_process_timestamp'] ,"T|Z"," ") as timestamp) as ess_process_timestamp_p,
cast(regexp_replace(eventattributes ['ess_src_event_timestamp'] ,"T|Z"," ") as timestamp) as ess_src_event_timestamp_p,
cast(regexp_replace(get_json_object(eventAttributes['SourceEventHeader'], '$.eventTimestamp'),"T|Z"," ") as timestamp) as eventTimestamp_p,

get_json_object(eventAttributes['eventPayload'], '$.preferenceType') as preferenceType_p,
get_json_object(eventAttributes['eventPayload'], '$.clientId') as clientId_p,
get_json_object(eventAttributes['eventPayload'], '$.sendAlertEligible') as sendAlertEligible_p,
get_json_object(eventAttributes['eventPayload'], '$.active') as active_p,
get_json_object(eventAttributes['eventPayload'], '$.threshold') as threshold_p,
get_json_object(eventAttributes['eventPayload'], '$.optOutDate') as optOutDate_p,
get_json_object(eventAttributes['eventPayload'], '$.account') as account,
get_json_object(eventAttributes['eventPayload'], '$.productType') as productType_p

from prod_brt0_ess.ffs0___client_alert_preferences_dep___initial_load
where
get_json_object(eventAttributes['eventPayload'], '$.preferenceType') = 'DDA_BALANCE_ALERT'
and partition_date = '20220324'
);
Disconnect from sql1;
quit;

/* Create Table PREF_NEW */
proc sql noprint;
connect to hadoop as sql1 (
/* ... connection string repeated as above ... */
);
/* ... execute SET commands repeated ... */

create table pref_new as
select * from connection to sql1
(
select
/* ... same columns as pref_init ... */
from prod_brt0_ess.ffs0___client_alert_preferences_dep
where
get_json_object(eventAttributes['eventPayload'], '$.preferenceType') = 'DDA_BALANCE_ALERT'
);
Disconnect from sql1;
quit;

/* Create Table COLT_START */
proc sql noprint;
connect to hadoop as sql1 ( ... );
/* ... set commands ... */
create table colt_start as
select * from connection to sql1
(
select
cast(regexp_replace(eventattributes ['ess_process_timestamp'] ,"T|Z"," ") as timestamp) as ess_process_timestamp_c,
cast(regexp_replace(eventattributes ['ess_src_event_timestamp'] ,"T|Z"," ") as timestamp) as ess_src_event_timestamp_c,
cast(regexp_replace(get_json_object(eventAttributes['SourceEventHeader'], '$.eventTimestamp'),"T|Z"," ") as timestamp) as eventTimestamp_c,
get_json_object(eventAttributes['SourceEventHeader'], '$.eventActivityName') as eventActivityName_c,

get_json_object(eventAttributes['eventPayload'], '$.alertType') as alertType_c,
get_json_object(eventAttributes['eventPayload'], '$.clientId') as clientId_c,
get_json_object(eventAttributes['eventPayload'], '$.thresholdAmount') as thresholdAmount_c,
cast(regexp_replace(get_json_object(eventAttributes['eventPayload'], '$.transactionTimestamp'),"T|Z"," ") as timestamp) as transactionTimestamp_c,
get_json_object(eventAttributes['eventPayload'], '$.alertAmount') as alertAmount_c,
get_json_object(eventAttributes['eventPayload'], '$.previousBalance') as previousBalance_c,
get_json_object(eventAttributes['eventPayload'], '$.accountStatus') as accountStatus_c,
get_json_object(eventAttributes['eventPayload'], '$.accountId') as accountId,
get_json_object(eventAttributes['eventPayload'], '$.processingCentre') as processingCentre_c,
get_json_object(eventAttributes['eventPayload'], '$.accountCloseInd') as accountCloseInd_c,
get_json_object(eventAttributes['eventPayload'], '$.decisionId') as decisionId,
get_json_object(eventAttributes['eventPayload'], '$.reasonCodes') as reasonCodes_c

from prod_brt0_ess.zgv0___colt_front_end_system
where
partition_date > &pardt. and
get_json_object(eventAttributes['eventPayload'], '$.alertType') = "DDA_BALANCE_ALERT" AND
get_json_object(eventAttributes['eventPayload'], '$.transactionTimestamp') between &snap_dt_hive. and &today.
);
Disconnect from sql1;
quit;

proc sql;
create table colt as
select 
    *,
    case when processingCentre_c = 'B' then datepart(tzoneu2s(transactionTimestamp_c,'America/Vancouver'))
         else datepart(tzoneu2s(transactionTimestamp_c,'America/Toronto')) end as transactionTimestamp_c2 format date9.
from colt_start;
quit;

/* Create Table ALERT_INBOX */
proc sql noprint;
connect to hadoop as sql1 ( ... );
/* ... set commands ... */
create table alert_inbox as 
select * from connection to sql1
(
select 
cast(regexp_replace(eventattributes ['ess_process_timestamp'] ,"T|Z"," ") as timestamp) as ess_process_timestamp_a,
cast(regexp_replace(eventattributes ['ess_src_event_timestamp'] ,"T|Z"," ") as timestamp) as ess_src_event_timestamp_a,
cast(regexp_replace(get_json_object(eventAttributes['SourceEventHeader'], '$.eventTimestamp'),"T|Z"," ") as timestamp) as eventTimestamp_a,

get_json_object(eventAttributes['eventPayload'], '$.alertType') as alertType_a,
get_json_object(eventAttributes['eventPayload'], '$.decisionId') as decisionId,
get_json_object(eventAttributes['eventPayload'], '$.accountId') as AccountId,
get_json_object(eventAttributes['eventPayload'], '$.alertAmount') as alertAmount_a,
get_json_object(eventAttributes['eventPayload'], '$.thresholdAmount') as thresholdAmount_a,
get_json_object(eventAttributes['eventPayload'], '$.alertSent') as alertSent_a,
get_json_object(eventAttributes['eventPayload'], '$.reasonCode') as reasonCode_a

from prod_brt0_ess.fft0___alert_inbox_dep
where
partition_date > &pardt. and
eventattributes ['ess_src_event_timestamp'] >= &snap_dt_hive.
and get_json_object(eventAttributes['eventPayload'], '$.alertType') = 'DDA_BALANCE_ALERT'
);
Disconnect from sql1;
quit;

/*-----------------------------------------------------------------------------------------
  SECTION 4: TERADATA PULLS & PREP
  (Images 10-11)
-----------------------------------------------------------------------------------------*/
data pref_new2 (drop=accountID);
set pref_new;
account=substr(accountId,6,7);
run;

data pref;
set pref_init
pref_new2;
run;

proc sql;
%ConnectSql
create table Calendar as select * from connection to teradata
(
select distinct
holidy_dt as DATE_T,
holidy_dt + 1 as DATE_T2,
Holidy_TYP,
subcntry_cd,
cntry_Cd,
snap_dt
from ddwv01.HOLIDY
where
SNAP_DT = &me_dt.
and (holidy_typ = 'F' or (holidy_typ = 'P' and subcntry_cd = 'ON') or (holidy_typ = 'P' and subcntry_cd = 'QC'))
and DATE_T between '2021-11-01' and date
order by 1
);
disconnect from teradata;
quit;

data calendar1 (rename=(date_t=transaction_date));
set Calendar (keep = DATE_T);
run;

data calendar2 (rename=(date_t2=transaction_date));
set Calendar (keep = DATE_T2);
run;

data holiday;
set calendar1
calendar2;
run;

proc sort data= holiday;
by transaction_date;
run;

/*-----------------------------------------------------------------------------------------
  SECTION 5: COLT PROCESSING & SAMPLING
  (Images 11-14)
-----------------------------------------------------------------------------------------*/
data colt2;
set colt;
transaction_date = transactiontimestamp_c2;
if transaction_date >= &sasdt. and transaction_date <= &sasdt2.;
run;

proc sort data= colt2;
by transaction_date;
run;

data colt3;
    merge colt2 (in=inbase)
    holiday (in=inholi);
    by transaction_date;
    if inbase;
    newdate=intnx('week.2',transaction_date,1);
    if weekday(transaction_date) = 2 then newdate = transaction_date;
run;

/*Sort by descending transaction time within the client, account and date*/
proc sort data = colt3 out=colt4;
by clientid_c accountid transaction_date descending transactiontimestamp_c;
run;

/*Only keep the final alert creating transaction of the day*/
proc sort data = colt4 nodupkey;
by clientid_c accountid transaction_date;
run;

/*Sort by transaction date for purpose of strata - need 10 per day*/
proc sort data = colt4 ;
by transaction_date;
run;

%macro sample_dda (indata, outdata);

/*Ensure sample size is set to correct value if there are less than 10 records */
proc sql noprint;
    select count(decisionid) into :size
    from &indata.
    ;
quit;
%let size=&size;
%put &size.;

%let samp_size=%sysfunc(min(&size.,10));
%put &samp_size.;

%if &samp_size. > 0 %then %do;

/*sample clients */
data version2;
    set &indata.;
    where transaction_date ne .;
run;

proc surveyselect data = version2
    out = &outdata.
    method=srs
    sampsize=&samp_size.
    strata transaction_date;
run;
%end;

/* if no samples exist, the matches and unused files still need to get created for the total population table */
%if &samp_size. <= 0 %then %do;
data &outdata.;
    set &indata.;
run;
%end;

%mend sample_dda;
%sample_dda (colt4, dda_colt);

data dda_colt;
    set dda_colt;
    format ar_id $20.;
    ar_id = '00000000' || accountid;
run;

/*Create listing of arrangements to be queried in EDW*/
proc sql;
    select "'" || ar_id || "'" into :dda_alert_list separated by ','
    from dda_colt
    ;
quit;
%put &=dda_alert_list;

/*Create listing of clients to be queried in EDW*/
proc sql;
    select clientId_c into :dda_alert_list_clnt separated by ','
    from dda_colt
    ;
quit;
%put &=dda_alert_list_clnt;

/*-----------------------------------------------------------------------------------------
  SECTION 6: MERGING WITH INBOX & PREFERENCES
  (Images 14-19)
-----------------------------------------------------------------------------------------*/
/* combine colt and inbox data */
proc sort data = dda_colt ;
    by decisionId;
run;
proc sort data = alert_inbox ;
    by decisionId;
run;

data dda_colt_inbox;
    merge dda_colt (in=inbase)
    alert_inbox;
    by decisionid;
    if inbase;
    account=substr(accountId,6,7);
run;

/*combine data with preference data (last record before transaction event)*/
proc sql;
create table pref_sum as
select
    colt.account,
    colt.clientid_c,
    colt.alertType_c,
    colt.transactionTimestamp_c,
    max(pref.ess_src_event_timestamp_p) as pref_time format dateampm22.2
from
    dda_colt_inbox as colt
    left join pref as pref
    on colt.account = pref.account
    and colt.clientid_c = pref.clientid_p
    and colt.alertType_c = pref.preferencetype_p
    and colt.transactionTimestamp_c >= pref.ess_src_event_timestamp_p
where alertType_c = 'DDA_BALANCE_ALERT'
group by 1,2,3,4;
quit;

proc sql;
create table dda_total_hive_1 as
select
    dda_colt_inbox.ess_process_timestamp_c,
    dda_colt_inbox.ess_src_event_timestamp_c,
    dda_colt_inbox.eventactivityname_c,
    dda_colt_inbox.alerttype_c,
    dda_colt_inbox.clientid_c,
    dda_colt_inbox.thresholdamount_c,
    dda_colt_inbox.transactiontimestamp_c,
    dda_colt_inbox.transactiontimestamp_c2,
    dda_colt_inbox.transaction_date,
    dda_colt_inbox.alertamount_c,
    dda_colt_inbox.previousbalance_c,
    dda_colt_inbox.accountstatus_c,
    dda_colt_inbox.accountid,
    dda_colt_inbox.processingcentre_c,
    dda_colt_inbox.accountcloseind_c,
    dda_colt_inbox.decisionid,
    dda_colt_inbox.reasoncodes_c,
    dda_colt_inbox.ar_id,
    dda_colt_inbox.ess_src_event_timestamp_a,
    dda_colt_inbox.eventtimestamp_a,
    dda_colt_inbox.alerttype_a,
    dda_colt_inbox.alertsent_a,
    dda_colt_inbox.reasoncode_a,
    prop_pref.ess_process_timestamp_p,
    prop_pref.ess_src_event_timestamp_p,
    prop_pref.eventtimestamp_p,
    prop_pref.preferencetype_p,
    prop_pref.clientid_p,
    prop_pref.sendalerteligible_p,
    prop_pref.active_p,
    prop_pref.threshold_p,
    prop_pref.optoutdate_p,
    prop_pref.producttype_p

from dda_colt_inbox as dda_colt_inbox
left join
(
    select
        pref_total.*,
        pref_sum.transactionTimestamp_c
    from
        pref as pref_total,
        pref_sum
    where pref_total.account = pref_sum.account
    and pref_total.clientid_p = pref_sum.clientid_c
    and pref_total.preferencetype_p = pref_sum.alertType_c
    and pref_total.ess_src_event_timestamp_p = pref_sum.pref_time
) as prop_pref

on dda_colt_inbox.account = prop_pref.account
and dda_colt_inbox.clientid_c = prop_pref.clientid_p
and dda_colt_inbox.alertType_c = prop_pref.preferencetype_p
and dda_colt_inbox.transactionTimestamp_c = prop_pref.transactionTimestamp_c
;
quit;

proc sort data = dda_total_hive_1 nodup;
by clientid_c ar_id;
run;

data dda_total_hive;
set dda_total_hive_1;
format clnt_no 9.;
clnt_no = clientid_c;
run;

/*timeliness and completeness logic*/
proc sort data = alert_inbox;
    by decisionid ess_process_timestamp_a;
run;

/*If a dup exists, take the one with the earlier processing timestamp*/
proc sort data = alert_inbox dupout=alert_dup nodupkey;
    by decisionid;
run;

/*timeliness and completeness logic*/
proc sort data = colt;
    by decisionid ess_process_timestamp_c;
run;

/*If a dup exists, take the one with the earlier processing timestamp*/
proc sort data = colt dupout=colt_dup nodupkey;
    by decisionid;
run;

data colt_decisioned;
    set colt;
    transaction_date = transactiontimestamp_c2;
    if transaction_date <= &sasdt2.;
    if transaction_date >= &sasdt.;
    if eventactivityname_c = 'AlertDecision';
run;

proc sort data = colt_decisioned;
    by decisionid;
run;

proc sql;
create table alert_inbox_utc as
select
    alert_inbox.*,
    case when colt_decisioned.processingCentre_c = 'B' then datepart(tzoneu2s(alert_inbox.ess_src_event_timestamp_a,'America/Vancouver'))
         else datepart(tzoneu2s(alert_inbox.ess_src_event_timestamp_a,'America/Toronto')) end as ess_src_event_timestamp_a2 format date9.
from
    alert_inbox as alert_inbox
    left join colt_decisioned as colt_decisioned
        on colt_decisioned.decisionid = alert_inbox.decisionid
;
quit;

/*-----------------------------------------------------------------------------------------
  SECTION 7: TIMELINESS & LOGIC
  (Images 20-24)
-----------------------------------------------------------------------------------------*/
data inboth_met_time
     inboth_did_not_meet_time
     incolt_only
     inbox_only
     transaction_utc_issue
     alert_inbox_buffer
     remaining_inbox;
merge alert_inbox_utc (in=inbox)
      colt_decisioned (in=incolt);
by decisionid;
RegulatoryName = 'C86';
LOB = 'Payments & Banking';
ReportName = 'C86 Alerts';
ProductType = 'Personal Deposit Accounts';
newdate=intnx('week.2',transaction_date,1);
if weekday(transaction_date) = 2 then newdate = transaction_date;
timecheck = INTCK("second", transactiontimestamp_c, ess_src_event_timestamp_a);
ess_src_event_a = datepart(ess_src_event_timestamp_a);
if inbox and incolt then do;
    if timecheck <= 1800 then output inboth_met_time;
    else output inboth_did_not_meet_time;
end;
if not inbox and incolt then output incolt_only;
if inbox and not incolt and ess_src_event_timestamp_a2 <= &sasdt2. and ess_src_event_timestamp_a2 > &sasdt. then output inbox_only;
else if inbox and not incolt and ess_src_event_timestamp_a2 <= &sasdt. then output transaction_utc_issue;
else if inbox and not incolt and ess_src_event_timestamp_a2 > &sasdt2. then output alert_inbox_buffer;
else if inbox and not incolt then output remaining_inbox;
run;

data alrtdata.inbox_only_&ymd2.;
set inbox_only;
run;

data total_timeliness;
    set inboth_met_time (in=intime)
        inboth_did_not_meet_time (in=notime)
        incolt_only (in=inmiss);
    ControlRisk = 'Timeliness';
    format transaction_date date9.;
    transaction_date = transactiontimestamp_c2;
    alertamount = input(alertamount_c, comma9.);
    thresholdamount = input(thresholdamount_c, comma9.);
    if intime then do;
        CommentCode = 'COM16';
        Comments = 'Pass';
    end;
    if notime then do;
        CommentCode = 'COM19';
        Comments = 'Potential Fail';
    end;
    if inmiss then do;
        CommentCode = 'COM19';
        Comments = 'Potential Fail';
    end;
run;

proc sql;
create table ac_time_dda_alert as 
select
RegulatoryName,
LOB,
ReportName,
ControlRisk,
'Anomaly' as TestType,
'Portfolio' as TestPeriod,
ProductType,
'Alert002_Timeliness_SLA' as RDE,
'' as SubDE,
'' as Segment,
/* ... Segments 2-9 empty ... */
put(transaction_date, yymmn6.) as segment10 length = 50,
'N' as HoldoutFlag,
CommentCode,
Comments,
count(accountId) as Volume,
sum(alertamount) as Bal,
sum(thresholdamount) as Amount,
input(&date_com., yymmdd10.) as DateCompleted format = yymmdd10.,
newdate as SnapDate format = yymmdd10.

from total_timeliness
where transaction_date ne .

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,26,27
order by 1,2,27
;
quit;

data total_completeness;
    set inboth_met_time (in=intime)
        inboth_did_not_meet_time (in=notime)
        incolt_only (in=inmiss);
    ControlRisk = 'Completeness';
    /* ... format and variable setup identical to total_timeliness ... */
    /* logic same as total_timeliness */
run;

proc sql;
create table ac_comp_dda_alert as
select
/* ... same columns as ac_time_dda_alert but RDE='Alert003_Completeness_All_Clients' ... */
from total_completeness
where transaction_date ne .
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,26,27
order by 1,2,27
;
quit;

/*-----------------------------------------------------------------------------------------
  SECTION 8: MACRO DDA_WEEK & TERADATA LOOPS
  (Images 25-29)
-----------------------------------------------------------------------------------------*/
options sasautos = ("&cdepath/common/macros/general");

%macro dda_week(n);

data _null_;
tday=intnx('week.4',today(),-0);    /* Every Wednesday run */
week_end = tday - 2;                /* Ends on every monday */
RELTN_DLY = intnx('day',week_end,-&n.,'B');
call symput ('snap_dt',"'"||put(RELTN_DLY,yymmdd10.)||"'");
run;
%put &=snap_dt.;

proc sql;
%connectsql
create table pers_pda_population_&n. as
select * from connection to teradata
(select CLNT_AR_RELTN_DLY.clnt_no,
        CLNT_AR_RELTN_DLY.PRMRY_CLNT_IND,
        CLNT_AR_RELTN_DLY.CLNT_AR_RELTN_TYP,  /*4-6*/
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
        DEPOSIT_ACCOUNT_DLY.DEP_ACCT_STS,      /*7-9*/
        DEPOSIT_ACCOUNT_DLY.MSG_CD1,      /*65 or 51*/
        DEPOSIT_ACCOUNT_DLY.MSG_CD2,
        DEPOSIT_ACCOUNT_DLY.MSG_CD3,
        DEPOSIT_ACCOUNT_DLY.MSG_CD4,
        DEPOSIT_ACCOUNT_DLY.MSG_CD5,
        AR_BAL_DLY.BAL,
        AR_BAL_DLY2.BAL       as bal_yesterday,

        case when DEPOSIT_ACCOUNT_DLY.DEP_ACCT_STS between 0 and 6 then 'Valid Status   '
             else 'Invalid Status'
             end as acct_status,

        case when CLNT_AR_RELTN_DLY.CLNT_AR_RELTN_TYP in (4,5,6) then 'Invalid Relationship type'
             else 'Valid Relationship type'
             end as Relationship_type,

        case when DEPOSIT_ACCOUNT_DLY.MSG_CD1 = 51 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD2 = 51 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD3 = 51 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD4 = 51 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD5 = 51 then 'Invalid - deceased restraint'
             else 'Valid - non-deceased'
             end as deceased_status,

        case when DEPOSIT_ACCOUNT_DLY.MSG_CD1 = 65 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD2 = 65 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD3 = 65 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD4 = 65 or
                  DEPOSIT_ACCOUNT_DLY.MSG_CD5 = 65 then 'Invalid - Closed restraint'
             else 'Valid - non-closed'
             end as closed_status

from DDWV01.CLNT_AR_RELTN_DLY      as CLNT_AR_RELTN_DLY
     inner join DDWV01.AR_STATIC_DLY    as AR_STATIC_DLY
     on CLNT_AR_RELTN_DLY.ar_id        = AR_STATIC_DLY.ar_id
     and CLNT_AR_RELTN_DLY.snap_dt     = &snap_dt.
     and CLNT_AR_RELTN_DLY.snap_dt     = AR_STATIC_DLY.snap_dt
     left join DDWV01.DEPOSIT_ACCOUNT_DLY as DEPOSIT_ACCOUNT_DLY
     on CLNT_AR_RELTN_DLY.ar_id        = DEPOSIT_ACCOUNT_DLY.ar_id
     and DEPOSIT_ACCOUNT_DLY.snap_dt   = &snap_dt.
     left join DDWV01.AR_BAL_DLY   as AR_BAL_DLY /*To get daily end of day balance*/
     on CLNT_AR_RELTN_DLY.ar_id        = AR_BAL_DLY.ar_id
     and AR_BAL_DLY.snap_dt            = &snap_dt.
     left join
     (select
             AR_BAL_DLY_pre1.ar_id,
             AR_BAL_DLY_pre1.bal
      from
             DDWV01.AR_BAL_DLY as AR_BAL_DLY_pre1
             inner join
                 (select
                         AR_BAL_DLY_inner.ar_id,
                         max(AR_BAL_DLY_inner.snap_dt) as max_snap
                  from
                         DDWV01.AR_BAL_DLY as AR_BAL_DLY_inner
                  where
                         AR_BAL_DLY_inner.snap_dt < &snap_dt.
                         and AR_BAL_DLY_inner.ar_id in (&dda_alert_list.)
                  group by 1) as AR_BAL_DLY_pre2
             on AR_BAL_DLY_pre1.ar_id = AR_BAL_DLY_pre2.ar_id
             and AR_BAL_DLY_pre1.snap_dt = AR_BAL_DLY_pre2.max_snap) as AR_BAL_DLY2
     on CLNT_AR_RELTN_DLY.ar_id       = AR_BAL_DLY2.ar_id

where
CLNT_AR_RELTN_DLY.ar_id in (&dda_alert_list.)
and CLNT_AR_RELTN_DLY.clnt_no in (&dda_alert_list_clnt.)
);
disconnect from teradata;
quit;
%mend dda_week;

%dda_week(0);
%dda_week(1);
/* ... repeats for 2 through 13 ... */
%dda_week(13);

data pers_pda_population;
    set
    pers_pda_population_0
    /* ... repeats for 1-12 ... */
    pers_pda_population_13
    ;
    if bal_yesterday = . then bal_yesterday = 0;
run;

/*-----------------------------------------------------------------------------------------
  SECTION 9: LOGIC & FLAGS
  (Images 30-35)
-----------------------------------------------------------------------------------------*/
proc sort data=pers_pda_population nodupkey;
    by clnt_no ar_id snap_dt;
run;

proc sort data=dda_total_hive nodupkey;
    by clnt_no ar_id;
run;

data edw_hive_full_init;
    merge pers_pda_population (in=inedw)
    dda_total_hive (in=inhive);
    by clnt_no ar_id;
    if inedw and inhive;
    if snap_dt = transaction_date then correct_date = 'Y';
    else correct_date = 'N';
    Pass_flag = 'N';
    if correct_date = 'N' then Pass_flag = 'X';
    if threshold_p = . then threshold_p = 100;
    if eventActivityName_c = "AlertDecision" then do;
        if (acct_status = 'Valid Status   ' and Relationship_type = 'Valid Relationship type' and deceased_status = 'Valid - non-deceased'
        and closed_status = 'Valid - non-closed' and bal_yesterday > threshold_p and BAL < threshold_p
        and optoutdate_p = . and accountcloseind_c in ('','false')) then pass_flag = 'Y';
    end;
    if eventActivityName_c = "AlertSuppression" then do;
        if (acct_status ne 'Valid Status   ' or Relationship_type ne 'Valid Relationship type' or deceased_status ne 'Valid - non-deceased'
        or closed_status ne 'Valid - non-closed' or bal_yesterday <= threshold_p
        or optoutdate_p ne . or accountcloseind_c = 'true') then pass_flag = 'Y';
    end;
    if correct_date = 'Y';
    if DEP_ACCT_STS = 0 then account_status = 'Active';
    else if DEP_ACCT_STS = 2 then account_status = 'Dormant';
    else if DEP_ACCT_STS in (3,4,5) then account_status = 'Opened Today';
    else if DEP_ACCT_STS = 6 then account_status = 'Conversion Pending';
    else if DEP_ACCT_STS in (7,8,9) then account_status = 'Closed';
    else if DEP_ACCT_STS = 10 then account_status = 'OATS transferred 1st day non financial';
    else if DEP_ACCT_STS = 11 then account_status = 'OATS transferred end of 1st day after financials are processed';
    else if DEP_ACCT_STS = 12 then account_status = 'OATS transfer complete';
    else if DEP_ACCT_STS = 13 then account_status = 'OATS transfer non zero balance';
    else account_status = 'Unknown';

    RegulatoryName = 'C86';
    LOB = 'Payments & Banking';
    ReportName = 'C86 Alerts';
    ProductType = 'Personal Deposit Accounts';
run;

/*Found that there are cases that open an account on the weekend and trigger an alert.*/
/*These cases don't have their edw data until the next business day, so needed*/
/*to build the logic below to capture the first instance in edw for those arrangements*/

data new_acct;
    merge edw_hive_full_init (in=inedw)
    dda_total_hive (in=inhive);
    by clnt_no ar_id;
    if not inedw and inhive;
run;

data edw_hive_full_new_acct;
    merge pers_pda_population (in=inedw)
    new_acct (in=inhive);
    by clnt_no ar_id;
    if inedw and inhive;
    if snap_dt = transaction_date then correct_date = 'Y';
    else correct_date = 'N';
    /* ... same Pass_flag logic as above ... */
    /*if correct_date = 'Y';*/
    /* ... same account_status logic as above ... */
run;

proc sql;
create table edw_hive_full_new_acct2 as 
select edw_hive_full_new_acct.*
from 
    edw_hive_full_new_acct as edw_hive_full_new_acct,
    (select
        ar_id,
        clnt_no,
        min(snap_dt) as snap_dt
     from edw_hive_full_new_acct
     group by 1,2) as oldest_snap
where
    oldest_snap.snap_dt = edw_hive_full_new_acct.snap_dt and
    oldest_snap.ar_id = edw_hive_full_new_acct.ar_id and
    oldest_snap.clnt_no = edw_hive_full_new_acct.clnt_no
;
quit;

data edw_hive_full;
    set edw_hive_full_init
    edw_hive_full_new_acct2;
run;

data edw_hive_full_txn;
    set edw_hive_full (in=inbase);
    if inbase;
    ControlRisk = 'Accuracy';
    alertamount = input(alertamount_c, comma9.);
    thresholdamount = input(thresholdamount_c, comma9.);
    previousbalance = input(previousbalance_c, comma9.);
    format transaction_date date9.;
    newdate=intnx('week.2',transaction_date,1);
    if weekday(transaction_date) = 2 then newdate = transaction_date;
    if eventActivityName_c = "AlertDecision" then do;
        if (pass_flag = 'Y') and (previousbalance >= thresholdamount) and (thresholdamount > alertamount)
        then do;
            CommentCode = 'COM16';
            Comments = 'Pass';
        end;
        else do;
            CommentCode = 'COM19';
            Comments = 'Potential Fail';
        end;
    end;
    else if eventactivityname_c = 'AlertSuppression' then do;
        if (pass_flag = 'Y')
        then do;
            CommentCode = 'COM16';
            Comments = 'Pass';
        end;
        else if (pass_flag = 'N') and (previousbalance < thresholdamount)
        then do;
            CommentCode = 'COM16';
            Comments = 'Pass';
        end;
        else if (pass_flag = 'N') and (previousbalance > thresholdamount) and (thresholdamount > alertamount)
        then do;
            CommentCode = 'COM16';
            Comments = 'Pass';
        end;
        else do;
            CommentCode = 'COM19';
            Comments = 'Potential Fail';
        end;
    end;
run;

/*save samples in perm dataset*/
data alrtdata.DDA_Alert_Sample_Full_&ymd2.;
set edw_hive_full_txn;
run;

proc sql;
create table alrtdata.DDA_Alert_Sample_&ymd2. as 
select
AR_ID,
CLNT_NO,
CLNT_AR_RELTN_TYP,
DEP_ACCT_STS,
MSG_CD1,
MSG_CD2,
MSG_CD3,
MSG_CD4,
MSG_CD5,
acct_status,
Relationship_type,
deceased_status,
closed_status,
accountcloseind_c,
decisionid,
eventactivityname_c,
alerttype_c,
transaction_date,
threshold_p,
optoutdate_p,
alertamount,
previousbalance,
Comments
from edw_hive_full_txn
;
run;

proc sql;
create table ac_accu_dda_alert as
select
RegulatoryName,
LOB,
ReportName,
ControlRisk,
'Sample' as TestType,
'Portfolio' as TestPeriod,
'Personal Deposit Accounts' as ProductType,
'Alert001_Accuracy_Balance' as RDE,
/* ... empty segments ... */
put(transaction_date, yymmn6.) as segment10 length = 50,
'N' as HoldoutFlag,
CommentCode,
Comments,
count(accountId) as Volume,
sum(alertamount) as Bal,
sum(thresholdamount) as Amount,
input(&date_com., yymmdd10.) as DateCompleted format = yymmdd10.,
newdate as SnapDate format = yymmdd10.

from edw_hive_full_txn
where transaction_date ne .
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,26,27
order by 1,2,27
;
quit;

/*-----------------------------------------------------------------------------------------
  SECTION 10: FINAL REPORTING DATASETS
  (Images 38-40)
-----------------------------------------------------------------------------------------*/
data alrtdata.dda_alrt_ac_fail_wk_&ymd2. (keep= event_month event_week_ending lob product account_number account_status balance_after_transaction transaction_date decision clientid_mask thresholdamount decisionid reporting_date previousbalance);
set edw_hive_full_txn;
if Comments = 'Potential Fail';
eod_bal = bal;
format transaction_date yymmdd10. fail_reason $30. reporting_date yymmdd10. event_week_ending yymmdd10. account_status $10.;
if eventActivityName_c = "AlertDecision" then do;
    if not (bal_yesterday > threshold_p and BAL < threshold_p) then fail_reason = 'Threshold not breached';
    else if acct_status ne 'Valid Status   ' then fail_reason = 'Invalid status';
    else if Relationship_type ne 'Valid Relationship type' then fail_reason = 'Invalid Relationship Type';
    else if deceased_status ne 'Valid - non-deceased' then fail_reason = 'Deceased';
    else if closed_status ne 'Valid - non-closed' then fail_reason = 'Closed account';
    else if optoutdate_p ne . then fail_reason = 'Opted out';
    else if accountcloseind_c ne '' then fail_reason = 'Closing account (Hive)';
end;
if eventActivityName_c = "AlertSuppression" then fail_reason = 'Required Alert';
account_number = ar_id;
date_of_decision = transaction_date;
decision = eventactivityname_c;
balance_after_transaction = alertamount_c;
clientid_mask = '******' || substr(clientid_c,7,3);
clientid = clientid_c;
thresholdamount = thresholdamount_c;
product = producttype;
/*... account_status assignment logic repeated ...*/
event_month = put(transaction_date, yymmn6.);
reporting_date = input(&date_com., yymmdd10.);
event_week_ending = input(&snap_dt_hive2., yymmdd10.);
run;

data alrtdata.dda_alrt_time_fail_wk_&ymd2.;
set inboth_did_not_meet_time (rename = (eventTimestamp_a=sent_timestamp transactiontimestamp_c=transactiontimestamp))
    incolt_only (rename = (transactiontimestamp_c=transactiontimestamp));
format transaction_date yymmdd10. reporting_date yymmdd10. event_week_ending yymmdd10. account_status $10.;
transaction_date = transactiontimestamp_c2;
date_of_alert = transaction_date;
account_number = accountid;
total_minutes = round(timecheck/60,2);
balance_after_transaction = alertamount_c;
/*... account_status assignment logic repeated ...*/
clientid_mask = '******' || substr(clientid_c,7,3);
/*... variable assignments ...*/
event_month = put(transaction_date, yymmn6.);
reporting_date = input(&date_com., yymmdd10.);
event_week_ending = input(&snap_dt_hive2., yymmdd10.);
label sent_timestamp = 'sent_timestamp';
label transactiontimestamp = 'transactiontimestamp';
run;

data alrtdata.dda_alrt_comp_fail_wk_&ymd2.;
set incolt_only;
/*... similar formatting logic as time_fail_wk ...*/
run;

data alrtdata.dda_alert_ac_wk_&ymd2.;
    format rde $46. producttype $25. testtype $14. controlrisk $12. segment6 $50.;
    set ac_accu_dda_alert
    ac_time_dda_alert
    ac_comp_dda_alert
    ;
run;

%macro ac;
%if &ini_run = 'N' %then %do;
data alrtdata.dda_alert_ac_curr;
set alrtdata.dda_alert_ac_wk_&ymd2.;
run;
%end;
%mend ac;
%ac;

/*-----------------------------------------------------------------------------------------
  SECTION 11: TIMELINESS DETAIL & EXPORTS
  (Images 41-46)
-----------------------------------------------------------------------------------------*/
data totaltime;
set Inboth_did_not_meet_time (in=notime)
    Inboth_met_time (in=intime)
    incolt_only;
format alert_time $70.;
if timecheck >-1 and timecheck <= 1800 then alert_time = '01 - Less than or equal to 30 minutes';
else if timecheck < 0 then alert_time = '15 - Less than 0 seconds';
else if timecheck > 1800 and timecheck <= 3600 then alert_time = '02 - Greater than 30 mins and less than or equal to 60 mins';
/*... categories 03 through 14 ...*/
else if timecheck > 259200 then alert_time = '15 - Greater than 3 days';
else if timecheck = . then alert_time = '99 - Timestamp is missing';
if intime then result = 'P';
else result = 'F';
run;

proc sql;
create table alrtdata.alert_time_dda as
select
    alert_time,
    count(distinct decisionid) as decision_count
from totaltime
group by 1
order by 1;
quit;

proc export data=alrtdata.alert_time_dda
outfile="&outpath/alert_time_dda"
dbms=xlsx replace;
sheet="alert_time_dda";
run;

/*... multiple proc export calls follow for comp_fail_wk, ac_fail_wk, etc. ...*/

proc export data=alrtdata.dda_alert_ac_curr
outfile="&outpath/dda_alert_ac"
dbms=xlsx replace;
sheet="dda_alert_ac";
run;

x "find &regpath. -user $USER -mmin -720 -exec chmod 777 {} \;";

proc printto; run;
%ScanLog(&logfile);

/*endrsubmit;*/

/*-----------------------------------------------------------------------------------------
  SECTION 12: SAMPLING FOR MANUAL REVIEW
-----------------------------------------------------------------------------------------*/
proc surveyselect data = dda_alrt_time_fail_cur
    out = dda_alert_timeliness_fails_samp
    method=srs
    sampsize=25;
run;

proc surveyselect data = dda_alrt_comp_fail_cur
    out = dda_alert_com_fails_samp
    method=srs
    sampsize=25;
run;

/*... surveyselects for total_timeliness, total_completeness, edw_hive_full_txn (accuracy) ...*/
/*... surveyselects for transaction_utc_issue, inboth_did_not_meet_time, incolt_only, inbox_only ...*/

%macro commexp(ds,fl);
proc export data=&ds
outfile="&fl"
replace;
run;
%mend;

/*%commexp(rmtwork.colt, colt.xlsx);*/
/*... multiple %commexp calls ...*/

/*-----------------------------------------------------------------------------------------
  SECTION 13: UAT & CHECKS
  (Images 47-60)
-----------------------------------------------------------------------------------------*/
proc sql;
create table check as 
select 
    alert_status,
    Pass_flag,
    Comments,
    count(distinct ar_id) as ar_id_count
from edw_hive_full_txn
where eventactivityname_c = 'AlertDecision'
group by 1,2,3
order by 1,2,3
;
quit;

proc sql;
create table check_sup as
select
    alert_status,
    Pass_flag,
    Comments,
    count(distinct ar_id) as ar_id_count
from edw_hive_full_txn
where eventactivityname_c = 'AlertSuppression'
group by 1,2,3
order by 1,2,3
;
quit;

/*Additional check tables (alert_time_check2, colt_uat, etc.)*/
proc sql;
create table alert_time_check2 as
select
    transaction_date format date9.,
    alert_time,
    count(distinct decisionid) as decisionid_count
from totaltime
group by 1,2
order by 1,2;
quit;

data alert_inbox_for_ac;
set alert_inbox;
ControlRisk = 'Timeliness';
format ess_src_event_date date9. segment yymm.;
ess_src_event_date = datepart(ess_src_event_timestamp_a);
/*... formatting ...*/
if alertsent_a = 'true' then do;
    CommentCode = 'COM25';
    Comments = 'Alert Sent';
end;
else if alertsent_a = 'false' then do;
    CommentCode = 'COM26';
    Comments = 'Alert Suppressed';
end;
run;

proc sql;
create table ac_sent_dda_alert as
select
/*... columns similar to ac_accu but for sent alerts ...*/
from alert_inbox_for_ac
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,26,27
order by 1,2,27
;
quit;

/* UAT Logic */
data colt_uat;
set colt2;
format transaction_date date9.;
run;

data edw_hive_full_txn_uat;
set edw_hive_full_txn;
case_type = trim(eventactivityname_c) || '-' || trim(Comments);
if alertamount <= thresholdamount then bal_below_threshold = 'Y';
else bal_below_threshold = 'N';
if previousBalance_c ne . then new_account = 'N';
else new_account = 'Y';
run;

/*Proc Freq checks*/
proc freq data = edw_hive_full_txn;
tables DEP_ACCT_STS acct_status;
run;
/*... multiple proc freq tables ...*/

proc sql;
create table colt_decisionid_count as
select decisionid, count(decisionid) as decisionid_count
from colt_decisioned
group by 1 order by 2;
quit;
