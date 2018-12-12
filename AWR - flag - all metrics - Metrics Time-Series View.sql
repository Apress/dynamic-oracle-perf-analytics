-- AWR - flag - all metrics - Metrics Time-Series View.sql
-- 21-Sep-2018 RDCornejo
-- DOPA Process -- Metrics Time-Series View
-- -------------------------------------------------------------------------------------------
-- This work is offered by the author as a professional curtesy, “as-is” and without warranty.  
-- The author disclaims any liability for damages that may result from using this code.
-- -------------------------------------------------------------------------------------------
with taxonomy as
(
select taxonomy_type, stat_source, metric_name, category,  sub_category 
from metric_Taxonomy
where upper(taxonomy_type) like upper(nvl(:taxonomy_type, 'Infrastructure'))
  and category like nvl(:category, category)
  and sub_category like nvl(:sub_category, sub_category)
)
-- select * from taxonomy;  -- testing thus far
, snaps_to_use as
(
select distinct snap_id , begin_interval_time, end_interval_time
from dba_hist_snapshot 
where 1=1
  and decode(:stats_days_back_only_Y_N,'Y', begin_interval_time, trunc(sysdate-:stats_days_back) ) >= trunc(sysdate-:stats_days_back)
    and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:allint_st_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:allint_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )
)
-- select * from snaps_to_use order by snap_id;  -- testing thus far
, snaps_for_interval as
(
select distinct snap_id , begin_interval_time, end_interval_time
from dba_hist_snapshot 
where 1=1
  and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:intrvl_st_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:intrvl_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )
/*  and    (to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour, 0) and  nvl(:stats_end_hour, 24) 
       or to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour2, nvl(:stats_begin_hour, 0)) and  nvl(:stats_end_hour2, nvl(:stats_end_hour, 24))) */
)
-- select * from snaps_for_interval order by snap_id; -- testing thus far
, snaps_for_normal_ranges as
(
select distinct snap_id , begin_interval_time, end_interval_time
from dba_hist_snapshot 
where 1=1
  and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:normRng_st_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:normRng_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )
/*  and    (to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour, 0) and  nvl(:stats_end_hour, 24) 
       or to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour2, nvl(:stats_begin_hour, 0)) and  nvl(:stats_end_hour2, nvl(:stats_end_hour, 24))) */
)
-- select * from snaps_for_normal_ranges order by snap_id;  -- testing thus far
, snaps_to_use_for_deltas as
(
select snap_id from snaps_to_use union select min(snap_id)-1 from snaps_to_use
)
-- select snap_id from snaps_to_use_for_deltas order by snap_id;  -- testing thus far
, snaps as
(
select count(distinct snap_id) intervals 
from snaps_for_interval 
)
-- select intervals from snaps;  -- testing thus far
, latch as
(
select * from dba_hist_latch a where snap_id in (select snap_id from snaps_to_use_for_deltas) 
)
-- select * from latch where latch_name like 'cache %' order by latch_name, 1;  -- testing thus far
, unpivot_latch as
(
      select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' gets' metric_name, gets cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' misses' metric_name,  misses cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleeps' metric_name,  sleeps cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' immediate_gets' metric_name, immediate_gets cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' immediate_misses' metric_name,  immediate_misses cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' spin_gets' metric_name, spin_gets cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep1' metric_name,  sleep1 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep2' metric_name,  sleep2 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep3' metric_name,  sleep3 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep4' metric_name,  sleep4 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' wait_time' metric_name,  wait_time cumulative_value , a.dbid, a.instance_number from latch a
)
-- select * from unpivot_latch where metric_name like '%cache buffer%' order by 1,2; -- testing thus far
, system_event as
(
select * from dba_hist_system_event a where snap_id in (select snap_id from snaps_to_use_for_deltas) 
)
-- select * from system_event ;  -- testing thus far
, unpivot_system_event as
(
      select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_waits' metric_name, total_waits cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_timeouts' metric_name,  total_timeouts cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' time_waited_micro' metric_name,  time_waited_micro cumulative_value , a.dbid, a.instance_number from system_event a
-- comment out since _fg versions of the metric have the same values as the non-_fg version
--union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_waits_fg' metric_name,  total_waits_fg cumulative_value , a.dbid, a.instance_number from system_event a
--union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_timeouts_fg' metric_name,  total_timeouts_fg cumulative_value , a.dbid, a.instance_number from system_event a
--union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' time_waited_micro_fg' metric_name,  time_waited_micro_fg cumulative_value , a.dbid, a.instance_number from system_event a
)
, iostat_function as
(
select * from dba_hist_iostat_function a where snap_id in (select snap_id from snaps_to_use_for_deltas) 
)
-- select * from iostat_function where function_name = 'Direct Reads' order by snap_id;
, unpivot_iostat_function as
(
      select a.snap_id, a.function_id metric_id, function_name || ' small_read_megabytes' metric_name, small_read_megabytes cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' small_write_megabytes' metric_name,  small_write_megabytes cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_read_megabytes' metric_name, large_read_megabytes  cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_write_megabytes' metric_name, large_write_megabytes cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' small_read_reqs' metric_name, small_read_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' small_write_reqs' metric_name, small_write_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_read_reqs' metric_name, large_read_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_write_reqs' metric_name, large_write_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' number_of_waits' metric_name, number_of_waits cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' wait_time' metric_name, wait_time cumulative_value , a.dbid, a.instance_number from iostat_function a
)
--select * from unpivot_iostat_function where metric_name like 'Direct Reads%' order by snap_id;
, stat as
(
select /*+ MATERIALIZE */ instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, a.stat_id metric_id
, stat_name metric_name
,   nvl(decode(greatest(value, nvl(lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),0)),
    value,
    value - lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.stat_name) min_snap_id
, 'dba_hist_sys_time_model' stat_source
from dba_hist_sys_time_model a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_sys_time_model, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas) 
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, a.stat_id metric_id
, stat_name metric_name
,   nvl(decode(greatest(value, nvl(lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),0)),
    value,
    value - lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.stat_name) min_snap_id
, 'dba_hist_sysstat' stat_source
from dba_hist_sysstat a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_sysstat, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, a.stat_id metric_id
, stat_name metric_name
,   nvl(decode(greatest(value, nvl(lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),0)),
    value,
    value - lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.stat_name) min_snap_id
, 'dba_hist_osstat' stat_source
from dba_hist_osstat a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_osstat, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
  and stat_name not in ('x'
,'NUM_CPUS'
,'NUM_CPU_CORES'
,'NUM_CPU_SOCKETS'
,'PHYSICAL_MEMORY_BYTES'
,'TCP_SEND_SIZE_DEFAULT'
,'TCP_SEND_SIZE_MAX'
,'TCP_RECEIVE_SIZE_DEFAULT'
,'TCP_RECEIVE_SIZE_MAX')
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
,   nvl(decode(greatest(cumulative_value, nvl(lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),0)),
    cumulative_value,
    cumulative_value - lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),cumulative_value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_iostat_function' stat_source
from unpivot_iostat_function a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_iostat_function, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
, round(average) average
, a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_sysmetric_summary' stat_source
from dba_hist_sysmetric_summary a 
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_sysmetric_summary, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
,   nvl(decode(greatest(cumulative_value, nvl(lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),0)),
    cumulative_value,
    cumulative_value - lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),cumulative_value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_system_event' stat_source
from unpivot_system_event a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_system_event, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union 
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
,   nvl(decode(greatest(cumulative_value, nvl(lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),0)),
    cumulative_value,
    cumulative_value - lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),cumulative_value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_latch' stat_source
from unpivot_latch a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_latch, 'N') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
)
--select distinct stat_source from stat;
--select * from stat  /* where snap_id = (select min(snap_id) from stat)+2 and (metric_name like '%cache buffer chains%' or metric_name like '%TCP Socket (KGAS)%' or metric_name like '%Direct Reads%') */ order by snap_id, stat_source, metric_name; -- testing SQL up tp this point
, outliers as
( select iqr.*
, case when Q1 - (nvl(:iqr_factor, 1.5) * IQR) > 0 then Q1 - (nvl(:iqr_factor, 1.5) * IQR) else 0 end as lower_outlier
, Q3 + (nvl(:iqr_factor, 1.5) * IQR) as upper_outlier
from (select stat.*
, Percentile_Cont(nvl(:Q1_PERCENTILE,0.25)) WITHIN GROUP (Order By average) OVER(partition by stat_source, metric_name) As Q1
, Percentile_Cont(nvl(:Q3_PERCENTILE,0.75)) WITHIN GROUP(Order By average) OVER(partition by stat_source, metric_name) As Q3
, Percentile_Cont(nvl(:Q3_PERCENTILE,0.75)) WITHIN GROUP(Order By average) OVER(partition by stat_source, metric_name) 
- Percentile_Cont(nvl(:Q1_PERCENTILE,0.25)) WITHIN GROUP(Order By average) OVER(partition by stat_source, metric_name) as IQR
from stat
where 1=1
  and snap_id in (select snap_id from snaps_to_use)
  and stat_source like nvl(:stat_source, stat_source)
) iqr
)
-- select * from outliers order by snap_id, stat_source, metric_name;
, normal_ranges as
(
select metric_id
, metric_name
, stat_source
, round(case when (AVG_average - (2 * STDDEV_average )) < 0 then min_average
       else (AVG_average - (2 * STDDEV_average))
  end) as lower_bound
, avg_average average_value
, round(case when (AVG_average + (2 * STDDEV_average )) >= max_average then max_average
       else AVG_average + (2 * STDDEV_average) 
  end) as upper_bound
, variance_average
, stddev_average
from
(
select metric_id
, metric_name
, round((VARIANCE(average) ), 1) variance_average
, round((STDDEV(average)  ), 1) stddev_average
, round((AVG(average)  )) avg_average
, round((MIN(average)  )) min_average
, round((MAX(average)  )) max_average
, stat_source
from outliers stat
where 1=1
  and (average > lower_outlier and average < upper_outlier) -- remove the outliers
  and snap_id <> min_snap_id
  and snap_id in (select snap_id from snaps_for_normal_ranges)
--  and trunc(begin_interval_time) between trunc(sysdate- nvl(:normal_ranges_days_back,8)) and trunc(sysdate)
--  and upper(metric_name) like upper(nvl(:metric_name, metric_name))
group by metric_id, metric_name, stat_source
)
)
-- select * from normal_ranges order by upper(metric_name); -- testing SQL up tp this point
, metrics as
(
select instance_name
, snap_id
, begin_interval_time as begin_time
, a.metric_name
, average 
, metric_id
, host_name
, version
, dbid
, instance_number
, a.stat_source
, taxonomy_type
, category
, sub_category
from stat a
, taxonomy b
where 1=1
  and a.stat_source = b.stat_source and a.metric_name = b.metric_name
  and snap_id <> min_snap_id
  and snap_id in (select snap_id from snaps_for_interval)
/*
  and decode(:days_back_only_Y_N,'Y', begin_interval_time, trunc(sysdate-:days_back) ) >= trunc(sysdate-:days_back)
  and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:sam_tm_str_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:sam_tm_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )
  and    (to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:begin_hour, 0) and  nvl(:end_hour, 24) 
       or to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:begin_hour2, nvl(:begin_hour, 0)) and  nvl(:end_hour2, nvl(:end_hour, 24)))
*/
)
-- select * from metrics order by snap_id; -- testing SQL up tp this point
, flags as
(
select instance_name
, host_name
, version
, snap_id
--, a.instance_number
--, a.dbid
, begin_time
--, end_time
, metrics.metric_name
--, minval
, average
, lower_bound
, upper_bound
--, round(100*(1 - ((upper_bound) / nullif(average,0)))) flag_ratio
, round((average - upper_bound) / (nullif(stddev_average, 0)), 2 ) flag_ratio
, case when average > upper_bound then 1 else 0 end as flag
, variance_average
, stddev_average
, average_value
, metrics.metric_id
, metrics.stat_source
, taxonomy_type
, category
, sub_category
from metrics
, normal_ranges
where 1=1
and metrics.metric_name = normal_ranges.metric_name
and metrics.metric_id = normal_ranges.metric_id
and metrics.stat_source = normal_ranges.stat_source
)
-- select * from flags; -- testing SQL up tp this point
, metrics_time_series_view as
(
select taxonomy_type
, category
, sub_category
, stat_source
, snap_id
--, begin_time
, to_char(begin_time,'YYYY-MM-DD HH24:MI') as begin_time
, metric_name
, average  
, lower_bound
, upper_bound
-- , flag_ratio
, flag_ratio
, CASE 
    when flag_ratio between  0.00 and  0.49 then  '*'  
    when flag_ratio between  0.50 and  0.99 then  '**'  
    when flag_ratio between  1.00 and  1.49 then  '***'  
    when flag_ratio between  1.50 and  1.99 then  '****'  
    when flag_ratio between  2.00 and  2.49 then  '*****'  
    when flag_ratio between  2.50 and  2.99 then  '******' 
    when flag_ratio between  3.00 and  3.49 then  '*******'  
    when flag_ratio between  3.50 and  3.99 then  '********'  
    when flag_ratio between  4.00 and  4.49 then  '*********'  
    when flag_ratio between  4.50 and  4.99 then  '**********' 
    when flag_ratio between  5.00 and  5.49 then  '***********'  
    when flag_ratio between  5.50 and  5.99 then  '************'  
    when flag_ratio between  6.00 and  6.49 then  '*************'  
    when flag_ratio between  6.50 and  6.99 then  '**************'  
    when flag_ratio between  7.00 and  7.49 then  '***************'  
    when flag_ratio between  7.50 and  7.99 then  '****************' 
    when flag_ratio between  8.00 and  8.49 then  '*****************'  
    when flag_ratio between  8.50 and  8.99 then  '******************'  
    when flag_ratio between  9.00 and  9.49 then  '*******************'  
    when flag_ratio between  9.50 and  9.99 then  '********************'  
    when flag_ratio between 10.00 and 10.99 then  '**********************'  
    when flag_ratio between 11.00 and 11.99 then  '************************'
    when flag_ratio between 12.00 and 12.99 then  '**************************'  
    when flag_ratio between 13.00 and 13.99 then  '****************************'  
    when flag_ratio between 14.00 and 14.99 then  '******************************'  
    when flag_ratio between 15.00 and 15.99 then  '********************************'    
    when flag_ratio is null                then null
    when flag_ratio < 0                    then null    
    else '******************** ********** **********' 
END as flag_eval
, flag
, variance_average
--, instance_name
--, host_name
--, version
, stddev_average
, average_value
--, metric_id
from flags
where 1=1
/* */
  and decode(:flagged_values_only_Y_N,'Y', 1, flag) = flag  -- if you want to see flagged values only, then include rows that are flagged above or below usual ranges
  and decode(:flagged_values_only_Y_N,'Y', flag_ratio, 999) >= nvl(:flag_ratio, 0.00) -- if you want to see flagged values only, include rows if average is x% bigger then the upper bound
--  and decode(:flagged_values_only_Y_N,'Y', variance_average, 1) >=  .1  -- if you want to see flagged values only, include rows if there is a variance in the values
/* */
--  and upper(metric_name) like upper(nvl(:metric_name_2, metric_name))
--  and stat_source like nvl(:stat_source, stat_source)
-- and category like nvl(:category, category)
-- and sub_category like nvl(:sub_category, sub_category)
order by instance_name, snap_id, decode(:flagged_values_only_Y_N,'Y', flag_ratio, 1) desc, stat_source,  metric_id
)
-- select * from metrics_time_series_view; -- testing thus far
, metrics_aggregate_view as
(
select taxonomy_type, CATEGORY,SUB_CATEGORY,STAT_SOURCE, METRIC_NAME
, count(flag) flag_count
, (select intervals from snaps) intervals
, round(avg(average)) "AVG Flagged Values"
, min(LOWER_BOUND) lower_bound
, max(UPPER_BOUND) upper_bound
--, round(avg(flag_Ratio)) flag_Ratio
, round(avg(flag_ratio), 2) flag_ratio
, CASE 
    when round(avg(flag_ratio),2) between  0.00 and 0.49 then  '*'  
    when round(avg(flag_ratio),2) between  0.50 and 0.99 then  '**'  
    when round(avg(flag_ratio),2) between  1.00 and 1.49 then  '***'  
    when round(avg(flag_ratio),2) between  1.50 and 1.99 then  '****'  
    when round(avg(flag_ratio),2) between  2.00 and 2.49 then  '*****'  
    when round(avg(flag_ratio),2) between  2.50 and 2.99 then  '******' 
    when round(avg(flag_ratio),2) between  3.00 and 3.49 then  '*******'  
    when round(avg(flag_ratio),2) between  3.50 and 3.99 then  '********'  
    when round(avg(flag_ratio),2) between  4.00 and 4.49 then  '*********'  
    when round(avg(flag_ratio),2) between  4.50 and 4.99 then  '**********'  
    
    when round(avg(flag_ratio),2) between  5.00 and 5.49 then  '***********'  
    when round(avg(flag_ratio),2) between  5.50 and 5.99 then  '************'  
    when round(avg(flag_ratio),2) between  6.00 and 6.49 then  '*************'  
    when round(avg(flag_ratio),2) between  6.50 and 6.99 then  '**************'  
    when round(avg(flag_ratio),2) between  7.00 and 7.49 then  '***************'  
    when round(avg(flag_ratio),2) between  7.50 and 7.99 then  '****************' 
    when round(avg(flag_ratio),2) between  8.00 and 8.49 then  '*****************'  
    when round(avg(flag_ratio),2) between  8.50 and 8.99 then  '******************'  
    when round(avg(flag_ratio),2) between  9.00 and 9.49 then  '*******************'  
    when round(avg(flag_ratio),2) between  9.50 and 9.99 then  '********************'  
    when round(avg(flag_ratio),2) is null                then null
    when round(avg(flag_ratio),2) < 0                    then null    
    else '******************** ********** **********' 

END as flag_eval

, avg(average_value) "AVG All"
-- , SNAP_ID,BEGIN_TIME,FLAG_RATIO,FLAG_EVAL,FLAG,VARIANCE_AVERAGE,STDDEV_AVERAGE,AVERAGE_VALUE
from metrics_time_series_view
where 1=1
-- and flag=1 -- look at flagged values only
  and decode(:flagged_values_only_Y_N,'Y', 1, flag) = flag  -- if you want to see flagged values only, then include rows that are flagged above or below usual ranges
  and decode(:flagged_values_only_Y_N,'Y', flag_ratio, 999) >= nvl(:flag_ratio, 0.00) -- if you want to see flagged values only, include rows if average is x% bigger then the upper bound
group by taxonomy_type, CATEGORY,SUB_CATEGORY,STAT_SOURCE, METRIC_NAME
order by 6 desc, 11 desc, taxonomy_type, category, sub_category, stat_source, metric_name
)
--select * from metrics_aggregate_view
, category_count_view as
(select taxonomy_type, category, count(distinct stat_source||':'||metric_name) category_count 
from metrics_time_series_view
group by taxonomy_type, category
)
select * from metrics_time_series_view;
--select * from metrics_aggregate_view;
--select * from category_count_view;

