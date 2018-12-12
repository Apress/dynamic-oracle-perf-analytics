-- DOPA_Normalized_Unioned_Metrics_3_9.sql
-- RDCornejo 26-Sep-2018
-- example SQL code for producing listing used in Figure 3-9.
-- note: this runs faster if you subset the snapshot range 
--       snapshot range not subset here to focus in on 
--       -  normalizing ; 
--       -  converting cumulatives to DELTAs ; 
--       -  and Unioning the normalized data from all the metric sources
-- -----------------------------------------------------------------------
with latch as (select * from dba_hist_latch)
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
, system_event as ( select * from dba_hist_system_event )
, unpivot_system_event as
(
      select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_waits' metric_name, total_waits cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_timeouts' metric_name,  total_timeouts cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' time_waited_micro' metric_name,  time_waited_micro cumulative_value , a.dbid, a.instance_number from system_event a
)
, iostat_function as ( select * from dba_hist_iostat_function )
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
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
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
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
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
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
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
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
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
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
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
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
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
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
)
select snap_id, stat_source, metric_name, average
from stat  
where 1=1
-- just one snapshot for the example
and snap_id = (select max(snap_id) from stat)-2
-- the metrics from figures 3-4, 3-5, 3-6 
and (metric_name like '%cache buffers chains%' 
  or metric_name like '%cursor: pin S wait on X%' 
  or metric_name like '%Buffer Cache Reads%')
order by snap_id, stat_source, metric_name
; 
