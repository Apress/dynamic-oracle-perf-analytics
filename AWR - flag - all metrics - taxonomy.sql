-- AWR - flag - all metrics - taxonomy.sql
-- code to create the taxonomy table and populate it
-- Author: Roger Cornejo
-- updated 21-Sep-2018 RDCornejo
-- DOPA Process -- used to create the raxonomy table and populate it
-- -------------------------------------------------------------------------------------------
-- This work is offered by the author as a professional curtesy, “as-is” and without warranty.  
-- The author disclaims any liability for damages that may result from using this code.
-- --------------------------------------------------------------------------------------------- 
-- Updated: 17-Apr-2018 implemented refactored taxonomy [added taxonomy_type so 1 or more different taxonomies can use the same structure]
-- modified 02-Jan-2018 RDCornejo improving taxonomy

-- drop table metric_taxonomy;
-- drop table metric_taxonomy_old;
-- rename metric_taxonomy to  metric_taxonomy_old;
-- create table metric_taxonomy as select * from metric_taxonomy;
-- =============================================================================================================================
create table metric_taxonomy as  -- select * from metric_taxonomy;
with latch as
(
select * from dba_hist_latch a 
where snap_id in 
(select snap_id from dba_hist_snapshot 
where 1=1
  and trunc(begin_interval_time)+ 1/24 = trunc(sysdate)+1/24
)
--  and nvl(:dba_hist_latch, 'Y') = 'Y'

)
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
, system_event as
(
select * from dba_hist_system_event a where snap_id in (
select snap_id from dba_hist_snapshot 
where 1=1
and trunc(begin_interval_time)+ 1/24 = trunc(sysdate)+1/24
) 
)
, unpivot_system_event as
(
      select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_waits' metric_name, total_waits cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_timeouts' metric_name,  total_timeouts cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' time_waited_micro' metric_name,  time_waited_micro cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_waits_fg' metric_name,  total_waits_fg cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_timeouts_fg' metric_name,  total_timeouts_fg cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' time_waited_micro_fg' metric_name,  time_waited_micro_fg cumulative_value , a.dbid, a.instance_number from system_event a
)
, iostat_function as
(
select * from dba_hist_iostat_function a where snap_id in (
select snap_id from dba_hist_snapshot 
where 1=1
  and trunc(begin_interval_time)+ 1/24 = trunc(sysdate)+1/24
  )
)
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
select distinct stat_name metric_name
, 'dba_hist_sys_time_model' stat_source
from dba_hist_sys_time_model a
where 1=1
--  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
union
select distinct stat_name metric_name
, 'dba_hist_sysstat' stat_source
from dba_hist_sysstat a
where 1=1
--  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
union
select distinct stat_name metric_name
, 'dba_hist_osstat' stat_source
from dba_hist_osstat a
where 1=1
--  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
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
select distinct metric_name
, 'dba_hist_iostat_function' stat_source
from unpivot_iostat_function a
where 1=1
--  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
union
select distinct metric_name
, 'dba_hist_sysmetric_summary' stat_source
from dba_hist_sysmetric_summary a 
where 1=1
--  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
union
select distinct metric_name
, 'dba_hist_system_event' stat_source
from unpivot_system_event a
where 1=1
--  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
union 
select distinct metric_name
, 'dba_hist_latch' stat_source
from unpivot_latch a
where 1=1
--  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
)
, metrics as
(
select stat_source, metric_name
from stat a
where 1=1
)
select 'Infrastructure' taxonomy_type
, stat_source, metric_name
, case when stat_source = 'dba_hist_iostat_function' then 'io' 
       when upper(metric_name) like '% IO %' then 'io'
       when upper(metric_name) like '%BUFFER BUSY%' then 'io'
       when upper(metric_name) like 'USER I/O:%' then 'io'
       when upper(metric_name) like '%REDO LOG%' then 'io'
       when upper(metric_name) like '%SPACE MANAGER%' then 'io'
       when upper(metric_name) like '%INDEX%' then 'io'
       when upper(metric_name) like '%LEAF%' then 'io'
       when upper(metric_name) like '%DB FILE%' then 'io'
       when upper(metric_name) like '%ASYNCH%' then 'io'
       when upper(metric_name) like '%CURSOR' then 'memory'
       when upper(metric_name) like '%LOG FILE%' then 'io'
       when upper(metric_name) like '%SEGMENT%' then 'io'
       when upper(metric_name) like '%REDO%' then 'sql'
       when upper(metric_name) like 'APPLICATION:%' then 'sql'
       when upper(metric_name) like 'DB TIME' then 'cpu'
       when upper(metric_name) like '%CPU%' then 'cpu' 
       when upper(metric_name) like '%OS%' then 'cpu'
       when upper(metric_name) like '%LOGO%' then 'cpu'
       when upper(metric_name) like '%TIME%' then 'cpu'
       when upper(metric_name) like '%ACTIVE%' then 'cpu'
       when upper(metric_name) like 'LOAD' then 'cpu'
       when upper(metric_name) like '%RUN QUEUE%' then 'cpu'
       when upper(metric_name) like '%TO/FROM%' then 'network' 
       when upper(metric_name) like '%SQL*NET%' then 'network' 
       when upper(metric_name) like '%NETWORK%' then 'network'
       when upper(metric_name) like '%CACHE%' then 'memory'
       -- when upper(metric_name) like 'CURRENT OPEN CURSORS COUNT' then 'memory' 
       when upper(metric_name) like 'ROWS PER SORT' then 'memory' 
       when upper(metric_name) like 'TOTAL SORTS PER USER CALL' then 'memory' 
       when upper(metric_name) like 'SWAP_FREE_BYTES' then 'memory' 
       when upper(metric_name) like '%MEM%' then 'memory' 
       when upper(metric_name) like '%SGA%' then 'memory' 
       when upper(metric_name) like '%PGA%' then 'memory' 
       when upper(metric_name) like 'VM_%' then 'memory' 
       when upper(metric_name) like '%GC%' then 'memory'  
       when upper(metric_name) like '%SESSION COUNT%' then 'memory'
       when upper(metric_name) like '%BUFFER%' then 'memory'
       when upper(metric_name) like '%CURSOR:%' then 'memory'
       when upper(metric_name) like '%SQL AREA%' then 'memory'
       when upper(metric_name) like '%WRIT%' then 'io' 
       when upper(metric_name) like '%READ%' then 'io' 
       when upper(metric_name) like '% IO %' then 'io' 
       when upper(metric_name) like '%SCANS%' then 'io' 
       when upper(metric_name) like '% I/O%' then 'io' 
       when upper(metric_name) like 'I/O%' then 'io' 
       when upper(metric_name) like 'LOGICAL%' then 'io'
       when upper(metric_name) like '%CONSISTENT%' then 'io'
       when upper(metric_name) like '%LOG FILE%' then 'io' 
       when upper(metric_name) like 'USER I/O:%' then 'io' 
       when upper(metric_name) like 'FILE IO%' then 'io' 
       when upper(metric_name) like '%SECUREFILE%' then 'io' 
       
       when upper(metric_name) like 'CR %' then 'io'
       when upper(metric_name) like '%BLOCK%' then 'io' 
       when upper(metric_name) like '%NODE SPLITS%' then 'io' 
       when upper(metric_name) like 'TABLE %' then 'io' 
       when upper(metric_name) like 'DISK SORT %' then 'io'
       when upper(metric_name) like '%CHECKPOINT%' then 'io'
       when upper(metric_name) like '%HEATMAP%' then 'io'
       when upper(metric_name) like '%TEMP%' then 'io'
       when upper(metric_name) like '%CHAINED ROWS%' then 'io'
       when upper(metric_name) like '%FETCH%' then 'io'
       when upper(metric_name) like '%INDEX%' then 'io'
       when upper(metric_name) like '%ENQUEUE%' then 'db'
       when upper(metric_name) like '%PARALLEL%' then 'sql'
       when upper(metric_name) like '%PQ%' then 'sql'
       when upper(metric_name) like '%EXECUTION%' then 'sql'
       when upper(metric_name) like '%PX%' then 'sql'
       when upper(metric_name) like '%JAVA%' then 'sql'
       when upper(metric_name) like '%PARSE%' then 'sql'
       when upper(metric_name) like '%RECURSIVE%' then 'sql'
       when upper(metric_name) like '%CURSORS%' then 'sql'
       when upper(metric_name) like '%SHARED POOL%' then 'sql'
       when upper(metric_name) like '%USER C%' then 'sql'
       when upper(metric_name) like '%ENQUEUE%' then 'sql'
       when upper(metric_name) like '%CR %' then 'sql'
       when upper(metric_name) like '%REDO%' then 'sql'
       when upper(metric_name) like '%UNDO%' then 'sql'
       when upper(metric_name) like '%TRANSACTION%' then 'sql'
       when upper(metric_name) like '%COMMIT%' then 'sql'
       when upper(metric_name) like '%LIMIT%' then 'db'
       when upper(metric_name) like '%ROLLBACKS%' then 'db'
       when upper(metric_name) like '%LATCH%' then 'db'
       
              else 'any'
  end as category
, case when stat_source = 'dba_hist_system_event' then 'wait'
       when upper(metric_name) like '%WAIT%' then 'wait' 
       when upper(metric_name) like '%READ%' then 'read' 
       when upper(metric_name) like '%WRITE%' then 'write' 
       when upper(metric_name) like '%CHECKPOINT%' then 'write'
       when upper(metric_name) like '%PARSE%' then 'parse'
       when upper(metric_name) like '%EXECUTION%' then 'execute'
       when upper(metric_name) like '%RECURSIVE%' then 'recursive'
       when upper(metric_name) like '%CURSORS%' then 'cursors'
       when upper(metric_name) like '%REDO%' then 'recovery'
       when upper(metric_name) like '%UNDO%' then 'recovery'
       when upper(metric_name) like '%ROLLBACKS%' then 'recovery'
       when upper(metric_name) like '%PARALLEL%' then 'parallel'
       when upper(metric_name) like '%PX%' then 'parallel'
       when upper(metric_name) like '%PQ%' then 'parallel'
       when upper(metric_name) like '%NODE SPLITS%' then 'index'
       when upper(metric_name) like '%ENQUEUE%' then 'concurrency'
       when upper(metric_name) like '%CR %' then 'concurrency'
       when upper(metric_name) like '%CONCURRENCY%' then 'concurrency'
       when upper(metric_name) like '%LIMIT%' then 'limit'
       when upper(metric_name) like '%TEMP%' then 'temp'
       when upper(metric_name) like '%SESSION COUNT%' then 'sessions'
       else 'any'
  end as sub_category
from (select distinct stat_source, metric_name from metrics  )
union ALL
select 'Oracle' taxonomy_type
, stat_source, metric_name
, case 
       when stat_source = 'dba_hist_latch' then 'LOCKING'
       when stat_source = 'dba_hist_osstat' then 'OS STAT'
       when upper(metric_name) like '%ENQUEUE%' then 'LOCKING'
       when upper(metric_name) like 'REDO%'  then 'REDO' 
       when upper(metric_name) like '%LOG WRITE%'  then 'REDO' 
       when upper(metric_name) like '%: LOG %'  then 'REDO' 
       when upper(metric_name) like 'ARCH%'  then 'ARCH' 
       when upper(metric_name) like '%PGA%'  then 'PGA' 
       when upper(metric_name) like '%SGA%'  then 'SGA'
       when upper(metric_name) like '%UNDO%' then 'UNDO'
       when upper(metric_name) like '%IMU%' then 'UNDO'
       when upper(metric_name) like '%DBWR%' then 'DBWR' 
       when upper(metric_name) like '%LGWR%' then 'LGWR' 
       when upper(metric_name) like '%RMAN%' then 'RMAN' 
       when upper(metric_name) like '%RESMGR%' then 'RESMGR' 
       when upper(metric_name) like '%STREAMS%' then 'STREAMS' 
       when upper(metric_name) like '%UTL_FILE%' then 'UTL_FILE' 
       when upper(metric_name) like '%EXTERNAL TABLE%' then 'EXTERNAL TABLE' 
       when upper(metric_name) like 'SMART SCAN%' then 'SMART SCAN' 
       when upper(metric_name) like 'OTHERS%' then 'OTHERS' 
       when upper(metric_name) like 'RECOVERY%' then 'RECOVERY' 
       when upper(metric_name) like 'STREAMS AQ%' then 'STREAMS AQ' 
       when upper(metric_name) like 'XDB%' then 'XDB' 
       when upper(metric_name) like '%PARSE%' then 'PARSE'
       when upper(metric_name) like '%JAVA%' then 'JAVA'
       when upper(metric_name) like '%OLAP%' then 'OLAP'
       when upper(metric_name) like '%PL/SQL%' then 'PL/SQL'
       when upper(metric_name) like '%BACKGROUND%' then 'BACKGROUND'
       when upper(metric_name) like '%CONNECTION%' then 'CONNECTION'
       when upper(metric_name) like '%INMEMORY POPULATE%' then 'IN-MEMORY'
       when upper(metric_name) like '%BUFFER CACHE%' then 'BUFFER CACHE'
       when upper(metric_name) like '%DATA PUMP%' then 'DATA PUMP'
       when upper(metric_name) like 'DIRECT %E%S%' then 'DIRECT I/O'
       when upper(metric_name) like '%HEATMAP%' then 'COMPRESSION'
       else 'ALL'
   end as category
, case when upper(metric_name) like 'DB CPU' then 'WORKLOAD'
       when upper(metric_name) like 'ROWS PER SORT' then 'WORKLOAD'
       when upper(metric_name) like '%LOAD%' then 'WORKLOAD'
       when upper(metric_name) like '%ACTIVE%' then 'WORKLOAD'
       when upper(metric_name) like '%USER C%' then 'WORKLOAD'
       when upper(metric_name) like '%PER SEC%' then 'WORKLOAD'
       when upper(metric_name) like '%PER TXN%' then 'WORKLOAD'
       when upper(metric_name) like 'DB TIME' then 'WORKLOAD'
       when upper(metric_name) like '%TIME%' then 'TIME'
       when upper(metric_name) like '%LATENCY%' then 'LATENCY'
       when upper(metric_name) like '%RATIO%' then 'RATIO'
       when upper(metric_name) like '%COUNT%' then 'COUNT'
       else 'ALL'
   end as sub_category
from (select distinct stat_source, metric_name from metrics  )
order by 3,1,2
--order by 1,2
;

