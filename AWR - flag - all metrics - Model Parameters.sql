-- AWR - flag - all metrics - Model Parameters.sql
-- 21-Sep-2018 RDCornejo
-- describes current parameter settings that are used in the various DOPA views.
-- -------------------------------------------------------------------------------------------
-- This work is offered by the author as a professional curtesy, “as-is” and without warranty.  
-- The author disclaims any liability for damages that may result from using this code.
-- -------------------------------------------------------------------------------------------

select ord , param_name, nvl(param_val, 'NULL') param_value, default_value, description from 
(
      select '01' ord, nvl(:taxonomy_type, 'Infrastructure') param_val, ':taxonomy_type' param_name , 'Infrastructure' default_value, 'to subset on this metric' description from dual
union select '02', :category , ':category' , 'NULL' default_value , 'optional: to subset on this metric' description from dual
union select '03', :sub_category, ':sub_category' , 'NULL' default_value , 'optional: to subset on this metric' description from dual
union select '04', :stats_days_back_only_Y_N, ':stats_days_back_only_Y_N', 'N' default_value  , 'optional: easy way to set a problem interval date range' description from dual
union select '05', :stats_days_back, ':stats_days_back' , 'NULL' default_value , 'optional: specify a number of days back' description from dual
union select '06', :allint_st_MM_DD_YYYY_HH24_MI , ':allint_st_MM_DD_YYYY_HH24_MI' , 'NULL' default_value , 'Start date/time for all metrics' description from dual
union select '07', :allint_end_MM_DD_YYYY_HH24_MI , ':allint_end_MM_DD_YYYY_HH24_MI', 'NULL' default_value , 'End date/time for all metrics' description from dual
union select '08', :intrvl_st_MM_DD_YYYY_HH24_MI , ':intrvl_st_MM_DD_YYYY_HH24_MI', 'NULL' default_value  , 'Start date/time for the problem interval' description from dual
union select '09', :intrvl_end_MM_DD_YYYY_HH24_MI , ':intrvl_end_MM_DD_YYYY_HH24_MI', 'NULL' default_value  , 'End date/time for the problem interval' description from dual
union select '10', :normRng_st_MM_DD_YYYY_HH24_MI , ':normRng_st_MM_DD_YYYY_HH24_MI' , 'NULL' default_value  , 'Start date/time for the normal ranges interval' description from dual
union select '11', :normRng_end_MM_DD_YYYY_HH24_MI , ':normRng_end_MM_DD_YYYY_HH24_MI', 'NULL' default_value  , 'End date/time for the normal ranges interval' description from dual
union select '12', nvl(:dba_hist_sys_time_model, 'Y') , ':dba_hist_sys_time_model' , 'Y' default_value  , 'Use "Y" to include this data source in the analysis' description from dual
union select '13', :metric_name , ':metric_name' , 'NULL' default_value  , 'to subset :metric_name like <>' description from dual
union select '14', :inst_id , ':inst_id' , 'NULL' default_value  , 'optional: to specify an instance number as in RAC environments' description from dual
union select '15', nvl(:dba_hist_sysstat, 'Y') , ':dba_hist_sysstat' , 'Y' default_value , 'Use "Y" to include this data source in the analysis' description from dual
union select '16', nvl(:dba_hist_osstat, 'Y') , ':dba_hist_osstat', 'Y' default_value   , 'Use "Y" to include this data source in the analysis'  description from dual
union select '17', nvl(:dba_hist_iostat_function, 'Y') , ':dba_hist_iostat_function', 'Y' default_value   , 'Use "Y" to include this data source in the analysis'  description from dual
union select '18', nvl(:dba_hist_sysmetric_summary, 'Y') , ':dba_hist_sysmetric_summary', 'Y' default_value   , 'Use "Y" to include this data source in the analysis'  description from dual
union select '19', nvl(:dba_hist_system_event, 'Y') , ':dba_hist_system_event', 'Y' default_value  , 'Use "Y" to include this data source in the analysis'  description  from dual
union select '20', nvl(:dba_hist_latch, 'N') , ':dba_hist_latch', 'N' default_value   , 'Use "N" to exclude this data source in the analysis'  description from dual
union select '21', nvl(:iqr_factor, 1.5) , ':iqr_factor' , '1.5' default_value  , 'to define a different inter-quartile range factor' description from dual
union select '22', nvl(:Q1_PERCENTILE,0.25) , ':Q1_PERCENTILE', '.25' default_value   , 'to specify different lower percentile (default is 1st quartile)' description from dual
union select '23', nvl(:Q3_PERCENTILE,0.75) , ':Q3_PERCENTILE' , '.75' default_value  , 'to specify different upper percentile (default is 3rd quartile)' description from dual
union select '24', :stat_source , ':stat_source', 'NULL' default_value  , 'to subset on a single STAT_SOURCE' description from dual
union select '25', :flagged_values_only_Y_N , ':flagged_values_only_Y_N', 'NULL' default_value   , 'usually set to Y  - exception: "N" for Metric Time-Series' description from dual
union select '26', nvl(:flag_ratio, 0.00) , ':flag_ratio', '0.00' default_value   , 'to subset on metrics with flag_ratio values > = to specified flag_ratio' description from dual
)
order by 1
;
