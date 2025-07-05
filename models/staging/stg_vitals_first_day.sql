{{ config(materialized='view') }}

-- Purpose: Stage and pivot vital signs for the first 24h of ICU stay

with vital_signs_map as (
    select
        ce.subject_id,
        ce.hadm_id,
        ce.icustay_id,
        -- Map itemids to standardized vitalid categories
        case
            when ce.itemid in (211,220045) and ce.valuenum > 0 and ce.valuenum < 300 then 1 -- HeartRate
            when ce.itemid in (51,442,455,6701,220179,220050) and ce.valuenum > 0 and ce.valuenum < 400 then 2 -- SysBP
            when ce.itemid in (8368,8440,8441,8555,220180,220051) and ce.valuenum > 0 and ce.valuenum < 300 then 3 -- DiasBP
            when ce.itemid in (456,52,6702,443,220052,220181,225312) and ce.valuenum > 0 and ce.valuenum < 300 then 4 -- MeanBP
            when ce.itemid in (615,618,220210,224690) and ce.valuenum > 0 and ce.valuenum < 70 then 5 -- RespRate
            when ce.itemid in (223761,678) and ce.valuenum > 70 and ce.valuenum < 120 then 6 -- TempF, will be converted to degC
            when ce.itemid in (223762,676) and ce.valuenum > 10 and ce.valuenum < 50 then 6 -- TempC
            when ce.itemid in (646,220277) and ce.valuenum > 0 and ce.valuenum <= 100 then 7 -- SpO2
            when ce.itemid in (807,811,1529,3745,3744,225664,220621,226537) and ce.valuenum > 0 then 8 -- Glucose
            else null
        end as vitalid,
        -- Convert Fahrenheit to Celsius
        case
            when ce.itemid in (223761,678) then (ce.valuenum - 32) / 1.8
            else ce.valuenum
        end as valuenum
    from {{ ref('stg_chartevents') }} ce
    inner join {{ ref('stg_icustays') }} icu
        on ce.icustay_id = icu.icustay_id
    where ce.charttime between icu.intime and DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
      and ce.itemid in (
        211,220045,
        51,442,455,6701,220179,220050,
        8368,8440,8441,8555,220180,220051,
        456,52,6702,443,220052,220181,225312,
        615,618,220210,224690,
        646,220277,
        807,811,1529,3745,3744,225664,220621,226537,
        223762,676,223761,678
      )
      and (ce.error IS NULL or ce.error = 0)
)

select
    subject_id,
    hadm_id,
    icustay_id,

    min(case when vitalid = 1 then valuenum else null end) as heartrate_min,
    max(case when vitalid = 1 then valuenum else null end) as heartrate_max,
    avg(case when vitalid = 1 then valuenum else null end) as heartrate_mean,

    min(case when vitalid = 2 then valuenum else null end) as sysbp_min,
    max(case when vitalid = 2 then valuenum else null end) as sysbp_max,
    avg(case when vitalid = 2 then valuenum else null end) as sysbp_mean,

    min(case when vitalid = 3 then valuenum else null end) as diasbp_min,
    max(case when vitalid = 3 then valuenum else null end) as diasbp_max,
    avg(case when vitalid = 3 then valuenum else null end) as diasbp_mean,

    min(case when vitalid = 4 then valuenum else null end) as meanbp_min,
    max(case when vitalid = 4 then valuenum else null end) as meanbp_max,
    avg(case when vitalid = 4 then valuenum else null end) as meanbp_mean,

    min(case when vitalid = 5 then valuenum else null end) as resprate_min,
    max(case when vitalid = 5 then valuenum else null end) as resprate_max,
    avg(case when vitalid = 5 then valuenum else null end) as resprate_mean,

    min(case when vitalid = 6 then valuenum else null end) as tempc_min,
    max(case when vitalid = 6 then valuenum else null end) as tempc_max,
    avg(case when vitalid = 6 then valuenum else null end) as tempc_mean,

    min(case when vitalid = 7 then valuenum else null end) as spo2_min,
    max(case when vitalid = 7 then valuenum else null end) as spo2_max,
    avg(case when vitalid = 7 then valuenum else null end) as spo2_mean,

    min(case when vitalid = 8 then valuenum else null end) as glucose_min,
    max(case when vitalid = 8 then valuenum else null end) as glucose_max,
    avg(case when vitalid = 8 then valuenum else null end) as glucose_mean

from vital_signs_map
where vitalid is not null
group by subject_id, hadm_id, icustay_id
