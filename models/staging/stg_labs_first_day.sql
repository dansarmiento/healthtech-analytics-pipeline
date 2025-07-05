{{ config(materialized='view') }}

-- Purpose: Pivot and clean lab results for the first 24 hours of ICU stay

with labeled_labs as (
    select
        icu.subject_id,
        icu.hadm_id,
        icu.icustay_id,
        -- Label assignment
        case
            when le.itemid = 50868 then 'anion_gap'
            when le.itemid = 50862 then 'albumin'
            when le.itemid = 51144 then 'bands'
            when le.itemid = 50882 then 'bicarbonate'
            when le.itemid = 50885 then 'bilirubin'
            when le.itemid = 50912 then 'creatinine'
            when le.itemid in (50806, 50902) then 'chloride'
            when le.itemid in (50809, 50931) then 'glucose'
            when le.itemid in (50810, 51221) then 'hematocrit'
            when le.itemid in (50811, 51222) then 'hemoglobin'
            when le.itemid = 50813 then 'lactate'
            when le.itemid = 51265 then 'platelet'
            when le.itemid in (50822, 50971) then 'potassium'
            when le.itemid = 51275 then 'ptt'
            when le.itemid = 51237 then 'inr'
            when le.itemid = 51274 then 'pt'
            when le.itemid in (50824, 50983) then 'sodium'
            when le.itemid = 51006 then 'bun'
            when le.itemid in (51300, 51301) then 'wbc'
            else null
        end as label,
        -- Value cleaning
        case
            when le.itemid = 50862 and le.valuenum > 10 then null -- albumin
            when le.itemid = 50868 and le.valuenum > 10000 then null -- anion gap
            when le.itemid = 51144 and (le.valuenum < 0 or le.valuenum > 100) then null -- bands
            when le.itemid = 50882 and le.valuenum > 10000 then null -- bicarbonate
            when le.itemid = 50885 and le.valuenum > 150 then null -- bilirubin
            when le.itemid in (50806, 50902) and le.valuenum > 10000 then null -- chloride
            when le.itemid = 50912 and le.valuenum > 150 then null -- creatinine
            when le.itemid in (50809, 50931) and le.valuenum > 10000 then null -- glucose
            when le.itemid in (50810, 51221) and le.valuenum > 100 then null -- hematocrit
            when le.itemid in (50811, 51222) and le.valuenum > 50 then null -- hemoglobin
            when le.itemid = 50813 and le.valuenum > 50 then null -- lactate
            when le.itemid = 51265 and le.valuenum > 10000 then null -- platelet
            when le.itemid in (50822, 50971) and le.valuenum > 30 then null -- potassium
            when le.itemid = 51275 and le.valuenum > 150 then null -- ptt
            when le.itemid = 51237 and le.valuenum > 50 then null -- inr
            when le.itemid = 51274 and le.valuenum > 150 then null -- pt
            when le.itemid in (50824, 50983) and le.valuenum > 200 then null -- sodium
            when le.itemid = 51006 and le.valuenum > 300 then null -- bun
            when le.itemid in (51300, 51301) and le.valuenum > 1000 then null -- wbc
            else le.valuenum
        end as valuenum
    from {{ ref('stg_icustays') }} icu
    left join {{ ref('stg_labevents') }} le
        on le.subject_id = icu.subject_id
        and le.hadm_id = icu.hadm_id
        and le.charttime between DATETIME_SUB(icu.intime, INTERVAL '6' HOUR) and DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
        and le.itemid in (
            50868, 50862, 51144, 50882, 50885, 50912,
            50806, 50902, 50809, 50931, 50810, 51221,
            50811, 51222, 50813, 51265, 50822, 50971,
            51275, 51237, 51274, 50824, 50983, 51006,
            51301, 51300
        )
    where le.valuenum is not null and le.valuenum > 0
)

select
    subject_id,
    hadm_id,
    icustay_id,

    min(case when label = 'anion_gap' then valuenum end) as aniongap_min,
    max(case when label = 'anion_gap' then valuenum end) as aniongap_max,
    min(case when label = 'albumin' then valuenum end) as albumin_min,
    max(case when label = 'albumin' then valuenum end) as albumin_max,
    min(case when label = 'bands' then valuenum end) as bands_min,
    max(case when label = 'bands' then valuenum end) as bands_max,
    min(case when label = 'bicarbonate' then valuenum end) as bicarbonate_min,
    max(case when label = 'bicarbonate' then valuenum end) as bicarbonate_max,
    min(case when label = 'bilirubin' then valuenum end) as bilirubin_min,
    max(case when label = 'bilirubin' then valuenum end) as bilirubin_max,
    min(case when label = 'creatinine' then valuenum end) as creatinine_min,
    max(case when label = 'creatinine' then valuenum end) as creatinine_max,
    min(case when label = 'chloride' then valuenum end) as chloride_min,
    max(case when label = 'chloride' then valuenum end) as chloride_max,
    min(case when label = 'glucose' then valuenum end) as glucose_min,
    max(case when label = 'glucose' then valuenum end) as glucose_max,
    min(case when label = 'hematocrit' then valuenum end) as hematocrit_min,
    max(case when label = 'hematocrit' then valuenum end) as hematocrit_max,
    min(case when label = 'hemoglobin' then valuenum end) as hemoglobin_min,
    max(case when label = 'hemoglobin' then valuenum end) as hemoglobin_max,
    min(case when label = 'lactate' then valuenum end) as lactate_min,
    max(case when label = 'lactate' then valuenum end) as lactate_max,
    min(case when label = 'platelet' then valuenum end) as platelet_min,
    max(case when label = 'platelet' then valuenum end) as platelet_max,
    min(case when label = 'potassium' then valuenum end) as potassium_min,
    max(case when label = 'potassium' then valuenum end) as potassium_max,
    min(case when label = 'ptt' then valuenum end) as ptt_min,
    max(case when label = 'ptt' then valuenum end) as ptt_max,
    min(case when label = 'inr' then valuenum end) as inr_min,
    max(case when label = 'inr' then valuenum end) as inr_max,
    min(case when label = 'pt' then valuenum end) as pt_min,
    max(case when label = 'pt' then valuenum end) as pt_max,
    min(case when label = 'sodium' then valuenum end) as sodium_min,
    max(case when label = 'sodium' then valuenum end) as sodium_max,
    min(case when label = 'bun' then valuenum end) as bun_min,
    max(case when label = 'bun' then valuenum end) as bun_max,
    min(case when label = 'wbc' then valuenum end) as wbc_min,
    max(case when label = 'wbc' then valuenum end) as wbc_max

from labeled_labs
group by subject_id, hadm_id, icustay_id
