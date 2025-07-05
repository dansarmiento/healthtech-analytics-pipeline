{{ config(materialized='table') }}

-- Purpose: Calculate Sequential Organ Failure Assessment (SOFA) score for each ICU stay on day 1.

with wt as (
    select
        icu.icustay_id,
        avg(
            case
                when ce.itemid in (762, 763, 3723, 3580, 226512) then ce.valuenum
                when ce.itemid = 3581 then ce.valuenum * 0.45359237  -- lbs to kg
                when ce.itemid = 3582 then ce.valuenum * 0.0283495231  -- oz to kg
                else null
            end
        ) as weight_kg
    from {{ ref('stg_icustays') }} icu
    left join {{ ref('stg_chartevents') }} ce
        on icu.icustay_id = ce.icustay_id
    where ce.valuenum is not null
      and ce.itemid in (762, 763, 3723, 3580, 3581, 3582, 226512)
      and ce.valuenum != 0
      and ce.charttime between datetime_sub(icu.intime, interval '1' day) and datetime_add(icu.intime, interval '1' day)
      and (ce.error is null or ce.error = 0)
    group by icu.icustay_id
),
echo2 as (
    select
        icu.icustay_id,
        avg(echo.weight_lb * 0.45359237) as weight_kg
    from {{ ref('stg_icustays') }} icu
    left join {{ ref('stg_echodata') }} echo
        on icu.hadm_id = echo.hadm_id
        and echo.charttime > datetime_sub(icu.intime, interval '7' day)
        and echo.charttime < datetime_add(icu.intime, interval '1' day)
    group by icu.icustay_id
),
vaso_cv as (
    select
        icu.icustay_id,
        max(case when cv.itemid = 30047 then cv.rate / coalesce(wt.weight_kg, echo2.weight_kg) end) as rate_norepinephrine,
        max(case when cv.itemid = 30120 then cv.rate end) as rate_norepinephrine_alt,
        max(case when cv.itemid = 30044 then cv.rate / coalesce(wt.weight_kg, echo2.weight_kg) end) as rate_epinephrine,
        max(case when cv.itemid in (30119, 30309) then cv.rate end) as rate_epinephrine_alt,
        max(case when cv.itemid in (30043, 30307) then cv.rate end) as rate_dopamine,
        max(case when cv.itemid in (30042, 30306) then cv.rate end) as rate_dobutamine
    from {{ ref('stg_icustays') }} icu
    inner join {{ ref('stg_inputevents_cv') }} cv
        on icu.icustay_id = cv.icustay_id
        and cv.charttime between icu.intime and datetime_add(icu.intime, interval '1' day)
    left join wt on icu.icustay_id = wt.icustay_id
    left join echo2 on icu.icustay_id = echo2.icustay_id
    where cv.itemid in (30047, 30120, 30044, 30119, 30309, 30043, 30307, 30042, 30306)
      and cv.rate is not null
    group by icu.icustay_id
),
vaso_mv as (
    select
        icu.icustay_id,
        max(case when mv.itemid = 221906 then mv.rate end) as rate_norepinephrine,
        max(case when mv.itemid = 221289 then mv.rate end) as rate_epinephrine,
        max(case when mv.itemid = 221662 then mv.rate end) as rate_dopamine,
        max(case when mv.itemid = 221653 then mv.rate end) as rate_dobutamine
    from {{ ref('stg_icustays') }} icu
    inner join {{ ref('stg_inputevents_mv') }} mv
        on icu.icustay_id = mv.icustay_id
        and mv.starttime between icu.intime and datetime_add(icu.intime, interval '1' day)
    where mv.itemid in (221906, 221289, 221662, 221653)
      and mv.statusdescription != 'Rewritten'
    group by icu.icustay_id
),
pafi1 as (
    select
        bg.icustay_id,
        bg.charttime,
        bg.pao2fio2,
        case when vd.icustay_id is not null then 1 else 0 end as isvent
    from {{ ref('stg_blood_gas_first_day_arterial') }} bg
    left join {{ ref('stg_ventilation_durations') }} vd
        on bg.icustay_id = vd.icustay_id
        and bg.charttime >= vd.starttime
        and bg.charttime <= vd.endtime
),
pafi2 as (
    select
        icustay_id,
        min(case when isvent = 0 then pao2fio2 else null end) as pao2fio2_novent_min,
        min(case when isvent = 1 then pao2fio2 else null end) as pao2fio2_vent_min
    from pafi1
    group by icustay_id
),
scorecomp as (
    select
        icu.icustay_id,
        v.meanbp_min,
        coalesce(cv.rate_norepinephrine, cv.rate_norepinephrine_alt, mv.rate_norepinephrine) as rate_norepinephrine,
        coalesce(cv.rate_epinephrine, cv.rate_epinephrine_alt, mv.rate_epinephrine) as rate_epinephrine,
        coalesce(cv.rate_dopamine, mv.rate_dopamine) as rate_dopamine,
        coalesce(cv.rate_dobutamine, mv.rate_dobutamine) as rate_dobutamine,
        l.creatinine_max,
        l.bilirubin_max,
        l.platelet_min,
        pf.pao2fio2_novent_min,
        pf.pao2fio2_vent_min,
        uo.urineoutput,
        gcs.mingcs
    from {{ ref('stg_icustays') }} icu
    left join vaso_cv cv on icu.icustay_id = cv.icustay_id
    left join vaso_mv mv on icu.icustay_id = mv.icustay_id
    left join pafi2 pf on icu.icustay_id = pf.icustay_id
    left join {{ ref('stg_vitals_first_day') }} v on icu.icustay_id = v.icustay_id
    left join {{ ref('stg_labs_first_day') }} l on icu.icustay_id = l.icustay_id
    left join {{ ref('stg_urine_output_first_day') }} uo on icu.icustay_id = uo.icustay_id
    left join {{ ref('stg_gcs_first_day') }} gcs on icu.icustay_id = gcs.icustay_id
),
scorecalc as (
    select
        icustay_id,

        -- Respiration
        case
            when pao2fio2_vent_min < 100 then 4
            when pao2fio2_vent_min < 200 then 3
            when pao2fio2_novent_min < 300 then 2
            when pao2fio2_novent_min < 400 then 1
            when coalesce(pao2fio2_vent_min, pao2fio2_novent_min) is null then null
            else 0
        end as respiration,

        -- Coagulation
        case
            when platelet_min < 20 then 4
            when platelet_min < 50 then 3
            when platelet_min < 100 then 2
            when platelet_min < 150 then 1
            when platelet_min is null then null
            else 0
        end as coagulation,

        -- Liver
        case
            when bilirubin_max >= 12.0 then 4
            when bilirubin_max >= 6.0 then 3
            when bilirubin_max >= 2.0 then 2
            when bilirubin_max >= 1.2 then 1
            when bilirubin_max is null then null
            else 0
        end as liver,

        -- Cardiovascular
        case
            when rate_dopamine > 15 or rate_epinephrine > 0.1 or rate_norepinephrine > 0.1 then 4
            when rate_dopamine > 5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
            when rate_dopamine > 0 or rate_dobutamine > 0 then 2
            when meanbp_min < 70 then 1
            when coalesce(meanbp_min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
            else 0
        end as cardiovascular,

        -- CNS
        case
            when (mingcs >= 13 and mingcs <= 14) then 1
            when (mingcs >= 10 and mingcs <= 12) then 2
            when (mingcs >= 6 and mingcs <= 9) then 3
            when (mingcs < 6) then 4
            when mingcs is null then null
            else 0
        end as cns,

        -- Renal
        case
            when creatinine_max >= 5.0 then 4
            when urineoutput < 200 then 4
            when creatinine_max >= 3.5 and creatinine_max < 5.0 then 3
            when urineoutput < 500 then 3
            when creatinine_max >= 2.0 and creatinine_max < 3.5 then 2
            when creatinine_max >= 1.2 and creatinine_max < 2.0 then 1
            when coalesce(urineoutput, creatinine_max) is null then null
            else 0
        end as renal
    from scorecomp
)

select
    icu.subject_id,
    icu.hadm_id,
    icu.icustay_id,
    coalesce(sc.respiration, 0)
    + coalesce(sc.coagulation, 0)
    + coalesce(sc.liver, 0)
    + coalesce(sc.cardiovascular, 0)
    + coalesce(sc.cns, 0)
    + coalesce(sc.renal, 0) as sofa,
    sc.respiration,
    sc.coagulation,
    sc.liver,
    sc.cardiovascular,
    sc.cns,
    sc.renal
from {{ ref('stg_icustays') }} icu
left join scorecalc sc on icu.icustay_id = sc.icustay_id
