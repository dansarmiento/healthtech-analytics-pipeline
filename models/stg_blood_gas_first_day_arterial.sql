{{ config(materialized='view') }}

-- Purpose: Stage and clean arterial blood gas values for the first 24h of ICU stay

with stg_spo2 as (
    select
        ce.subject_id,
        ce.hadm_id,
        ce.icustay_id,
        ce.charttime,
        max(case when ce.valuenum <= 0 or ce.valuenum > 100 then null else ce.valuenum end) as spo2
    from {{ ref('stg_chartevents') }} ce
    where ce.itemid in (646, 220277)
    group by ce.subject_id, ce.hadm_id, ce.icustay_id, ce.charttime
),

stg_fio2 as (
    select
        ce.subject_id,
        ce.hadm_id,
        ce.icustay_id,
        ce.charttime,
        max(
            case
                when ce.itemid = 223835 then
                    case
                        when ce.valuenum > 0 and ce.valuenum <= 1 then ce.valuenum * 100
                        when ce.valuenum > 1 and ce.valuenum < 21 then null
                        when ce.valuenum >= 21 and ce.valuenum <= 100 then ce.valuenum
                        else null
                    end
                when ce.itemid in (3420, 3422) then ce.valuenum
                when ce.itemid = 190 and ce.valuenum > 0.20 and ce.valuenum < 1 then ce.valuenum * 100
                else null
            end
        ) as fio2_chartevents
    from {{ ref('stg_chartevents') }} ce
    where ce.itemid in (3420, 190, 223835, 3422)
      and (ce.error is null or ce.error = 0)
    group by ce.subject_id, ce.hadm_id, ce.icustay_id, ce.charttime
),

stg2 as (
    select
        bg.*,
        row_number() over (partition by bg.icustay_id, bg.charttime order by s1.charttime desc) as lastrow_spo2,
        s1.spo2
    from {{ ref('stg_blood_gas_first_day') }} bg
    left join stg_spo2 s1
        on bg.icustay_id = s1.icustay_id
        and s1.charttime >= DATETIME_SUB(bg.charttime, INTERVAL '2' HOUR)
        and s1.charttime <= bg.charttime
    where bg.po2 is not null
),

stg3 as (
    select
        bg.*,
        row_number() over (partition by bg.icustay_id, bg.charttime order by s2.charttime desc) as lastrow_fio2,
        s2.fio2_chartevents,
        -- Specimen prediction model
        1 / (1 + exp(-(
            -0.02544
            + 0.04598 * po2
            + coalesce(-0.15356 * spo2, -0.15356 * 97.49420 + 0.13429)
            + coalesce(0.00621 * fio2_chartevents, 0.00621 * 51.49550 - 0.24958)
            + coalesce(0.10559 * hemoglobin, 0.10559 * 10.32307 + 0.05954)
            + coalesce(0.13251 * so2, 0.13251 * 93.66539 - 0.23172)
            + coalesce(-0.01511 * pco2, -0.01511 * 42.08866 - 0.01630)
            + coalesce(0.01480 * fio2, 0.01480 * 63.97836 - 0.31142)
            + coalesce(-0.00200 * aado2, -0.00200 * 442.21186 - 0.01328)
            + coalesce(-0.03220 * bicarbonate, -0.03220 * 22.96894 - 0.06535)
            + coalesce(0.05384 * totalco2, 0.05384 * 24.72632 - 0.01405)
            + coalesce(0.08202 * lactate, 0.08202 * 3.06436 + 0.06038)
            + coalesce(0.10956 * ph, 0.10956 * 7.36233 - 0.00617)
            + coalesce(0.00848 * o2flow, 0.00848 * 7.59362 - 0.35803)
        ))) as specimen_prob
    from stg2 bg
    left join stg_fio2 s2
        on bg.icustay_id = s2.icustay_id
        and s2.charttime between DATETIME_SUB(bg.charttime, INTERVAL '4' HOUR) and bg.charttime
    where bg.lastrow_spo2 = 1
)

select
    subject_id,
    hadm_id,
    icustay_id,
    charttime,
    specimen,
    case
        when specimen is not null then specimen
        when specimen_prob > 0.75 then 'ART'
        else null
    end as specimen_pred,
    specimen_prob,

    -- oxygen related parameters
    so2, spo2, po2, pco2, fio2_chartevents, fio2, aado2,

    -- calculated aado2
    case
        when po2 is not null and pco2 is not null and coalesce(fio2, fio2_chartevents) is not null
            then (coalesce(fio2, fio2_chartevents)/100) * (760 - 47) - (pco2/0.8) - po2
        else null
    end as aado2_calc,

    -- calculated pao2fio2
    case
        when po2 is not null and coalesce(fio2, fio2_chartevents) is not null
            then 100 * po2 / (coalesce(fio2, fio2_chartevents))
        else null
    end as pao2fio2,

    -- acid-base
    ph, baseexcess, bicarbonate, totalco2,

    -- blood count
    hematocrit, hemoglobin, carboxyhemoglobin, methemoglobin,

    -- chemistry
    chloride, calcium, temperature, potassium, sodium, lactate, glucose,

    -- ventilation
    intubated, tidalvolume, ventilationrate, ventilator, peep, o2flow, requiredo2

from stg3
where lastrow_fio2 = 1
  and (specimen = 'ART' or specimen_prob > 0.75)
