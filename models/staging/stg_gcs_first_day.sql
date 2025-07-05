{{ config(materialized='view') }}

-- Purpose: Extract Glasgow Coma Scale (GCS) for each icustay_id during the first 24 hours of ICU stay

with base as (
    select
        pvt.icustay_id,
        pvt.charttime,

        -- Standardized variable names
        max(case when pvt.itemid = 454 then pvt.valuenum else null end) as gcsmotor,
        max(case when pvt.itemid = 723 then pvt.valuenum else null end) as gcsverbal,
        max(case when pvt.itemid = 184 then pvt.valuenum else null end) as gcseyes,

        case
            when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0 then 1
            else 0
        end as endotrachflag,

        row_number() over (partition by pvt.icustay_id order by pvt.charttime asc) as rn

    from (
        select
            ce.icustay_id,
            -- Standardize itemids across systems
            case
                when ce.itemid in (723, 223900) then 723
                when ce.itemid in (454, 223901) then 454
                when ce.itemid in (184, 220739) then 184
                else ce.itemid
            end as itemid,
            -- Handle intubated (special values)
            case
                when ce.itemid = 723 and ce.value = '1.0 ET/Trach' then 0
                when ce.itemid = 223900 and ce.value = 'No Response-ETT' then 0
                else ce.valuenum
            end as valuenum,
            ce.charttime
        from {{ ref('stg_chartevents') }} ce
        inner join {{ ref('stg_icustays') }} icu
            on ce.icustay_id = icu.icustay_id
        where
            ce.itemid in (184, 454, 723, 223900, 223901, 220739)
            and ce.charttime between icu.intime and DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
            and (ce.error is null or ce.error = 0)
    ) pvt
    group by pvt.icustay_id, pvt.charttime
),

gcs_with_prev as (
    select
        b.*,
        b2.gcsverbal as gcsverbal_prev,
        b2.gcsmotor as gcsmotor_prev,
        b2.gcseyes as gcseyes_prev,
        -- GCS computation with special handling for sedation/intubation
        case
            when b.gcsverbal = 0 then 15
            when b.gcsverbal is null and b2.gcsverbal = 0 then 15
            when b2.gcsverbal = 0 then
                coalesce(b.gcsmotor, 6)
                + coalesce(b.gcsverbal, 5)
                + coalesce(b.gcseyes, 4)
            else
                coalesce(b.gcsmotor, coalesce(b2.gcsmotor, 6))
                + coalesce(b.gcsverbal, coalesce(b2.gcsverbal, 5))
                + coalesce(b.gcseyes, coalesce(b2.gcseyes, 4))
        end as gcs
    from base b
    left join base b2
        on b.icustay_id = b2.icustay_id
        and b.rn = b2.rn + 1
        and b2.charttime > DATETIME_SUB(b.charttime, INTERVAL '6' HOUR)
),

gcs_final as (
    select
        g.*,
        row_number() over (partition by g.icustay_id order by g.gcs) as is_min_gcs
    from gcs_with_prev g
)

select
    icu.subject_id,
    icu.hadm_id,
    icu.icustay_id,
    gcs_final.gcs as mingcs,
    coalesce(gcs_final.gcsmotor, gcs_final.gcsmotor_prev) as gcsmotor,
    coalesce(gcs_final.gcsverbal, gcs_final.gcsverbal_prev) as gcsverbal,
    coalesce(gcs_final.gcseyes, gcs_final.gcseyes_prev) as gcseyes,
    gcs_final.endotrachflag
from {{ ref('stg_icustays') }} icu
left join gcs_final
    on icu.icustay_id = gcs_final.icustay_id and gcs_final.is_min_gcs = 1
