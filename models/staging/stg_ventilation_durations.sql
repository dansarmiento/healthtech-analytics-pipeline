{{ config(materialized='view') }}

-- Purpose: Identify and aggregate distinct mechanical ventilation events per ICU stay.

with vd0 as (
    select
        vc.icustay_id,
        case
            when vc.mechvent = 1 then
                lag(vc.charttime, 1) over (partition by vc.icustay_id, vc.mechvent order by vc.charttime)
            else null
        end as charttime_lag,
        vc.charttime,
        vc.mechvent,
        vc.oxygentherapy,
        vc.extubated,
        vc.selfextubated
    from {{ ref('stg_ventilation_classification') }} vc
),

vd1 as (
    select
        icustay_id,
        charttime_lag,
        charttime,
        mechvent,
        oxygentherapy,
        extubated,
        selfextubated,
        -- Calculate duration since last event
        case
            when mechvent = 1 then
                datetime_diff(charttime, charttime_lag, 'MINUTE') / 60
            else null
        end as ventduration,
        lag(extubated, 1) over (
            partition by icustay_id, case when mechvent=1 or extubated=1 then 1 else 0 end
            order by charttime
        ) as extubated_lag,
        -- Identify new ventilation event
        case
            when lag(extubated, 1) over (
                partition by icustay_id, case when mechvent=1 or extubated=1 then 1 else 0 end
                order by charttime
            ) = 1 then 1
            when mechvent = 0 and oxygentherapy = 1 then 1
            when charttime > datetime_add(charttime_lag, interval '8' hour) then 1
            else 0
        end as newvent
    from vd0
),

vd2 as (
    select
        vd1.*,
        -- Cumulative sum of new ventilation events to create unique event numbers
        case when mechvent = 1 or extubated = 1 then
            sum(newvent) over (partition by icustay_id order by charttime)
        else null end as ventnum
    from vd1
)

select
    icustay_id,
    row_number() over (partition by icustay_id order by ventnum) as ventnum,
    min(charttime) as starttime,
    max(charttime) as endtime,
    datetime_diff(max(charttime), min(charttime), 'MINUTE') / 60 as duration_hours
from vd2
group by icustay_id, ventnum
having min(charttime) != max(charttime)
   and max(mechvent) = 1
