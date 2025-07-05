{{ config(materialized='view') }}

-- Purpose: Aggregate urine output per icustay_id for the first 24 hours of ICU stay

WITH output_events_filtered AS (
    SELECT
        oe.subject_id,
        oe.hadm_id,
        oe.icustay_id,
        oe.itemid,
        oe.value,
        oe.charttime
    FROM {{ ref('stg_outputevents') }} oe
    WHERE oe.itemid IN (
        40055, 43175, 40069, 40094, 40715, 40473, 40085, 40057, 40056, 40405, 40428, 40086, 40096, 40651,
        226559, 226560, 226561, 226584, 226563, 226564, 226565, 226567, 226557, 226558, 227488, 227489
    )
),

first_day_urine AS (
    SELECT
        icu.subject_id,
        icu.hadm_id,
        icu.icustay_id,
        oe.itemid,
        oe.value,
        oe.charttime
    FROM {{ ref('stg_icustays') }} icu
    LEFT JOIN output_events_filtered oe
        ON icu.subject_id = oe.subject_id
        AND icu.hadm_id = oe.hadm_id
        AND icu.icustay_id = oe.icustay_id
        AND oe.charttime BETWEEN icu.intime AND DATETIME_ADD(icu.intime, INTERVAL '1' DAY)
)

SELECT
    subject_id,
    hadm_id,
    icustay_id,
    SUM(
        CASE
            WHEN itemid = 227488 AND value > 0 THEN -1 * value  -- GU irrigant as negative volume
            ELSE value
        END
    ) AS urineoutput
FROM first_day_urine
GROUP BY subject_id, hadm_id, icustay_id
