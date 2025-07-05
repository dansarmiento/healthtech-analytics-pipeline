{{ config(materialized='view') }}

-- Purpose: Extract and clean structured fields from echocardiography notes.

select
    ne.row_id as row_id,
    ne.subject_id as subject_id,
    ne.hadm_id as hadm_id,
    ne.chartdate as chartdate,

    -- Reconstruct charttime using regex on text if present
    parse_datetime(
        '%Y-%m-%d%H:%M:%S',
        format_date('%Y-%m-%d', ne.chartdate) ||
        regexp_extract(ne.text, 'Date/Time: .+? at ([0-9]+:[0-9]{2})') ||
        ':00'
    ) as charttime,

    regexp_extract(ne.text, 'Indication: (.*?)\n') as indication,

    -- Numeric values (removes de-identified placeholders)
    cast(regexp_extract(ne.text, 'Height: \\x28in\\x29 ([0-9]+)') as numeric) as height_in,
    cast(regexp_extract(ne.text, 'Weight \\x28lb\\x29: ([0-9]+)\n') as numeric) as weight_lb,
    cast(regexp_extract(ne.text, 'BSA \\x28m2\\x29: ([0-9]+) m2\n') as numeric) as bsa_m2,

    regexp_extract(ne.text, 'BP \\x28mm Hg\\x29: (.+)\n') as bp_raw,
    cast(regexp_extract(ne.text, 'BP \\x28mm Hg\\x29: ([0-9]+)/[0-9]+?\n') as numeric) as bp_sys,
    cast(regexp_extract(ne.text, 'BP \\x28mm Hg\\x29: [0-9]+/([0-9]+?)\n') as numeric) as bp_dias,
    cast(regexp_extract(ne.text, 'HR \\x28bpm\\x29: ([0-9]+?)\n') as numeric) as hr,

    regexp_extract(ne.text, 'Status: (.*?)\n') as status,
    regexp_extract(ne.text, 'Test: (.*?)\n') as test,
    regexp_extract(ne.text, 'Doppler: (.*?)\n') as doppler,
    regexp_extract(ne.text, 'Contrast: (.*?)\n') as contrast,
    regexp_extract(ne.text, 'Technical Quality: (.*?)\n') as technical_quality

from {{ ref('stg_noteevents') }} ne
where ne.category = 'Echo'
