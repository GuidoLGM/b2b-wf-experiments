SELECT
    Series_Identifier,
    Appointment_Month,
    EXTRACT(
        YEAR
        FROM
            Appointment_Month
    ) AS Year,
    EXTRACT(
        MONTH
        FROM
            Appointment_Month
    ) AS Month,
    Region_Type AS `Region Type`, -- Tier
    -- Combine Region (not a priority now; used for supply)
    District,
    Product,
    Work_Order_Action AS `Job Type`,
    Work_Force AS `Workforce`, -- (not a priority now; used for supply)
    Technology,
    SWT,
    SWT_Type
FROM
    `{project}.b2b_wf_prediction.vw_wf_display`
ORDER BY
    Appointment_Month DESC