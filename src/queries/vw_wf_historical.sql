WITH
    date_range AS (
        SELECT
            DATE (FORMAT_TIMESTAMP ('%Y-%m-%d', day)) AS Appointment_Day
        FROM
            UNNEST (
                GENERATE_DATE_ARRAY (
                    '2022-01-01',
                    DATE_SUB (
                        DATE_TRUNC (CURRENT_DATE, DAY),
                        INTERVAL 1 DAY
                    ),
                    INTERVAL 1 DAY
                )
            ) AS day
    ),
    historical_values AS (
        SELECT DISTINCT
            DATE_TRUNC(Appointment_Timestamp, DAY) AS Appointment_Day,
            District,
            Product,
            CASE 
                WHEN Product_Grp LIKE "%Unmanaged%" THEN "Unmanaged"
                WHEN Product_Grp LIKE "%Managed" THEN "Managed"
                ELSE "OTHER"
            END Product_Grp,
            Technology,
            Work_Order_Action,
            Work_Order_Action_Grp,
            Work_Force,
            SUM(SWT) AS SWT
        FROM
            `{project}.b2b_wf_prediction.bq_wf_historical`
        WHERE
            Work_Order_Action_Grp NOT IN (
                'EXCLUDE',
                'PROJECT (EXCLUDE)',
                'PROJECT',
                'OTHER'
            )
            AND Work_Force NOT IN ('QX', 'Go Sales')
            AND Product_Grp NOT IN ('OTHER')
        GROUP BY
            DATE_TRUNC(Appointment_Timestamp, DAY),
            District, Product,
            Product_Grp, Technology,
            Work_Order_Action, 
            Work_Order_Action_Grp,
            Work_Force
    ), 
    district_tier AS (
        SELECT 
            district,
            Region_Type,
        FROM `{project}.b2b_wf_prediction.bq_wf_districts`
        GROUP BY
            district, Region_Type
    )


SELECT
    dr.Appointment_Day,
    h.District,
    dt.Region_Type,
    h.Product,
    h.Product_Grp,
    h.Technology,
    h.Work_Order_Action,
    h.Work_Order_Action_Grp,
    h.Work_Force,
    COALESCE(h.SWT, 0) AS SWT
FROM
    date_range dr
    CROSS JOIN historical_values h
    LEFT JOIN district_tier dt ON dt.district = h.District
