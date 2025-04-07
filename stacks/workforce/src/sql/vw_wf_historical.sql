WITH
    date_range AS (
        SELECT
            DATE (FORMAT_TIMESTAMP ('%Y-%m-%d', month)) AS Appointment_Month
        FROM
            UNNEST (
                GENERATE_DATE_ARRAY (
                    '2022-01-01',
                    DATE_SUB (
                        DATE_TRUNC (CURRENT_DATE, MONTH),
                        INTERVAL 1 MONTH
                    ),
                    INTERVAL 1 MONTH
                )
            ) AS month
    ),
    dimensions AS (
        SELECT DISTINCT
            CONCAT (
                COALESCE(District, 'None'),
                ' ',
                COALESCE(Product, 'None'),
                ' ',
                COALESCE(Technology, 'None'),
                ' ',
                COALESCE(Work_Order_Action, 'None'),
                ' ',
                COALESCE(Work_Force, 'None')
            ) as Series_Identifier,
            District,
            Product,
            Technology,
            Work_Order_Action,
            Work_Force,
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
    ),
    historical_values AS (
        SELECT
            DATE_TRUNC (Appointment_Timestamp, MONTH) AS Appointment_Month,
            CONCAT (
                COALESCE(District, 'None'),
                ' ',
                COALESCE(Product, 'None'),
                ' ',
                COALESCE(Technology, 'None'),
                ' ',
                COALESCE(Work_Order_Action, 'None'),
                ' ',
                COALESCE(Work_Force, 'None')
            ) as Series_Identifier,
            District,
            Product,
            Technology,
            Work_Order_Action,
            Work_Force,
            SUM(SWT) AS SWT,
            SUM(AWT) AS AWT
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
            DATE_TRUNC (Appointment_Timestamp, MONTH),
            District,
            Product,
            Technology,
            Work_Order_Action,
            Work_Force
    )
SELECT
    dr.Appointment_Month,
    d.Series_Identifier,
    d.District,
    d.Product,
    d.Technology,
    d.Work_Order_Action,
    d.Work_Force,
    COALESCE(h.SWT, 0) AS SWT,
    COALESCE(h.AWT, 0) AS AWT
FROM
    date_range dr
    CROSS JOIN dimensions d
    LEFT JOIN historical_values h ON dr.Appointment_Month = h.Appointment_Month
    AND d.Series_Identifier = h.Series_Identifier