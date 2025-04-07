BEGIN
INSERT INTO
    `{project}.b2b_wf_prediction.bq_wf_predictions`
SELECT
    *
FROM
    (
        WITH
            Categorical_Columns AS (
                SELECT DISTINCT
                    District,
                    Product,
                    Technology,
                    Work_Order_Action,
                    Work_Force,
                FROM
                    `{project}.b2b_wf_prediction.vw_wf_historical`
                WHERE
                    District IS NOT NULL
                    AND Product IS NOT NULL
                    AND Technology IS NOT NULL
                    AND Work_Order_Action IS NOT NULL
                    AND Work_Force IS NOT NULL
            ),
            Date_Range AS (
                SELECT
                    (
                        DATE (DATE_TRUNC (_end_period_dt, MONTH)) + INTERVAL i MONTH
                    ) AS Appointment_Month
                FROM
                    -- TODO: change this to make adjustable forecast horizon (horizon - 1)
                    UNNEST (GENERATE_ARRAY (0, 17)) AS i
            )
        SELECT
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
            CAST(d.Appointment_Month as DATE),
            c.District,
            c.Product,
            c.Technology,
            c.Work_Order_Action,
            c.Work_Force,
            NULL AS SWT,
            "predicted" as SWT_Type,
        FROM
            Categorical_Columns AS c,
            Date_Range AS d
        UNION ALL
        SELECT
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
            CAST(h.Appointment_Month as DATE),
            h.District,
            h.Product,
            h.Technology,
            h.Work_Order_Action,
            h.Work_Force,
            h.SWT,
            "actual" as SWT_Type,
        FROM
            `{project}.b2b_wf_prediction.vw_wf_historical` AS h
        WHERE
            h.Appointment_Month >= DATE_TRUNC (_start_period_dt, MONTH)
            AND h.Appointment_Month < DATE_TRUNC (_end_period_dt, MONTH)
            AND District IS NOT NULL
            AND Product IS NOT NULL
            AND Technology IS NOT NULL
            AND Work_Order_Action IS NOT NULL
            AND Work_Force IS NOT NULL
    );

END