BEGIN
DELETE FROM `{project}.b2b_wf_prediction.bq_wf_marketing` WHERE TRUE;
INSERT INTO `{project}.b2b_wf_prediction.bq_wf_marketing`
SELECT Appointment_Month,
    Product,
    Actual_RGU,
    Forecasted_RGU,
    Adjusted_RGU
FROM (
        WITH current_date AS (
            SELECT EXTRACT(
                    YEAR
                    FROM CURRENT_DATE()
                ) as current_year,
                EXTRACT(
                    MONTH
                    FROM CURRENT_DATE()
                ) as current_month
        ),
        date_spine AS (
            SELECT DISTINCT Year,
                Month
            FROM `{project}.b2b_wf_prediction.bq_wf_marketing_sheet`
        ),
        products AS (
            SELECT DISTINCT Product
            FROM `{project}.b2b_wf_prediction.bq_wf_marketing_sheet`
        ),
        base_dates_products AS (
            SELECT Year,
                Month,
                Product
            FROM date_spine
                CROSS JOIN products
        )
        SELECT CAST(
                CONCAT(
                    b.Year,
                    '-',
                    LPAD(CAST(b.Month AS STRING), 2, '0'),
                    '-01'
                ) AS DATE
            ) AS Appointment_Month,
            b.Product,
            CASE
                WHEN (
                    b.Year < (
                        SELECT current_year
                        FROM current_date
                    )
                )
                OR (
                    b.Year = (
                        SELECT current_year
                        FROM current_date
                    )
                    AND b.Month <= (
                        SELECT current_month
                        FROM current_date
                    )
                ) THEN COALESCE(
                    LAG(a.Adjusted_Actual) OVER (
                        PARTITION BY b.Product
                        ORDER BY b.Year,
                            b.Month
                    ),
                    0
                )
                ELSE 0
            END AS Actual_RGU,
            CASE
                WHEN (
                    b.Year = (
                        SELECT current_year
                        FROM current_date
                    )
                    AND b.Month = (
                        SELECT current_month
                        FROM current_date
                    )
                )
                OR (
                    b.Year > (
                        SELECT current_year
                        FROM current_date
                    )
                )
                OR (
                    b.Year = (
                        SELECT current_year
                        FROM current_date
                    )
                    AND b.Month > (
                        SELECT current_month
                        FROM current_date
                    )
                ) THEN COALESCE(a.Original_Actual, 0)
                ELSE 0
            END AS Forecasted_RGU,
            CASE
                WHEN (
                    b.Year = (
                        SELECT current_year
                        FROM current_date
                    )
                    AND b.Month = (
                        SELECT current_month
                        FROM current_date
                    )
                )
                OR (
                    b.Year > (
                        SELECT current_year
                        FROM current_date
                    )
                )
                OR (
                    b.Year = (
                        SELECT current_year
                        FROM current_date
                    )
                    AND b.Month > (
                        SELECT current_month
                        FROM current_date
                    )
                ) THEN COALESCE(a.Adjusted_Actual, 0)
                ELSE 0
            END AS Adjusted_RGU
        FROM base_dates_products b
            LEFT JOIN `{project}.b2b_wf_prediction.bq_wf_marketing_sheet` a ON b.Year = a.Year
            AND b.Month = a.Month
            AND b.Product = a.Product
            AND a.RGU_Type = 'Gross Adds'
        ORDER BY b.Year,
            b.Month,
            b.Product
);
END