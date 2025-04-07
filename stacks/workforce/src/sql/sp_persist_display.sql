BEGIN

DELETE FROM `{project}.b2b_wf_prediction.bq_wf_display` WHERE TRUE;

INSERT INTO `{project}.b2b_wf_prediction.bq_wf_display`

SELECT Forecast_Date,
    Appointment_Month,
    District,
    Region_Type,
    Product,
    Product_Grp,
    Technology,
    Work_Order_Action,
    Work_Order_Action_Grp,
    Work_Force,
    AWT_Historical,
    SWT_Historical,
    SWT_Predicted
FROM (
       WITH region_mapping AS (
    SELECT district_old as district,
    province,
        region_type
    FROM `{project}.b2b_wf_prediction.bq_wf_districts`
    GROUP BY district,
    province,
        region_type
),
dimensions AS (
    SELECT DISTINCT h.District,
        rm.Region_Type,
        h.Product,
        CASE
            WHEN h.Product_Grp LIKE "%Unmanaged%" THEN "Unmanaged"
            WHEN h.Product_Grp LIKE "%Managed%" THEN "Managed"
            ELSE "OTHER"
        END AS Product_Grp,
        h.Technology,
        h.Work_Order_Action,
        h.Work_Order_Action_Grp,
        h.Work_Force
    FROM `{project}.b2b_wf_prediction.bq_wf_historical` h
        LEFT JOIN region_mapping rm ON h.District = rm.district
    WHERE h.Work_Order_Action_Grp NOT IN ('EXCLUDE', 'PROJECT (EXCLUDE)', 'OTHER')
        AND h.Work_Force NOT IN ('QX', 'Go Sales')
        AND h.Product_Grp NOT IN ('OTHER')
),
date_spine AS (
    SELECT DISTINCT f.Forecast_Date,
        DATE_TRUNC(DATE(d), MONTH) as Appointment_Month
    FROM `bi-stg-aaaie-pr-750ff7.b2b_wf_prediction.bq_wf_forecast` f
        CROSS JOIN UNNEST(
            GENERATE_DATE_ARRAY(
                DATE('2022-01-01'),
                DATE_ADD(
                    DATE_TRUNC(DATE(f.Forecast_Date), MONTH),
                    INTERVAL 17 MONTH
                )
            )
        ) as d
    WHERE d >= DATE('2022-01-01')
),
historical_metrics AS (
    SELECT DATE_TRUNC(Appointment_Timestamp, MONTH) as Appointment_Month,
        District,
        Product,
        Technology,
        Work_Order_Action,
        Work_Force,
        SUM(AWT) as AWT_Historical,
        SUM(SWT) as SWT_Historical
    FROM `{project}.b2b_wf_prediction.bq_wf_historical`
    WHERE DATE_TRUNC(Appointment_Timestamp, MONTH) < DATE_TRUNC(CURRENT_DATE(), MONTH)
    GROUP BY DATE_TRUNC(Appointment_Timestamp, MONTH),
        District,
        Product,
        Technology,
        Work_Order_Action,
        Work_Force
),
special_projects_base AS (
    SELECT Product,
        UPPER(Job_Type) AS Work_Order_Action,
        Region AS District,
        DATE(
            FORMAT_DATE('%Y-%m-01', DATE(PARSE_DATE('%B_%Y', Month)))
        ) AS Appointment_month,
        SUM(Value) AS SWT_Project
    FROM `{project}.b2b_wf_prediction.bq_wf_special_projects` UNPIVOT(
            Value FOR Month IN (
                July_2024,
                August_2024,
                September_2024,
                October_2024,
                November_2024,
                December_2024,
                January_2025,
                February_2025,
                March_2025,
                April_2025,
                May_2025,
                June_2025,
                July_2025,
                August_2025,
                September_2025,
                October_2025,
                November_2025,
                December_2025
            )
        )
    WHERE Value IS NOT NULL
        AND Notes NOT LIKE "%EXCLUDE%"
    GROUP BY Product,
        Region,
        UPPER(Job_Type),
        DATE(
            FORMAT_DATE('%Y-%m-01', DATE(PARSE_DATE('%B_%Y', Month)))
        )
),
special_projects AS (
    SELECT special_projects_base.Product,
        special_projects_base.Work_Order_Action,
        special_projects_base.District,
        rm.region_type AS Region_Type,
        CASE
            WHEN special_projects_base.Product IN ('BUSINESS INTERNET', 'CARRIER ETHERNET', 'MANAGED LAN', 'PRIVATE LINE', 'SD WAN', 'WAN L2_L3', 'WAVELENGTH', 'CPS', 'MPAAS') THEN 'Managed'
            WHEN special_projects_base.Product IN ('BUSINESS CONNECT', 'CENTREX', 'HHM', 'HSIA', 'IPTV', 'NAAS', 'POTS', 'SDS WIFI', 'SECURITY', 'TRUE STATIC IP', 'WHSIA', 'WIFI', 'IOT MODEM') THEN 'Unmanaged'
            ELSE 'Other'
        END AS Product_Grp,
        'N/A' AS Technology,
        'PROJECT' AS Work_Order_Action_Grp,
        'N/A' AS Work_Force,
        Appointment_month,
        SWT_Project
    FROM special_projects_base
        LEFT JOIN region_mapping rm ON rm.district = special_projects_base.District
)
SELECT DATE(ds.Forecast_Date) AS Forecast_Date,
    ds.Appointment_Month,
    d.District,
    d.Region_Type,
    d.Product,
    d.Product_Grp,
    d.Technology,
    d.Work_Order_Action,
    d.Work_Order_Action_Grp,
    d.Work_Force,
    h.AWT_Historical,
    h.SWT_Historical,
    f.predicted_SWT.value as SWT_Predicted
FROM dimensions d
    CROSS JOIN date_spine ds
    LEFT JOIN historical_metrics h ON ds.Appointment_Month = h.Appointment_Month
    AND d.District = h.District
    AND d.Product = h.Product
    AND d.Technology = h.Technology
    AND d.Work_Order_Action = h.Work_Order_Action
    AND d.Work_Force = h.Work_Force
    LEFT JOIN `bi-stg-aaaie-pr-750ff7.b2b_wf_prediction.bq_wf_forecast` f ON ds.Forecast_Date = f.Forecast_Date
    AND ds.Appointment_Month = DATE(f.Appointment_Month)
    AND d.District = f.District
    AND d.Product = f.Product
    AND d.Technology = f.Technology
    AND d.Work_Order_Action = f.Work_Order_Action
    AND d.Work_Force = f.Work_Force
UNION ALL
SELECT DATE(ds.Forecast_Date) AS Forecast_Date,
    ds.Appointment_Month,
    sp.District,
    sp.Region_Type,
    sp.Product,
    sp.Product_Grp,
    sp.Technology,
    sp.Work_Order_Action,
    sp.Work_Order_Action_Grp,
    sp.Work_Force,
    CAST(NULL AS FLOAT64) as AWT_Historical,
    CAST(NULL AS FLOAT64) as SWT_Historical,
    sp.SWT_Project as SWT_Predicted
FROM special_projects sp
    CROSS JOIN (
        SELECT DISTINCT Forecast_Date,
            Appointment_Month
        FROM date_spine
    ) ds
WHERE ds.Appointment_Month = sp.Appointment_month
    AND sp.SWT_Project > 0
    AND sp.Appointment_month BETWEEN DATE_SUB(DATE (ds.Forecast_Date), INTERVAL 1 MONTH)
    AND DATE_ADD(DATE (ds.Forecast_Date), INTERVAL 17 MONTH)
);
END