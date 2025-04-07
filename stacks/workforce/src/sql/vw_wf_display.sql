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
    CAST(h.Appointment_Month as DATE) as Appointment_Month,
    EXTRACT(
        YEAR
        FROM
            CAST(h.Appointment_Month as DATE)
    ) as Year,
    EXTRACT(
        MONTH
        FROM
            CAST(h.Appointment_Month as DATE)
    ) as Month,
    h.District,
    h.Product,
    h.Technology,
    h.Work_Order_Action, -- Also called "Job Type" 
    h.Work_Force,
    (
        CASE
            WHEN h.District IN (
                'FFB - Calgary',
                'FFB - Edmonton',
                'FFB - LML Metro',
                'FFB - LML Valley'
            ) THEN 'Tier 1'
            WHEN h.District IN (
                'FFB - Fort McMurray',
                'FFB - Fort St John Dawson Creek',
                'FFB - Grande Prairie',
                'FFB - Kamloops',
                'FFB - Lethbridge',
                'FFB - Manitoba',
                'FFB - Medicine Hat',
                'FFB - Nova Scotia',
                'FFB - Okanagan',
                'FFB - Ontario',
                'FFB - Prince George',
                'FFB - Red Deer',
                'FFB - Saskatchewan',
                'FFB - Vancouver Island North',
                'FFB - Vancouver Island South',
                'Montréal - Québec'
            ) THEN 'Tier 2'
            WHEN h.District IN (
                'FFB - Athabasca Lac La Biche',
                'FFB - Bonnyville Cold Lake',
                'FFB - Edson Hinton',
                'FFB - High Level',
                'FFB - Kootenay Cranbrook',
                'FFB - Kootenay Golden',
                'FFB - Kootenay Nelson',
                'FFB - Lloydminster Wainright',
                'FFB - New Brunswick',
                'FFB - Newfoundland',
                'FFB - Northwest Territories and Nunavut',
                'FFB - Peace River High Prairie',
                'FFB - Prince Edward Island',
                'FFB - Sea to Sky',
                'FFB - Slave Lake',
                'FFB - Sunshine Coast',
                'FFB - Terrace Smithers',
                'FFB - Whitecourt',
                'FFB - Williams Lake 100 Mile House',
                'FFB - Yukon',
                'BSL-Gaspésie',
                'Côte-Nord - Nord du Québec'
            ) THEN 'Tier 3'
        END
    ) AS Region_Type,
    h.SWT,
    "Actual" AS Forecast_Date,
    "Actual" AS SWT_Type,
    h.SWT AS SWT_Actual,
    h.AWT AS AWT,
    null AS SWT_Predicted,
    FALSE AS Is_Latest_Forecast
FROM
    `{project}.b2b_wf_prediction.vw_wf_historical` AS h
WHERE
    h.Appointment_Month >= DATE_TRUNC ("2022-01-01", MONTH)
    AND h.Appointment_Month < DATE_TRUNC (DATE_SUB(CURRENT_DATE, INTERVAL 11 DAY), MONTH)
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
    CAST(p.Appointment_Month as DATE) AS Appointment_Month,
    EXTRACT(
        YEAR
        FROM
            CAST(p.Appointment_Month AS DATE)
    ) AS Year,
    EXTRACT(
        MONTH
        FROM
            CAST(p.Appointment_Month AS DATE)
    ) AS Month,
    p.District,
    p.Product,
    p.Technology,
    p.Work_Order_Action,
    p.Work_Force,
    (
        CASE
            WHEN p.District IN (
                'FFB - Calgary',
                'FFB - Edmonton',
                'FFB - LML Metro',
                'FFB - LML Valley'
            ) THEN 'Tier 1'
            WHEN p.District IN (
                'FFB - Fort McMurray',
                'FFB - Fort St John Dawson Creek',
                'FFB - Grande Prairie',
                'FFB - Kamloops',
                'FFB - Lethbridge',
                'FFB - Manitoba',
                'FFB - Medicine Hat',
                'FFB - Nova Scotia',
                'FFB - Okanagan',
                'FFB - Ontario',
                'FFB - Prince George',
                'FFB - Red Deer',
                'FFB - Saskatchewan',
                'FFB - Vancouver Island North',
                'FFB - Vancouver Island South',
                'Montréal - Québec'
            ) THEN 'Tier 2'
            WHEN p.District IN (
                'FFB - Athabasca Lac La Biche',
                'FFB - Bonnyville Cold Lake',
                'FFB - Edson Hinton',
                'FFB - High Level',
                'FFB - Kootenay Cranbrook',
                'FFB - Kootenay Golden',
                'FFB - Kootenay Nelson',
                'FFB - Lloydminster Wainright',
                'FFB - New Brunswick',
                'FFB - Newfoundland',
                'FFB - Northwest Territories and Nunavut',
                'FFB - Peace River High Prairie',
                'FFB - Prince Edward Island',
                'FFB - Sea to Sky',
                'FFB - Slave Lake',
                'FFB - Sunshine Coast',
                'FFB - Terrace Smithers',
                'FFB - Whitecourt',
                'FFB - Williams Lake 100 Mile House',
                'FFB - Yukon',
                'BSL-Gaspésie',
                'Côte-Nord - Nord du Québec'
            ) THEN 'Tier 3'
        END
    ) AS Region_Type,
    p.predicted_SWT.value AS SWT,
    CAST(p.Forecast_Date AS STRING) AS Forecast_Date,
    "Predicted" AS SWT_Type,
    null AS SWT_Actual,
    null AS AWT,
    p.predicted_SWT.value AS SWT_Predicted,
    CASE 
        WHEN p.Forecast_Date = FIRST_VALUE(p.Forecast_Date) OVER (PARTITION BY 1 ORDER BY p.Forecast_Date DESC)
            THEN TRUE
        ELSE 
            FALSE 
    END AS Is_Latest_Forecast
FROM
    `bi-stg-aaaie-pr-750ff7.b2b_wf_prediction.bq_wf_forecast` AS p