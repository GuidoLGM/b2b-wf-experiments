WITH all_districts AS (
  SELECT DISTINCT district_old AS district,
    region_type,
    province
  FROM `bi-stg-aaaie-pr-750ff7.b2b_wf_prediction.bq_wf_districts`
  WHERE district_old IS NOT NULL
),
efficiencies_with_uid AS (
  SELECT ROW_NUMBER() OVER (
      ORDER BY Products_Impacted,
        Job_Types_Impacted,
        Region_Impacted
    ) AS efficiency_uid,
    *
  FROM `bi-stg-aaaie-pr-750ff7.b2b_wf_prediction.bq_wf_efficiencies`
),
expanded_districts AS (
  SELECT eff.efficiency_uid,
    eff.Products_Impacted AS product_impacted,
    eff.Job_Types_Impacted AS job_type_impacted,
    CASE
      WHEN eff.Region_Impacted = 'All' THEN d.district
      WHEN eff.Region_Impacted LIKE 'Tier%' THEN CASE
        WHEN d.region_type = eff.Region_Impacted THEN d.district
        ELSE NULL
      END
      WHEN eff.Region_Impacted IN (
        'AB',
        'BC',
        'MB',
        'NB',
        'NL',
        'NS',
        'ON',
        'QC',
        'PE',
        'SK',
        'YT'
      ) THEN CASE
        WHEN d.province = eff.Region_Impacted THEN d.district
        ELSE NULL
      END
      WHEN eff.Region_Impacted = 'NU & NT' THEN CASE
        WHEN d.province IN ('NT', 'NU') THEN d.district
        ELSE NULL
      END
      ELSE CASE
        WHEN d.district = eff.Region_Impacted THEN d.district
        ELSE NULL
      END
    END AS district
  FROM efficiencies_with_uid eff
    CROSS JOIN all_districts d
  WHERE CASE
      WHEN eff.Region_Impacted = 'All' THEN TRUE
      WHEN eff.Region_Impacted LIKE 'Tier%' THEN d.region_type = eff.Region_Impacted
      WHEN eff.Region_Impacted IN (
        'AB',
        'BC',
        'MB',
        'NB',
        'NL',
        'NS',
        'ON',
        'QC',
        'PE',
        'SK',
        'YT'
      ) THEN d.province = eff.Region_Impacted
      WHEN eff.Region_Impacted = 'NU & NT' THEN d.province IN ('NT', 'NU')
      ELSE d.district = eff.Region_Impacted
    END
    AND district IS NOT NULL
),
product_types AS (
  SELECT ed.*,
    ad.province,
    CASE
      WHEN product_impacted = 'Managed'
      AND ad.province != 'QC' THEN [ 
        'BUSINESS INTERNET', 'CARRIER ETHERNET', 
        'MANAGED LAN', 'PRIVATE LINE', 'SD WAN', 
        'WAN L2_L3', 'WAVELENGTH']
      WHEN product_impacted = 'Managed'
      AND ad.province = 'QC' THEN [
         'CPS', 'BUSINESS CONNECT', 'CARRIER ETHERNET',
          'MANAGED LAN', 'MPAAS', 'NAAS', 'PRIVATE LINE',
          'SD WAN', 'WAN L2_L3'
      ]
      WHEN product_impacted = 'Unmanaged'
      AND ad.province != 'QC' THEN [
        'BUSINESS CONNECT', 'CENTREX', 'HHM', 
        'HSIA', 'IPTV', 'MPAAS', 'NAAS', 'POTS', 
        'SDS WIFI', 'SECURITY', 'TRUE STATIC IP', 
        'WHSIA', 'WIFI'
      ]
      WHEN product_impacted = 'Unmanaged'
      AND ad.province = 'QC' THEN [
        'IOT MODEM', 'HSIA', 'POTS', 'SECURITY', 'WIFI'
      ]
      WHEN product_impacted = 'All' THEN [
        'BUSINESS CONNECT', 'HSIA', 'IPTV', 'POTS', 'SDS WIFI', 'SECURITY', 
        'WIFI', 'CENTREX', 'MPAAS', 'NAAS', 'HHM', 'WHSIA', 'TRUE STATIC IP', 
        'BUSINESS INTERNET', 'CARRIER ETHERNET', 'WAN L2_L3', 'PRIVATE LINE', 
        'MANAGED LAN', 'WAVELENGTH', 'SD WAN', 'IOT MODEM', 'CPS'
      ]
      ELSE [product_impacted]
    END AS products
  FROM expanded_districts ed
    JOIN all_districts ad ON ed.district = ad.district
),
expanded_products AS (
  SELECT pt.efficiency_uid,
    p AS product,
    pt.job_type_impacted,
    pt.district
  FROM product_types pt
    CROSS JOIN UNNEST(pt.products) AS p
  WHERE p != 'OTHER'
),
expanded_job_types AS (
  SELECT ep.efficiency_uid,
    ep.product,
    UPPER(j) as job_type,
    ep.district
  FROM expanded_products ep
    CROSS JOIN UNNEST(
      CASE
        WHEN ep.job_type_impacted = 'All (Excluding Projects)' THEN ['INSTALL', 'PREFIELD', 'RISER', 'PRESTAGE', 'REPAIR', 'CHANGE', 'MOVE', 'OUT']
        ELSE [ep.job_type_impacted]
      END
    ) AS j
),
corrections AS (
  SELECT f.Forecast_Date,
    ejt.efficiency_uid,
    f.Appointment_Month,
    SUM(f.predicted_SWT.value) as total_value
  FROM expanded_job_types ejt
    CROSS JOIN (
      SELECT DISTINCT Forecast_Date
      FROM `bi-stg-aaaie-pr-750ff7.b2b_wf_prediction.bq_wf_forecast`
    ) fd
    LEFT JOIN `bi-stg-aaaie-pr-750ff7.b2b_wf_prediction.bq_wf_forecast` f ON f.District = ejt.district
    AND f.Product = ejt.product
    AND f.Work_Order_Action = ejt.job_type
    AND f.Forecast_Date = fd.Forecast_Date
  WHERE f.SWT_Type = 'predicted'
  GROUP BY f.Forecast_Date,
    ejt.efficiency_uid,
    f.Appointment_Month
),
unpivoted_efficiencies AS (
  SELECT efficiency_uid,
    Products_Impacted,
    Job_Types_Impacted,
    Region_Impacted,
    Type_of_Reduction,
    PARSE_DATE(
      '%b_%Y',
      REGEXP_REPLACE(month_col, r'([A-Za-z]+)_(\d{4})', r'\1_\2')
    ) as appointment_month,
    CAST(value AS FLOAT64) as reduction_value
  FROM efficiencies_with_uid UNPIVOT(
      value FOR month_col IN (
        Jan_2025 AS 'Jan_2025',
        Feb_2025 AS 'Feb_2025',
        Mar_2025 AS 'Mar_2025',
        Apr_2025 AS 'Apr_2025',
        May_2025 AS 'May_2025',
        Jun_2025 AS 'Jun_2025',
        Jul_2025 AS 'Jul_2025',
        Aug_2025 AS 'Aug_2025',
        Sep_2025 AS 'Sep_2025',
        Oct_2025 AS 'Oct_2025',
        Nov_2025 AS 'Nov_2025',
        Dec_2025 AS 'Dec_2025',
        Jan_2026 AS 'Jan_2026',
        Feb_2026 AS 'Feb_2026',
        Mar_2026 AS 'Mar_2026',
        Apr_2026 AS 'Apr_2026',
        May_2026 AS 'May_2026',
        Jun_2026 AS 'Jun_2026',
        Jul_2026 AS 'Jul_2026',
        Aug_2026 AS 'Aug_2026',
        Sep_2026 AS 'Sep_2026',
        Oct_2026 AS 'Oct_2026',
        Nov_2026 AS 'Nov_2026',
        Dec_2026 AS 'Dec_2026'
      )
    )
  WHERE value IS NOT NULL
),
merged_table AS (
  SELECT c.Forecast_Date,
    ue.appointment_month,
    ue.efficiency_uid,
    CASE
      WHEN CONTAINS_SUBSTR(ue.Type_of_Reduction, '%') THEN ue.reduction_value
      WHEN c.total_value > 0 THEN (ue.reduction_value / c.total_value)
      ELSE NULL
    END AS reduction_value
  FROM unpivoted_efficiencies ue
    LEFT JOIN corrections c ON ue.efficiency_uid = c.efficiency_uid
    AND DATE(ue.appointment_month) = DATE(PARSE_DATE('%Y-%m-%d', c.Appointment_Month))
),
cumulative_reductions AS (
  SELECT Forecast_Date,
    appointment_month,
    efficiency_uid,
    reduction_value,
    LAST_VALUE(reduction_value IGNORE NULLS) OVER (
      PARTITION BY Forecast_Date, efficiency_uid
      ORDER BY appointment_month
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_reduction
  FROM merged_table
  WHERE DATE_TRUNC(DATE(appointment_month), MONTH) >= DATE_TRUNC(DATE(Forecast_Date), MONTH)
)
SELECT cr.Forecast_Date,
  cr.appointment_month,
  ejt.product,
  ejt.job_type AS Work_Order_Action,
  ejt.district,
  cr.cumulative_reduction as efficiency_reduction
FROM cumulative_reductions cr
  JOIN expanded_job_types ejt ON cr.efficiency_uid = ejt.efficiency_uid
