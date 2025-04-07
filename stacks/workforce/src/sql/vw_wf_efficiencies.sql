SELECT 
  Program_Name,
  Initiative_Name,
  Owner,
  Start_Date,
  Products_Impacted,
  Job_Types_Impacted,
  Region_Impacted,
  Type_of_Reduction,
  FORMAT_DATE('%Y-%m', PARSE_DATE('%b_%Y', Month)) AS Month,
  Value
FROM `{project}.b2b_wf_prediction.bq_wf_efficiencies` AS Efficiencies_Counts
UNPIVOT(Value FOR Month IN (
  Jan_2025, Feb_2025, Mar_2025, Apr_2025, May_2025, Jun_2025, Jul_2025, Aug_2025, Sep_2025, Oct_2025, Nov_2025, Dec_2025,
  Jan_2026, Feb_2026, Mar_2026, Apr_2026, May_2026, Jun_2026, Jul_2026, Aug_2026, Sep_2026, Oct_2026, Nov_2026, Dec_2026
))