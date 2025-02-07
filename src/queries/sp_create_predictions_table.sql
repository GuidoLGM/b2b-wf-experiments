BEGIN
  EXECUTE IMMEDIATE FORMAT(
    "CREATE OR REPLACE TABLE `b2b_wf_prediction.bq_wf_temp_predictions` AS %s",
    query
  );
END;