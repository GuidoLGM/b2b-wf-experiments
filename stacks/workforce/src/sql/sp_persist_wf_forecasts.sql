BEGIN DECLARE dataset STRING;

DECLARE latest_predictions STRING;

DECLARE history_of_predictions STRING;

SET
  dataset = '`{project}.b2b_wf_prediction';

SET
  latest_predictions = CONCAT (dataset, '.', _table_name, '`');

SET
  history_of_predictions = CONCAT (dataset, '.', 'bq_wf_forecast`');

EXECUTE IMMEDIATE FORMAT (
  """
    INSERT INTO %s
    SELECT CAST(CURRENT_DATETIME() AS STRING) AS Forecast_Date,
          Appointment_Month,
          District,
          Product,
          SWT,
          SWT_Type,
          Series_Identifier,
          Technology,
          Work_Force,
          Work_Order_Action,
          explanation,
          predicted_SWT
      FROM %s;
  """,
  history_of_predictions,
  latest_predictions
);

EXECUTE IMMEDIATE FORMAT ("DROP TABLE %s", latest_predictions);

END