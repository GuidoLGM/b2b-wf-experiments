BEGIN
INSERT INTO
    `{project}.b2b_wf_prediction.bq_wf_historical`
SELECT
    Appointment_Timestamp,
    Work_Force,
    Job_Assignment_Status,
    Job_Type_Cd,
    Product_Category,
    Technology,
    Work_Order_Action,
    Work_Order_Action_Grp,
    District,
    Province,
    Product_Key,
    Product,
    Product_Grp,
    SWT,
    AWT,
    JobCount,
    WorkOrderCount
FROM
    `{project}.b2b_wf_prediction.tf_get_period_wf_data` (_start_period_dt, _end_period_dt);
END