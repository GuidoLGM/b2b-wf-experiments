WITH
/* TM: Team Member information including manager and manager's manager */
TM AS (
  SELECT OPS_TID,
    OPS_MGR,
    MGR_TID,
    FIELD_MGR,
    team_member_id,
    TECHNICIAN,
    COMPANY
  FROM `{project}.b2b_wf_prediction.tf_team_member_information`() TM
),
/*TMTE: Team Member Time Entry. Returns the total number of hours for each job or work order component */
TMTE AS (
  SELECT job_id,
    SUM(number_of_hours) `NUMBER_OF_HOURS`
  FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_team_member_time_entry_view`
  WHERE job_id IS NOT NULL
    AND effective_end_ts = "9999-12-31"
  GROUP BY job_id
),
-- TODO: doc
TMTEC AS (
  SELECT job_id,
    work_order_component_id,
    SUM(number_of_hours) `NUMBER_OF_HOURS`
  FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_team_member_time_entry_view`
  WHERE job_id IS NOT NULL
    AND effective_end_ts = "9999-12-31"
  GROUP BY job_id,
    work_order_component_id
),
WOL AS(
  SELECT WOL.work_order_id,
    WOL.province_cd,
    CASE
      WOL.region_nm
      WHEN 'RÃ©seau - Affaire QuÃ©bec' THEN 'Réseau - Affaire Québec'
      ELSE WOL.region_nm
    END `region_nm`,
    CASE 
        WHEN districts.migration IN ('') THEN districts.district
        ELSE districts.district_old
    END `district_nm`,
    WOL.service_area_nm,
    WOL.service_area_clli_cd,
    WOL.common_area_text
  FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_work_order_location_view` WOL
  LEFT JOIN `{project}.b2b_wf_prediction.bq_wf_districts` districts
        ON districts.service_area_clli = WOL.service_area_clli_cd
  WHERE WOL.work_order_location_usage_cd = 'NAVIGATION_HIERARCHY'
    AND WOL.effective_end_ts = "9999-12-31"
    AND (
      WOL.district_nm LIKE '%FFB%'
      or WOL.district_nm IN (
        'CÃ´te-Nord - Nord du QuÃ©bec',
        'MontrÃ©al - QuÃ©bec',
        'BSL-GaspÃ©sie'
      )
    ) QUALIFY RANK() OVER (
      PARTITION BY work_order_id
      ORDER BY src_last_updt_ts DESC
    ) = 1
),
/*WOL2: Work Order Location. Returns dispatch location coordinates and address*/
WOL2 AS (
  SELECT work_order_id,
    CASE
      WHEN municipality_nm is NULL THEN ARRAY_TO_STRING(
        [
          TRIM(serv_addr_apt_no),
          TRIM(serv_addr_house), 
          TRIM(serv_addr_street_name), 
          TRIM(sa_city_province_code), 
          TRIM(sa_postal_code)
        ],
        ' '
      )
      ELSE ARRAY_TO_STRING(
        [
          TRIM(civic_number), 
          TRIM(street_name_pre_type_cd), 
          TRIM(street_nm), 
          TRIM(street_name_post_type_cd), 
          TRIM(municipality_nm), 
          TRIM(province_state_cd), 
          TRIM(postal_code_txt)
        ],
        ' '
      )
    END `PARTY_ADDRESS`,
    lattitude `DISPATCH_LATITUDE`,
    longitude `DISPATCH_LONGITUDE`
  FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_work_order_location_view`
  WHERE effective_end_ts = "9999-12-31"
    AND work_order_location_usage_cd = 'DISPATCH' QUALIFY RANK() OVER (
      PARTITION BY work_order_id
      ORDER BY src_last_updt_ts DESC
    ) = 1
),
/* WOAV: Work Order Attributes (e.g. Technology, Product, Special Project). Returns most recent record for each work order attribute code */
WOAV AS (
  SELECT work_order_id,
    MAX(
      CASE
        WHEN work_order_attribute_cd = 'TECHNOLOGY' THEN work_order_attribute_value_txt
        ELSE NULL
      END
    ) TECHNOLOGY,
    MAX(
      CASE
        WHEN work_order_attribute_cd = 'PRODUCT_CATEGORY' THEN work_order_attribute_value_txt
        ELSE NULL
      END
    ) PRODUCT,
    MAX(
      CASE
        WHEN work_order_attribute_cd = 'SPECIAL_PROJECT' THEN work_order_attribute_value_txt
        ELSE NULL
      END
    ) SPECIAL_PROJECT,
    MAX(
      CASE
        WHEN work_order_attribute_cd = 'SITE_ACCESS' THEN work_order_attribute_value_txt
        ELSE NULL
      END
    ) SITE_ACCESS
  FROM (
      SELECT *
      FROM (
          SELECT A.*,
            RANK() OVER (
              PARTITION BY work_order_id,
              work_order_attribute_cd
              ORDER BY work_order_attribute_value_id DESC
            ) RANK
          FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_work_order_attribute_value_view` A
          WHERE work_order_attribute_cd IN (
              'PRODUCT_CATEGORY',
              'TECHNOLOGY',
              'SPECIAL_PROJECT',
              'SITE_ACCESS'
            )
        )
      WHERE RANK = 1
    )
  GROUP BY work_order_id
),
/* JPS: Job Processing State (e.g. Is Assist, Is Continuation, Is Pullahead, RFD). Returns most recent record for each job processing state */
JPS AS (
  SELECT job_id,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'IS_ASSIST' THEN job_processing_state_desc
        ELSE NULL
      END
    ) IS_ASSIST,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'IS_CONTINUATION' THEN job_processing_state_desc
        ELSE NULL
      END
    ) IS_CONTINUATION,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'IS_PULLAHEAD' THEN job_processing_state_desc
        ELSE NULL
      END
    ) IS_PULLAHEAD,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'HAS_PULLAHEAD' THEN job_processing_state_desc
        ELSE NULL
      END
    ) HAS_PULLAHEAD,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'READY_FOR_DISPATCH' THEN job_processing_state_desc
        ELSE NULL
      END
    ) RFD,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'READY_FOR_DISPATCH' THEN effective_start_ts
        ELSE NULL
      END
    ) RFD_DT,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'PARENT' THEN job_processing_state_desc
        ELSE NULL
      END
    ) PARENT,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'PINNED' THEN job_processing_state_desc
        ELSE NULL
      END
    ) PINNED,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'PINNED_TO_CONTRACTOR' THEN job_processing_state_desc
        ELSE NULL
      END
    ) PINNED_TO_CONTRACTOR,
    MAX(
      CASE
        WHEN job_processing_state_cd = 'IS_MULTIDAY' THEN job_processing_state_desc
        ELSE NULL
      END
    ) IS_MULTIDAY
  FROM (
      SELECT *
      FROM (
          SELECT A.*,
            RANK() OVER (
              PARTITION BY job_id,
              job_processing_state_cd
              ORDER BY job_processing_state_desc DESC
            ) RANK
          FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_processing_state_view` A
          WHERE job_processing_state_cd IN (
              'IS_ASSIST',
              'IS_CONTINUATION',
              'IS_PULLAHEAD',
              'HAS_PULLAHEAD',
              'READY_FOR_DISPATCH',
              'PARENT',
              'PINNED',
              'PINNED_TO_CONTRACTOR',
              'IS_MULTIDAY'
            )
        )
      WHERE RANK = 1
    )
  GROUP BY job_id
),
/* JAV: Job Attribute Values (e.g. Rebook Required Indicator, Megatask Call ID). Returns the most recent record for each job attribute value */
JAV AS (
  SELECT job_id,
    MAX(
      CASE
        WHEN job_attribute_cd = 'DEMANDTYPE' THEN job_attribute_value_txt
        ELSE NULL
      END
    ) DEMAND_TYPE,
    MAX(
      CASE
        WHEN job_attribute_cd = 'REBOOKREQ_IND' THEN CASE
          WHEN job_attribute_value_txt = 'true' THEN '1'
          ELSE '0'
        END
        ELSE NULL
      END
    ) REBOOKREQ_IND,
    MAX(
      CASE
        WHEN job_attribute_cd = 'MEGATASK_CALLID' THEN job_attribute_value_txt
        ELSE NULL
      END
    ) MEGATASK_CALLID
  FROM (
      SELECT *
      FROM (
          SELECT A.*,
            RANK() OVER (
              PARTITION BY job_id,
              job_attribute_cd
              ORDER BY job_attribute_value_txt DESC
            ) RANK
          FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_attribute_value_view` A
          WHERE job_attribute_cd IN (
              'DEMANDTYPE',
              'REBOOKREQ_IND',
              'MEGATASK_CALLID'
            )
        )
      WHERE RANK = 1
    )
  GROUP BY job_id
),
/* DT: Job Attribute Values (e.g. Demand Type). Returns the most recent demand type record for each external_job_grouping_id (e.g. Call ID). 
 Populates the demand type of the original job for assists, continuations, and pullaheads where there is no Demand Type job attribute value */
DT AS (
  SELECT external_job_grouping_id,
    MAX(
      CASE
        WHEN job_attribute_cd = 'DEMANDTYPE' THEN job_attribute_value_txt
        ELSE NULL
      END
    ) DEMAND_TYPE,
    FROM (
      SELECT *
      FROM (
          SELECT B.external_job_grouping_id,
            A.*,
            RANK() OVER (
              PARTITION BY B.external_job_grouping_id,
              job_attribute_cd
              ORDER BY job_attribute_value_txt DESC
            ) RANK
          FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_attribute_value_view` A
            INNER JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_view` B ON A.job_id = B.job_id
          WHERE job_attribute_cd IN ('DEMANDTYPE')
        )
      WHERE RANK = 1
    )
  GROUP BY external_job_grouping_id
),
/*JAS: Job Assignment status. Returns the most recent record for each job assignment status */
JAS AS (
  SELECT job_assignment_id,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'TENTATIVE' THEN actual_status_dt
        ELSE NULL
      END
    ) TENTATIVE_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'DISPATCHED' THEN actual_status_dt
        ELSE NULL
      END
    ) DISPATCHED_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'ACCEPTED' THEN actual_status_dt
        ELSE NULL
      END
    ) ACCEPTED_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'REJECTED' THEN actual_status_dt
        ELSE NULL
      END
    ) REJECTED_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'EN_ROUTE' THEN actual_status_dt
        ELSE NULL
      END
    ) ENROUTE_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'ON_SITE' THEN actual_status_dt
        ELSE NULL
      END
    ) ONSITE_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'COMPLETE' THEN actual_status_dt
        ELSE NULL
      END
    ) COMPLETE_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd = 'INCOMPLETE' THEN actual_status_dt
        ELSE NULL
      END
    ) INCOMPLETE_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd IN ('COMPLETE', 'INCOMPLETE', 'REJECTED') THEN actual_status_dt
        ELSE NULL
      END
    ) CLOSE_DT,
    MAX(
      CASE
        WHEN job_assignment_status_cd IN ('COMPLETE', 'INCOMPLETE', 'REJECTED') THEN job_assignment_status_cd
        ELSE NULL
      END
    ) JOB_ASSIGNMENT_STATUS_CD,
    MAX(
      CASE
        WHEN job_assignment_status_cd IN ('COMPLETE', 'INCOMPLETE', 'REJECTED') THEN job_assignment_status_rsn_cd
        ELSE NULL
      END
    ) JOB_ASSIGNMENT_STATUS_RSN
  FROM (
      SELECT *
      FROM (
          SELECT A.*,
            RANK() OVER (
              PARTITION BY job_assignment_id,
              job_assignment_status_cd
              ORDER BY actual_status_dt DESC
            ) RANK
          FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_assignment_status_view` A
          WHERE job_assignment_status_cd IN (
              'TENTATIVE',
              'DISPATCHED',
              'ACCEPTED',
              'REJECTED',
              'EN_ROUTE',
              'ON_SITE',
              'COMPLETE',
              'INCOMPLETE'
            )
        )
      WHERE RANK = 1
    )
  GROUP BY job_assignment_id
),
/*WOSRV: Work Order Service. Returns TEL/CKT */
WOSRV AS (
  SELECT work_order_id,
    UPPER(IFNULL(account, IFNULL(circuit, phone))) AS `TEL|CKT`
  FROM (
      SELECT work_order_id,
        MAX(
          CASE
            WHEN service_type_cd = 'ACCOUNT' THEN service_identification
            ELSE NULL
          END
        ) ACCOUNT,
        MAX(
          CASE
            WHEN service_type_cd = 'CIRCUIT' THEN service_identification
            ELSE NULL
          END
        ) CIRCUIT,
        MAX(
          CASE
            WHEN service_type_cd = 'PHONE' THEN service_identification
            ELSE NULL
          END
        ) PHONE
      FROM (
          SELECT *
          FROM (
              SELECT A.*,
                RANK() OVER (
                  PARTITION BY work_order_id,
                  service_type_cd
                  ORDER BY service_identification DESC
                ) RANK
              FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_work_order_service_view` A
              WHERE service_type_cd IN ('ACCOUNT', 'CIRCUIT', 'PHONE')
            )
          WHERE RANK = 1
        )
      GROUP BY work_order_id
    )
),
/*WOPI: Work Order Party Involve. Returns customer information */
WOPI AS (
  SELECT work_order_id,
    party_name,
    market_segment_txt
  FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_wo_party_involvement_view` WOPI
  WHERE effective_end_ts = "9999-12-31"
)
/* Main Query*/
SELECT Appointment_Timestamp,
  Work_Force,
  Job_Assignment_Status,
  Job_Type_Cd,
  Product_Category,
  Special_Project,
  Technology,
  Work_Order_Action,
  CASE
    WHEN Work_Order_Action = 'INSTALL' THEN 'Install'
    WHEN Work_Order_Action = 'PRESTAGE' THEN 'Install'
    WHEN Work_Order_Action = 'PREFIELD' THEN 'Install'
    WHEN Work_Order_Action = 'RISER' THEN 'Install'
    WHEN Work_Order_Action = 'CHANGE' THEN 'MAC & Out'
    WHEN Work_Order_Action = 'MOVE' THEN 'MAC & Out'
    WHEN Work_Order_Action = 'OUT' THEN 'MAC & Out'
    WHEN Work_Order_Action = 'REPAIR' THEN 'Repair'
    WHEN Work_Order_Action = 'PROJECT' THEN 'PROJECT'
    WHEN Work_Order_Action = 'EXCLUDE' THEN 'EXCLUDE'
    WHEN Work_Order_Action = 'PROJECT (EXCLUDE)' THEN 'PROJECT (EXCLUDE)'
    ELSE 'OTHER'
  END `Work_Order_Action_Grp`,
  District,
  Province,
  Product_Key,
  Product,
  CASE
    WHEN Product = 'BUSINESS INTERNET'
    AND province != 'QC' THEN 'Managed BI'
    WHEN Product = 'CARRIER ETHERNET'
    AND province != 'QC' THEN 'Managed CES'
    WHEN Product = 'MANAGED LAN'
    AND province != 'QC' THEN 'Managed Legacy'
    WHEN Product = 'PRIVATE LINE'
    AND province != 'QC' THEN 'Managed Legacy'
    WHEN Product = 'SD WAN'
    AND province != 'QC' THEN 'Managed WAN'
    WHEN Product = 'WAN L2_L3'
    AND province != 'QC' THEN 'Managed WAN'
    WHEN Product = 'WAVELENGTH'
    AND province != 'QC' THEN 'Managed WAN'
    WHEN Product = 'BUSINESS CONNECT'
    AND province != 'QC' THEN 'Unmanaged 3rd Wave'
    WHEN Product = 'CENTREX'
    AND province != 'QC' THEN 'Unmanaged Legacy'
    WHEN Product = 'HHM'
    AND province != 'QC' THEN 'Unmanaged 3rd Wave'
    WHEN Product = 'HSIA'
    AND province != 'QC' THEN 'Unmanaged'
    WHEN Product = 'IPTV'
    AND province != 'QC' THEN 'Unmanaged'
    WHEN Product = 'MPAAS'
    AND province != 'QC' THEN 'Unmanaged MPAAS'
    WHEN Product = 'NAAS'
    AND province != 'QC' THEN 'Unmanaged 3rd Wave'
    WHEN Product = 'POTS'
    AND province != 'QC' THEN 'Unmanaged'
    WHEN Product = 'SDS WIFI' THEN 'Unmanaged 3rd Wave'
    WHEN Product = 'SECURITY'
    AND province != 'QC' THEN 'Unmanaged 3rd Wave'
    WHEN Product = 'TRUE STATIC IP'
    AND province != 'QC' THEN 'Unmanaged 3rd Wave'
    WHEN Product = 'WHSIA'
    AND province != 'QC' THEN 'Unmanaged'
    WHEN Product = 'WIFI'
    AND province != 'QC' THEN 'Unmanaged Legacy'
    WHEN Product = 'IOT MODEM'
    AND province = 'QC' THEN 'Unmanaged'
    WHEN Product = 'CPS'
    AND province = 'QC' THEN 'Managed CPS'
    WHEN Product = 'BUSINESS CONNECT'
    AND province = 'QC' THEN 'Managed 3rd Wave'
    WHEN Product = 'BUSINESS INTERNET'
    AND province = 'QC' THEN 'Managed BI'
    WHEN Product = 'CARRIER ETHERNET'
    AND province = 'QC' THEN 'Managed CES'
    WHEN Product = 'MANAGED LAN'
    AND province = 'QC' THEN 'Managed Legacy'
    WHEN Product = 'MPAAS'
    AND province = 'QC' THEN 'Managed MPAAS'
    WHEN Product = 'NAAS'
    AND province = 'QC' THEN 'Managed 3rd Wave'
    WHEN Product = 'PRIVATE LINE'
    AND province = 'QC' THEN 'Managed Legacy'
    WHEN Product = 'SD WAN'
    AND province = 'QC' THEN 'Managed WAN'
    WHEN Product = 'WAN L2_L3'
    AND province = 'QC' THEN 'Managed WAN'
    WHEN Product = 'HSIA'
    AND province = 'QC' THEN 'Unmanaged'
    WHEN Product = 'POTS'
    AND province = 'QC' THEN 'Unmanaged'
    WHEN Product = 'SECURITY'
    AND province = 'QC' THEN 'Unmanaged 3rd Wave'
    WHEN Product = 'WIFI'
    AND province = 'QC' THEN 'Unmanaged Legacy'
    ELSE 'OTHER'
  END `Product_Grp`,
  SWT,
  AWT,
  JobCount,
  WorkOrderCount
FROM (
    SELECT CAST(appointment_start AS DATE) `Appointment_Timestamp`,
      work_force `Work_Force`,
      job_assignment_status `Job_Assignment_Status`,
      job_type_cd `Job_Type_Cd`,
      product_category `Product_Category`,
      technology `Technology`,
      special_project `Special_Project`,
      CASE
        WHEN (
          work_force IN ('ADT', 'CNI', 'Activo')
          AND province = 'QC'
        ) THEN 'EXCLUDE'
        WHEN UPPER(special_project) IN ('BCONN MULTIPORT ATA', 'SMP MIGRATION') THEN 'PROJECT'
        WHEN UPPER(special_project) like 'CUISSS%' THEN 'PROJECT'
        WHEN UPPER(special_project) like 'CIUSSS%' THEN 'PROJECT'
        WHEN UPPER(special_project) like 'WIFI RITM%' THEN 'PROJECT'
        WHEN UPPER(special_project) like 'C2F%' THEN 'PROJECT'
        WHEN (
          UPPER(special_project) like ('NBD SPECIAL PROJECT 1%')
          and (field_manager_tid != 'T849386')
        ) THEN 'PROJECT'
        WHEN (
          UPPER(special_project) IN ('GOC WCS')
          and (work_force = 'TELUS')
        ) THEN 'PROJECT'
        WHEN (
          UPPER(special_project) IN ('GOC WCS')
          and (work_force != 'TELUS')
        ) THEN 'PROJECT (EXCLUDE)'
        WHEN UPPER(special_project) like 'AHS ETTS SCHC%' THEN 'PROJECT (EXCLUDE)'
        WHEN job_classification = 'Prestage' THEN 'PRESTAGE'
        WHEN job_type_cd LIKE 'PRE_STAGE%' THEN 'PRESTAGE'
        WHEN job_type_cd LIKE 'PREFIELD%' THEN 'PREFIELD'
        WHEN work_order_action_cd IN ('PREV') THEN 'PREFIELD'
        WHEN work_order_action_cd IN ('C', 'CH', 'MAC', 'RG') THEN 'CHANGE'
        WHEN work_order_action_cd IN (
          'I',
          'PR',
          'INST',
          'DEPL',
          'KOFF',
          'HCUT',
          'POST',
          'SSURV',
          'PREP'
        ) THEN 'INSTALL'
        WHEN work_order_action_cd IN ('F', 'T', 'PV') THEN 'MOVE'
        WHEN work_order_action_cd IN ('R') THEN 'REPAIR'
        WHEN work_order_action_cd IN ('RSR') THEN 'RISER'
        WHEN work_order_action_cd IN ('O', 'PICKUP', 'DEPROG', 'D', 'CE') THEN 'OUT'
        WHEN (
          work_order_classification_cd = 'TROUBLE'
          AND work_order_action_cd is null
        ) THEN 'REPAIR'
        ELSE 'OTHER'
      END `Work_Order_Action`,
      district `District`,
      province `Province`,
      CONCAT(job_type_cd, product_category) `Product_Key`,
      (
        SELECT `Product`
        FROM `{project}.b2b_wf_prediction.tf_group_job_type_with_product_category`(job_type_cd, product_category)
      ) `Product`,
      SUM(SWT) `SWT`,
      SUM(AWT) `AWT`,
      COUNT(1) `JobCount`,
      COUNT(distinct work_order_id) `WorkOrderCount`
    FROM (
        SELECT WO.work_order_id `WORK_ORDER_ID`,
          J.job_id `JOB_ID`,
          J.external_job_id `TASK`,
          J.external_job_grouping_id `CALL_ID`,
          WO.orig_system_id `ORIG_SYSTEM_ID`,
          WO.orig_system_wo_id `ORIG_SYSTEM_WO_ID`,
          CASE
            WHEN WOSRV.`TEL|CKT` LIKE '%ONCALL%' THEN 'On Call'
            WHEN WOSRV.`TEL|CKT` LIKE '%ON-CALL%' THEN 'On Call'
            WHEN WOSRV.`TEL|CKT` LIKE '%ANALYZER%' THEN 'Analyzer'
            WHEN WOSRV.`TEL|CKT` LIKE '%FIELD_ANALYTICS%' THEN 'Analyzer'
            WHEN WOSRV.`TEL|CKT` LIKE '%CO_WORK%' THEN 'CO Work'
            WHEN WOSRV.`TEL|CKT` LIKE '%CO_TASK%' THEN 'CO Work'
            WHEN WOSRV.`TEL|CKT` LIKE '%PRESTAGE%' THEN 'Prestage'
            WHEN WOSRV.`TEL|CKT` LIKE '%FILLER%%' THEN 'Filler'
            WHEN WOSRV.`TEL|CKT` LIKE '%SPAN%' THEN 'SPAN'
            WHEN WOSRV.`TEL|CKT` LIKE '%WLNGM%' THEN 'NGM'
            WHEN WOSRV.`TEL|CKT` LIKE '%NGM-%' THEN 'NGM'
            WHEN WOSRV.`TEL|CKT` LIKE '%NGM_%' THEN 'NGM'
            WHEN WOSRV.`TEL|CKT` LIKE '%NGM%' THEN 'NGM'
            WHEN WOSRV.`TEL|CKT` LIKE '%NBD_INTERNAL_WORK%' THEN 'NBD_Internal_Work'
            ELSE 'BAU'
          END `JOB_CLASSIFICATION`,
          WOC.work_order_component_id `WORK_ORDER_COMPONENT_ID`,
          DATETIME(WO.src_create_ts, WOL.common_area_text) `WO_CREATE_DATETIME`,
          DATETIME(rfd_dt, WOL.common_area_text) `RFD_DATE`,
          DATETIME(enroute_dt, WOL.common_area_text) `ENROUTE_DATE`,
          DATETIME(close_dt, WOL.common_area_text) `WORK_DATE`,
          CASE
            WHEN J.appointment_start_datetime IS NULL THEN DATETIME(J.early_start_time, WOL.common_area_text)
            ELSE DATETIME(
              J.appointment_start_datetime,
              WOL.common_area_text
            )
          END `APPOINTMENT_START`,
          TM.team_member_id `TID`,
          TM.TECHNICIAN `TECHNICIAN`,
          TM.mgr_tid `FIELD_MANAGER_TID`,
          TM.FIELD_MGR `FIELD_MANAGER`,
          TM.OPS_MGR `OPS_MANAGER`,
          CASE
            WHEN TM.company = 'Activo'
            AND WOL.province_cd NOT IN ('AB', 'BC')
            AND TM.technician NOT LIKE '%SMB T%' THEN 'Activo (East)'
            ELSE TM.company
          END `WORK_FORCE`,
          JAS.job_assignment_status_cd `JOB_ASSIGNMENT_STATUS`,
          JAS.job_assignment_status_rsn `JOB_ASSIGNMENT_STATUS_RSN`,
          WOS.work_order_status_cd `WORK_ORDER_STATUS`,
          CASE
            WHEN WOC.work_order_component_id IS NOT NULL THEN WOC.estimated_duration_amt
            ELSE J.estimated_duration
          END `SWT`,
          CASE
            WHEN WOC.work_order_component_id IS NOT NULL THEN IFNULL(TMTEC.number_of_hours, 0)
            ELSE IFNULL(TMTE.number_of_hours, 0)
          END `AWT`,
          WO.service_class_cd `WORK_ORDER_SERVICE_CLASS_CD`,
          WO.work_order_classification_cd `WORK_ORDER_CLASSIFICATION_CD`,
          WO.work_order_category_cd `WORK_ORDER_CATEGORY_CD`,
          CASE
            WHEN WOC.work_order_component_id IS NOT NULL THEN WOC.work_order_action_cd
            ELSE WO.work_order_action_cd
          END `WORK_ORDER_ACTION_CD`,
          CASE
            WHEN WOC.work_order_component_id IS NOT NULL THEN WOC.job_type_cd
            ELSE J.job_type_cd
          END `JOB_TYPE_CD`,
          CASE
            WHEN WOC.work_order_component_id IS NOT NULL THEN WOC.product_category_cd
            ELSE product
          END `PRODUCT_CATEGORY`,
          CASE
            WHEN WOC.work_order_component_id IS NOT NULL THEN WOC.technology_cd
            ELSE technology
          END `TECHNOLOGY`,
          special_project `SPECIAL_PROJECT`,
          CAST(IFNULL(JAV.demand_type, 'OTHER') AS STRING) `JAV_DEMAND_TYPE`,
          CAST(IFNULL(DT.demand_type, 'OTHER') AS STRING) `DT_DEMAND_TYPE`,
          CAST(IFNULL(parent, '0') AS INT) `PARENT`,
          CAST(IFNULL(rfd, '0') AS INT) `RFD`,
          CAST(IFNULL(is_assist, '0') AS INT) `IS_ASSIST`,
          CAST(IFNULL(is_continuation, '0') AS INT) `IS_CONTINUATION`,
          CAST(IFNULL(is_pullahead, '0') AS INT) `IS_PULLAHEAD`,
          CAST(IFNULL(pinned, '0') AS INT) `PINNED`,
          CAST(IFNULL(pinned_to_contractor, '0') AS INT) `PINNED_TO_CONTRACTOR`,
          CAST(IFNULL(rebookreq_ind, '0') AS INT) `REBOOK_REQ`,
          WOL.province_cd `PROVINCE`,
          WOL.region_nm `REGION`,
          WOL.district_nm `DISTRICT`,
          WOPI.party_name `CUSTOMER_NAME`,
          WOL2.party_address `PARTY_ADDRESS`
        FROM `cio-datahub-enterprise-pr-183a.src_fwds.bq_work_order_view` WO
          LEFT JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_work_order_component_view` WOC ON WO.work_order_id = WOC.work_order_id
          INNER JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_view` J ON WO.work_order_id = J.work_order_id
          INNER JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_assignment_view` JA ON J.job_id = JA.job_id
          AND JA.effective_end_ts = "9999-12-31"
          LEFT JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_work_order_status_view` WOS ON WO.work_order_id = WOS.work_order_id
          AND WOS.effective_end_ts = "9999-12-31"
          INNER JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_job_status_view` JS ON J.job_id = JS.job_id
          AND JS.effective_end_ts = "9999-12-31"
          INNER JOIN WOL ON WO.work_order_id = WOL.work_order_id
          LEFT JOIN WOL2 ON WO.work_order_id = WOL2.work_order_id
          LEFT JOIN WOAV ON WO.work_order_id = WOAV.work_order_id
          LEFT JOIN WOSRV ON WO.work_order_id = WOSRV.work_order_id
          LEFT JOIN TM ON JA.team_member_id = TM.team_member_id
          LEFT JOIN JPS ON J.job_id = JPS.job_id
          LEFT JOIN JAV ON J.job_id = JAV.job_id
          LEFT JOIN DT ON J.external_job_grouping_id = DT.external_job_grouping_id
          LEFT JOIN JAS ON JA.job_assignment_id = JAS.job_assignment_id
          LEFT JOIN TMTE ON J.job_id = TMTE.job_id
          LEFT JOIN TMTEC ON J.job_id = TMTEC.job_id
          AND TMTEC.work_order_component_id = WOC.work_order_component_id
          LEFT JOIN WOPI ON WO.work_order_id = WOPI.work_order_id
        WHERE WO.work_order_category_cd IN ('DATA', 'MM')
          AND (
            (
              CAST(
                DATETIME(
                  J.appointment_start_datetime,
                  WOL.common_area_text
                ) AS DATE
              ) >= start_period
              AND CAST(
                DATETIME(
                  J.appointment_start_datetime,
                  WOL.common_area_text
                ) AS DATE
              ) < end_period
            )
            OR (
              CAST(
                DATETIME(J.early_start_time, WOL.common_area_text) AS DATE
              ) >= start_period
              AND CAST(
                DATETIME(J.early_start_time, WOL.common_area_text) AS DATE
              ) < end_period
            )
          )
          AND (
            WOL.district_nm LIKE '%FFB%'
            OR WOL.district_nm LIKE '%BUS%'
            OR WOL.district_nm IN (
              'BSL-Gaspésie',
              'Côte-Nord - Nord du Québec',
              'Montréal - Québec'
            )
          )
          AND JS.job_status_cd <> 'CANCELLED'
          AND JAS.job_assignment_status_cd <> 'REJECTED'
          AND (
            CAST(IFNULL(is_assist, '0') AS INT) = 0
            OR wo.orig_system_id = '14184'
          )
      )
    WHERE JOB_CLASSIFICATION NOT IN (
        'Analyzer',
        'SPAN',
        'NBD_Internal_Work',
        'On Call'
      )
      AND (
        province = 'QC'
        AND work_force NOT IN ('ADT', 'Activo', 'CNI', 'ATI')
        OR (
          province != 'QC'
          AND work_force IN ('Activo', 'LTS', 'CNI', 'TELUS', 'Activo (East)')
        )
      )
      AND (
        (
          technician NOT LIKE '%Z% Activo%'
          AND province IN ('AB', 'BC')
        )
        OR province NOT IN ('AB', 'BC')
      )
    GROUP BY CAST(appointment_start as DATE),
      work_force,
      job_assignment_status,
      job_type_cd,
      product_category,
      technology,
      special_project,
    CASE
                WHEN (
                    work_force IN ('ADT', 'CNI', 'Activo')
                    AND province = 'QC'
                ) THEN 'EXCLUDE'
                WHEN UPPER(special_project) IN ('BCONN MULTIPORT ATA', 'SMP MIGRATION') THEN 'PROJECT'
                WHEN UPPER(special_project) like 'CUISSS%' THEN 'PROJECT'
                WHEN UPPER(special_project) like 'CIUSSS%' THEN 'PROJECT'
                WHEN UPPER(special_project) like 'WIFI RITM%' THEN 'PROJECT'
                WHEN UPPER(special_project) like 'C2F%' THEN 'PROJECT'
                WHEN (
                    UPPER(special_project) like ('NBD SPECIAL PROJECT 1%')
                    and (field_manager_tid != 'T849386')
                ) THEN 'PROJECT'
                WHEN (
                    UPPER(special_project) IN ('GOC WCS')
                    and (work_force = 'TELUS')
                ) THEN 'PROJECT'
                WHEN (
                    UPPER(special_project) IN ('GOC WCS')
                    and (work_force != 'TELUS')
                ) THEN 'PROJECT (EXCLUDE)'
                WHEN UPPER(special_project) like 'AHS ETTS SCHC%' THEN 'PROJECT (EXCLUDE)'
                WHEN job_classification = 'Prestage' THEN 'PRESTAGE'
                WHEN job_type_cd LIKE 'PRE_STAGE%' THEN 'PRESTAGE'
                WHEN job_type_cd LIKE 'PREFIELD%' THEN 'PREFIELD'
                WHEN work_order_action_cd IN ('PREV') THEN 'PREFIELD'
                WHEN work_order_action_cd IN ('C', 'CH', 'MAC', 'RG') THEN 'CHANGE'
                WHEN work_order_action_cd IN (
                    'I',
                    'PR',
                    'INST',
                    'DEPL',
                    'KOFF',
                    'HCUT',
                    'POST',
                    'SSURV',
                    'PREP'
                ) THEN 'INSTALL'
                WHEN work_order_action_cd IN ('F', 'T', 'PV') THEN 'MOVE'
                WHEN work_order_action_cd IN ('R') THEN 'REPAIR'
                WHEN work_order_action_cd IN ('RSR') THEN 'RISER'
                WHEN work_order_action_cd IN ('O', 'PICKUP', 'DEPROG', 'D', 'CE') THEN 'OUT'
                WHEN (
                    work_order_classification_cd = 'TROUBLE'
                    AND work_order_action_cd is null
                ) THEN 'REPAIR'
                ELSE 'OTHER'
            END,
            province,
            district,
            CONCAT(job_type_cd, product_category)
    )