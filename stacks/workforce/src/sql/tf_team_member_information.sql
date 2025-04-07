SELECT
    OMGR.team_member_id AS OPS_TID,
    OMGR.preferred_given_name || ' ' || OMGR.family_name AS OPS_MGR,
    FMGR.team_member_id AS MGR_TID,
    FMGR.preferred_given_name || ' ' || FMGR.family_name AS FIELD_MGR,
    TM.team_member_id,
    TM.preferred_given_name || ' ' || TM.family_name AS TECHNICIAN,
    CASE
        WHEN TM.contracting_company_nm IS NULL THEN 'TELUS'
        ELSE TM.contracting_company_nm
    END AS COMPANY
FROM
    `cio-datahub-enterprise-pr-183a.src_fwds.bq_team_member_view` TM
    LEFT JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_team_member_view` FMGR ON TM.manager_team_member_id = FMGR.team_member_id
    LEFT JOIN `cio-datahub-enterprise-pr-183a.src_fwds.bq_team_member_view` OMGR ON FMGR.manager_team_member_id = OMGR.team_member_id