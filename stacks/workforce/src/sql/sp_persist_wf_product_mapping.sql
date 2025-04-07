BEGIN
    BEGIN TRANSACTION;

    TRUNCATE TABLE `{project}.b2b_wf_prediction.bq_wf_product_mapping`;

    INSERT INTO `{project}.b2b_wf_prediction.bq_wf_product_mapping`
    (FWDS_product_name, RGU_product_name)
    VALUES
        ('SECURITY', 'Secure Business'),
        ('HSIA', '-HSIA Non-Hospitality'),
        ('PRIVATE LINE', 'PRI'),
        ('WAN L2_L3', 'Internetworking'),
        ('BUSINESS INTERNET', 'Business Internet'),
        ('SDS WIFI', 'Wi-Fi (Meraki)'),
        ('POTS', 'Local Access'),
        ('BUSINESS CONNECT', 'Business Connect'),
        ('MPAAS', 'SD-WAN (MPasS)'),
        ('IPTV', '-BTV Non-Hospitality'),
        ('WIFI', '-Wi-Fi Non-Hospitality'),
        ('CENTREX', 'Centrex'),
        ('NAAS', 'SD-WAN (NaaS)'),
        ('SD WAN', 'SD-WAN (Viptella/Velocloud)');
    COMMIT;
END;