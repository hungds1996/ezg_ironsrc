# job_name = "Ironsrc daily aggregated data"
# table_id = "hide-and-seek-97def.OTHERS.import_logs"
# json_keys = "speedster-hit-682166e75581.json"
json_keys = "data-import-409408-97adc8924579.json"

secret_key = "71fc9ccb6e36f980b05b24c478c34c8b"
refresh_token = "12038665dce1f1a2747cfedb9a10d0f4"

email_config = {
    "sender": "hungds1996@gmail.com",
    "password": "pvbm vfld coeq wohw",
    "receiver": ["hungds1996@gmail.com", "hungds@e-lite.games"],
}

revenue_level_params = {
    "impression_level": {
        "base_url": "https://platform.ironsrc.com/partners/adRevenueMeasurements/v4?",
        "temp_file": ".\\data\\impression_level_revenue.csv",
    },
    "user_level": {
        "base_url": "https://platform.ironsrc.com/partners/userAdRevenue/v3?",
        "temp_file": ".\\data\\user_level_revenue.csv",
    },
    "reporting_api": {"temp_file": ".\\data\\reporting_api.csv"},
}

app_keys_list = ["19b574ff5"]


ironsrc_api_params = {
    "sample_api": {
        "metrics": "revenue,impressions,eCPM",
        "breakdown": "date,app",
        "temp_file": ".\data\sample_api.csv",
    },
    "instance_level": {
        "metrics": "revenue,eCPM,impressions,completions,clicks,clickThroughRate,adSourceChecks,adSourceResponses,adSourceAvailabilityRate,impressionPerEngagedSessions",
        "breakdown": "date,app,platform,country,adSource,adUnits,instance,att,adfa,abTest",
        "temp_file": ".\data\ironsrc_instance_data.csv",
    },
}

app_info = {
    "19b574ff5": {
        "app_name": "com.nmg.survivalhero",
        "reporting_api": "data-import-409408.temp_tables.reporting_api",
        "impression_level": "data-import-409408.temp_tables.impression_level_revenue",
        "user_level": "data-import-409408.temp_tables.user_level_revenue",
        "sample_table": "data-import-409408.temp_tables.sample_level",
    }
}

import_log_table = "data-import-409408.logs.ironsource_logs"

temp_file = "ironsrc_data.csv"
