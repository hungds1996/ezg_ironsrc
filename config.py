json_keys = "<google cloud console service account>"

secret_key = "<ironsrc secret key>"
refresh_token = "<ironsrc refresh token>"

email_config = {
    "sender": "<email>",
    "password": "<app password for email>",
    "receiver": ["hungds1996@gmail.com"],
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


app_info = {
    "<ironsrc appKey>": {
        "app_name": "<appname>",
        "reporting_api": "data-import-409408.temp_tables.reporting_api",
        "impression_level": "data-import-409408.temp_tables.impression_level_revenue",
        "user_level": "data-import-409408.temp_tables.user_level_revenue",
        "sample_table": "data-import-409408.temp_tables.sample_level",
    }
}

import_log_table = "data-import-409408.logs.ironsource_logs"
