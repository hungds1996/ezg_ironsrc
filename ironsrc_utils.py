import requests as rq

from bigquery_utils import *
from config import *


class BearerAuth(rq.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token

        return r


# Authenticate user and create AdMob Service Object.
def authenticate():
    auth_url = "https://platform.ironsrc.com/partners/publisher/auth"
    auth_headers = {"secretkey": secret_key, "refreshToken": refresh_token}

    response = rq.get(auth_url, headers=auth_headers)

    return response


def get_revenue_api_dataframe(report_name, start_date, app_key):
    base_url = revenue_level_params[report_name]["base_url"]

    url = base_url + "appKey={}&date={}".format(app_key, start_date)

    if report_name == "user_level":
        url = url + "&reportType=1"

    authen_response = authenticate()
    response = rq.get(url, auth=BearerAuth(authen_response.json()))

    if "code" in response.json():
        logger.error(response.json())
        raise Exception("ERROR: {}".format(response.json()))

    result = pd.DataFrame()
    for url in response.json()["urls"]:
        url_result = pd.read_csv(url, compression="gzip")
        result = pd.concat([result, url_result], ignore_index=True)
    result["appKey"] = app_key
    result["date"] = start_date
    result["import_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result.to_csv(revenue_level_params[report_name]["temp_file"], index=False)

    logger.info(
        "App {}: Pulled {} rows from {}".format(app_key, len(result), start_date)
    )

    return result


def pull_data_reporting_api(start_date, end_date, report_name):
    if start_date > end_date:
        logger.error(
            "ERROR: start_date({}) is greater than end_date({})".format(
                start_date, end_date
            )
        )
        sys.exit(1)
    else:
        metrics = "revenue,eCPM,impressions,completions,clicks,clickThroughRate,adSourceChecks,adSourceResponses,adSourceAvailabilityRate,impressionPerEngagedSessions"
        breakdown = (
            "date,app,platform,country,adSource,adUnits,instance,att,idfa,abTest"
        )

        is_url = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats?startDate={start_date}&endDate={end_date}&metrics={metrics}&breakdowns={breakdown}".format(
            start_date=start_date,
            end_date=end_date,
            metrics=metrics,
            breakdown=breakdown,
        )

        authen_response = authenticate()
        response = rq.get(is_url, auth=BearerAuth(authen_response.json()))

        if "code" in response.json():
            logger.error(response.json())
            raise Exception("ERROR: {}".format(response.json()))

        result = pd.json_normalize(response.json())
        result = result.explode("data")
        result = pd.concat(
            [result.drop(["data"], axis=1), result["data"].apply(pd.Series)], axis=1
        )
        result["import_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        result = result[
            [
                "date",
                "appKey",
                "platform",
                "adUnits",
                "att",
                "idfa",
                "abTest",
                "instanceName",
                "instanceId",
                "bundleId",
                "appName",
                "providerName",
                "revenue",
                "eCPM",
                "impressions",
                "videoCompletions",
                "clicks",
                "impressionPerEngagedSessions",
                "adSourceChecks",
                "adSourceResponses",
                "adSourceAvailabilityRate",
                "clickThroughRate",
                "countryCode",
                "import_time",
            ]
        ]
        result.to_csv(revenue_level_params[report_name]["temp_file"], index=False)
        return result
