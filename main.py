from ironsrc_utils import *
from bigquery_utils import *
from config import *


if __name__ == "__main__":
    try:
        for report in ["reporting_api", "impression_level", "user_level"]:
            logger.info("Start importing {} data".format(report))
            start_date, end_date = get_start_end_date(report)

            if report == "reporting_api":
                result = pull_data_reporting_api(start_date, end_date, report)
                row_imported = import_to_bigquery(
                    temp_file=revenue_level_params[report]["temp_file"],
                    app_name_list=app_keys_list,
                    df=result,
                    report_name=report,
                )
            else:
                for app in app_keys_list:
                    result = get_revenue_api_dataframe(report, start_date, app)

                    row_imported = import_to_bigquery(
                        temp_file=revenue_level_params[report]["temp_file"],
                        app_name_list=[app],
                        df=result,
                        report_name=report,
                    )
            # remove_duplicate_rows(report)

    except Exception as e:
        logger.info(e, exc_info=True)

        send_email(
            email_config["receiver"],
            e,
        )
