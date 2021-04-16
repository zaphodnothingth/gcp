# google cloud function which creates a row in survey.questions
# source: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/cloud-sql/mysql/sqlalchemy/main.py
# sql schema must be appropriately setup
# Entry point : add_question
# need Cloud Functions Invoker priv on gcf-admin-robot.iam.gserviceaccount.com	Google Cloud Functions Service Agent
# example payload:
'''
{
    "id": "7Fi72Y9bVtSRbskxemvk",
    "text": "This organization is driven by its stated mission.",
	"qtype": "likert",
    "construct": "purpose"
}
'''

# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging
import os
import json
import sqlalchemy
from flask import Flask, render_template, Response, request


logger = logging.getLogger()

db_config = {
    "pool_size": 5,
    "max_overflow": 2,
    "pool_timeout": 30,  # 30 seconds
    "pool_recycle": 1800,  # 30 minutes
}

db_user = ""
db_pass = ""
db_name = "survey"
db_socket_dir = "/cloudsql"
cloud_sql_connection_name = "culturalengagement-ffd7a:us-east1:culeng"

pool = sqlalchemy.create_engine(
    # Equivalent URL:
    # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=<socket_path>/<cloud_sql_instance_name>
    sqlalchemy.engine.url.URL(
        drivername="mysql+pymysql",
        username=db_user,  # e.g. "my-database-user"
        password=db_pass,  # e.g. "my-database-password"
        database=db_name,  # e.g. "my-database-name"
        query={
            "unix_socket": "{}/{}".format(
                db_socket_dir,  # e.g. "/cloudsql"
                cloud_sql_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
        }
    ),
    **db_config
)


global db
db = pool


def add_question(req):
    data = request.json
    placeholder = ", ".join(["%s"] * len(data))
    stmt = "insert into `{table}` ({columns}) values ({values});".format(table='questions', columns=",".join(data.keys()), values=placeholder)
    
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            conn.execute(stmt, list(data.values()))
    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.
        # [START_EXCLUDE]
        logger.exception(e)
        return Response(
            status=500,
            response="Add question failed. Please check the logs.",
        )
        # [END_EXCLUDE]
    # [END cloud_sql_mysql_sqlalchemy_connection]

    return Response(
        status=200,
        response="Question successfully added!",
    )
