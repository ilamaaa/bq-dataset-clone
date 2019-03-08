# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import time
import sys
import time
import os
import googleapiclient.discovery
from google.cloud import bigquery, storage
import json
import pytz
from google.protobuf import descriptor_pb2


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json" # TODO: set this to path of Google Cloud Storage service account json


FROM_PROJECT = ''  # TODO: set this to your project name
FROM_LOCATION = ''  # TODO: set this to the BigQuery location
FROM_BUCKET = ''  # TODO: set to bucket name in same location,
TO_PROJECT = ''
TO_LOCATION = ''  # TODO: set this to the destination BigQuery location
TO_BUCKET = ''  # TODO: set to bucket name in destination loc


bq_client = bigquery.Client.from_service_account_json('gbq_service_account.json') # TODO: set to BigQuery service account.json
transfer_client = googleapiclient.discovery.build('storagetransfer', 'v1')


def extract_tables(FROM_DATASET):
    # Extract all tables in a dataset to a Cloud Storage bucket.
    print('Extracting {}:{} to Cloud Storage bucket {}'.format(
        FROM_PROJECT, FROM_DATASET, FROM_BUCKET))

    tables = list(bq_client.list_tables(bq_client.dataset(FROM_DATASET)))
    extract_jobs = []
    for table in tables:
        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.AVRO
        extract_job = bq_client.extract_table(
            table.reference,
            ['gs://{}/{}.avro'.format(FROM_BUCKET, table.table_id)],
            location=FROM_LOCATION,  # Available in 0.32.0 library.
            job_config=job_config)  # Starts the extract job.
        extract_jobs.append(extract_job)

    for job in extract_jobs:
        job.result()

    return tables


def transfer_buckets():
    # Transfer files from one region to another using storage transfer service.
    print('Transferring Cloud Storage bucket {} to {}'.format(FROM_BUCKET, TO_BUCKET))
    now = datetime.datetime.now(pytz.utc)
    transfer_job = {
        'description': '{}-{}-{}_once'.format(
            FROM_PROJECT, FROM_BUCKET, TO_BUCKET),
        'status': 'ENABLED',
        'projectId': FROM_PROJECT,
        'transferSpec': {
            'transferOptions': {
                'overwriteObjectsAlreadyExistingInSink': True,
            },
            'gcsDataSource': {
                'bucketName': FROM_BUCKET,
            },
            'gcsDataSink': {
                'bucketName': TO_BUCKET,
            },
        },
        # Set start and end date to today (UTC) without a time part to start
        # the job immediately.
        'schedule': {
            'scheduleStartDate': {
                'year': now.year,
                'month': now.month,
                'day': now.day,
            },
            'scheduleEndDate': {
                'year': now.year,
                'month': now.month,
                'day': now.day,
            },
        },
    }
    transfer_job = transfer_client.transferJobs().create(
        body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(transfer_job, indent=4)))

    # Find the operation created for the job.
    job_filter = {
        'project_id': FROM_PROJECT,
        'job_names': [transfer_job['name']],
    }

    # Wait until the operation has started.
    response = {}
    while ('operations' not in response) or (not response['operations']):
        time.sleep(1)
        response = transfer_client.transferOperations().list(
            name='transferOperations', filter=json.dumps(job_filter)).execute()

    operation = response['operations'][0]
    print('Returned transferOperation: {}'.format(
        json.dumps(operation, indent=4)))

    # Wait for the transfer to complete.
    print('Waiting ', end='')
    while operation['metadata']['status'] == 'IN_PROGRESS':
        print('.', end='')
        sys.stdout.flush()
        time.sleep(5)
        operation = transfer_client.transferOperations().get(
            name=operation['name']).execute()

    print('Finished transferOperation: {}'.format(
        json.dumps(operation, indent=4)))


def create_dataset():
    print('Creating destination dataset')
    dataset_ref = bq_client.dataset(TO_DATASET, project=TO_PROJECT)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = TO_LOCATION
    dataset = bq_client.create_dataset(dataset)


def load_tables(tables):
    # Load all tables into the new dataset.
    print('Loading tables from bucket {} to {}:{}'.format(
        TO_BUCKET, FROM_PROJECT, TO_DATASET))
    create_dataset()
    load_jobs = []
    for table in tables:
        dest_dataset = bq_client.dataset(TO_DATASET, project= TO_PROJECT)
        dest_table = dest_dataset.table(table.table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO
        load_job = bq_client.load_table_from_uri(
            ['gs://{}/{}.avro'.format(TO_BUCKET, table.table_id)],
            dest_table,
            location=TO_LOCATION,  # Available in 0.32.0 library.
            job_config=job_config)  # Starts the load job.
        load_jobs.append(load_job)

    for job in load_jobs:
        job.result()
        assert job.state == 'DONE'


def migrate(FROM_DATASET):
    tables = extract_tables(FROM_DATASET)
    transfer_buckets()
    load_tables(tables)


def get_datasets():
    # Get a list of datasets you need if you don't want to migrate all datasets in the project
    datasets = list(bq_client.list_datasets())
    FROM_DATASETS = []
    for dataset in datasets:
        dataset_name = dataset.dataset_id
        if '' in dataset_name.lower():
            # filter datasets containing specific string
            FROM_DATASETS.append(dataset_name)
    return FROM_DATASETS


FROM_DATASETS = get_datasets()

for FROM_DATASET in FROM_DATASETS:
    TO_DATASET = FROM_DATASET
    migrate(FROM_DATASET)
