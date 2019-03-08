# bq-dataset-clone
_Modified from @tswast's answer to
https://stackoverflow.com/questions/32767245/how-to-best-handle-data-stored-in-different-locations-in-google-bigquery_


**Setup**
- GCloud Authentication
https://cloud.google.com/docs/authentication/getting-started

- Make sure this service account has access to both projects.

To grant cross project access you just need to add this initial service account email to both projects:
https://stackoverflow.com/questions/35479025/cross-project-management-using-service-account

- Update all the TODOs in the script


**Workflow**
- Under FROM_PROJECT, get datasets in migration scope
- Extract all tables under these datasets to a Google Cloud Storage bucket (FROM_BUCKET)
- Transfer these tables into a Google Cloud Storage bucket in another location (TO_BUCKET)
- Load these tables to a different BigQuery project (TO_PROJECT)
