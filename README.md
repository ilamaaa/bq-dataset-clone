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


**Flow**
- Under FROM_PROJECT, get datasets need to be cloned
- For each one of the above datasets, extract all the BigQuery tables into a Cloud Storage bucket in the same region as the tables. (Recommend Avro format for best fidelity in data types and fastest loading speed.)
- Run a storage transfer job to copy the extracted files from the starting location bucket to a bucket in the destination location (TO_BUCKET).
- Load all the files into a BigQuery dataset located in the destination location under TO_PROJECT.
