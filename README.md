# bq-dataset-clone
**Modified from @tswast's answer to**
https://stackoverflow.com/questions/32767245/how-to-best-handle-data-stored-in-different-locations-in-google-bigquery

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


clones BigQuery dataset(s) to a different project in different location.

**Setup**
- GCloud Authentication
https://cloud.google.com/docs/authentication/getting-started

- Make sure this service account has access to both projects.

To grant cross project access you just need to add this initial service account email to both projects:
https://stackoverflow.com/questions/35479025/cross-project-management-using-service-account

- Update all the TODOs in the script


**Workflow**
- In FROM_PROJECT, get datasets in migration scope
- Extract all tables under these datasets to a Google Cloud Storage bucket (FROM_BUCKET)
- Transfer these tables into a Google Cloud Storage bucket in another location (TO_BUCKET)
- Load these tables to a different BigQuery project (TO_PROJECT)




