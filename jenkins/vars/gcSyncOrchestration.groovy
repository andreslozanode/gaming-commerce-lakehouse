def call(String cloud, String environment) {
  if (cloud == 'gcp') {
    withCredentials([string(credentialsId: 'composer-dag-bucket', variable: 'DAG_BUCKET')]) {
      sh 'gcloud storage rsync orchestration/airflow/dags gs://$DAG_BUCKET/dags --recursive --delete-unmatched-destination-objects'
    }
  } else {
    echo 'AKS Airflow uses git-sync; DAGs land on the next sync interval.'
  }
  withCredentials([string(credentialsId: 'airbyte-api-token', variable: 'AIRBYTE_API_TOKEN'),
                   string(credentialsId: 'airbyte-api-url', variable: 'AIRBYTE_API_URL'),
                   string(credentialsId: 'airbyte-workspace-id', variable: 'AIRBYTE_WORKSPACE_ID')]) {
    sh "ENVIRONMENT=${environment} python orchestration/airbyte/apply_config.py orchestration/airbyte/connections.yaml"
  }
}
