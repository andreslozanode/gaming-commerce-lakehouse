// Keyless auth from Jenkins. GCP: WIF via the OIDC plugin. Azure: federated credential.
def call(String cloud) {
  if (cloud == 'gcp') {
    withCredentials([string(credentialsId: 'gcp-wif-provider', variable: 'WIF_PROVIDER'),
                     string(credentialsId: 'gcp-pipeline-sa', variable: 'SA_EMAIL')]) {
      sh '''
        gcloud iam workload-identity-pools create-cred-config "$WIF_PROVIDER" \
          --service-account="$SA_EMAIL" --output-file=/tmp/gcp-creds.json \
          --credential-source-file=/tmp/oidc-token
        gcloud auth login --cred-file=/tmp/gcp-creds.json
      '''
    }
  } else {
    withCredentials([string(credentialsId: 'azure-oidc-client-id', variable: 'AZ_CLIENT'),
                     string(credentialsId: 'azure-tenant-id', variable: 'AZ_TENANT'),
                     string(credentialsId: 'azure-subscription-id', variable: 'AZ_SUB')]) {
      sh '''
        az login --service-principal -u "$AZ_CLIENT" -t "$AZ_TENANT" --federated-token "$(cat /tmp/oidc-token)"
        az account set --subscription "$AZ_SUB"
      '''
    }
  }
}
