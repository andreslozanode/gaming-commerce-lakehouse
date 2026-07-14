def call(String cloud, String environment) {
  dir('infra/terraform') {
    sh """
      terraform init -reconfigure -backend-config=envs/${environment}/backend.${cloud}.hcl
      terraform plan -out=tfplan -var="cloud=${cloud}" -var-file=envs/${environment}/terraform.tfvars
      terraform show -no-color tfplan > tfplan.txt
    """
    archiveArtifacts artifacts: 'infra/terraform/tfplan.txt', allowEmptyArchive: true
    sh 'terraform apply -auto-approve tfplan'
  }
}
