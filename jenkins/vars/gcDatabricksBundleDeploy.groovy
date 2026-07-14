def call(String cloud, String environment) {
  withCredentials([string(credentialsId: "databricks-host-${cloud}", variable: 'DATABRICKS_HOST'),
                   string(credentialsId: "databricks-token-${cloud}", variable: 'DATABRICKS_TOKEN')]) {
    dir('databricks') {
      sh """
        databricks bundle validate -t ${environment} --var="cloud=${cloud}"
        databricks bundle deploy   -t ${environment} --var="cloud=${cloud}"
      """
    }
  }
}
