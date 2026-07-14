def call(String message) {
  def color = message.startsWith('SUCCESS') ? 'good' : 'danger'
  slackSend(color: color, message: "${message}\n${env.BUILD_URL}")
}
