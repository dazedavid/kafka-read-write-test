pipeline {
   agent any
   //triggers {
     //   cron('H/2 * * * *')
   // }
   stages {
       stage('Prepare Environment') {
           steps {
               withCredentials([file(credentialsId: 'client.key.pem', variable: 'clientkey'), file(credentialsId: 'client.cer.pem', variable: 'clientcer'), file(credentialsId: 'server.cer.pem', variable: 'servercer')]) {
                   sh "rm -rf client.key.pem"
                   sh "rm -rf client.cer.pem"
                   sh "rm -rf server.cer.pem"
                   sh "cp '$clientkey' client.key.pem"
                   sh "cp '$clientcer' client.cer.pem"
                   sh "cp '$servercer' server.cer.pem"
               }
           }
       }
       stage('run the test') {
           steps {
                   sh "go get -u -d github.com/Shopify/sarama"
                   sh "go get -u -d github.com/olekukonko/tablewriter"
                   sh "go run kafkaconsumerandproducer.go > test.txt"
              publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, escapeUnderscores: false, keepAll: false, reportDir: '', reportFiles: 'index.html', reportName: 'HTML Report', reportTitles: ''])
      }
    }
  }
}
 
