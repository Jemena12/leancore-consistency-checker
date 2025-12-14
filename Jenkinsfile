pipeline {
    agent {
        docker {
            image 'python:3.11-slim'
            args '-u root'
        }
    }

    triggers {
        cron('''
            0 7 * * *
            0 12 * * *
            0 17 * * *
        ''')
    }

    environment {
        MONGO_URL        = credentials('MONGO_URL')
        DATABASE_NAME    = credentials('DATABASE_NAME')
        STOP_ID          = credentials('STOP_ID')
        YOYO_ID          = credentials('YOYO_ID')
        RESEND_API_KEY   = credentials('RESEND_API_KEY')
        EMAIL_FROM       = credentials('EMAIL_FROM')
        EMAIL_TO         = credentials('EMAIL_TO')
    }

    stages {

        stage('Checkout') {
            steps {
                git branch: 'main',
                    url: 'https://github.com/Jemena12/leancore-consistency-checker.git'
            }
        }

        stage('Install dependencies') {
            steps {
                sh '''
                    python --version
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
            }
        }

        stage('Run Scripts') {
            steps {
                script {
                    def hour = new Date().format("H", TimeZone.getTimeZone('America/Bogota'))

                    if (hour == "7") {
                        sh '''
                            python main.py
                            python pagos_no_aplicados.py
                            python mora_saldo_cero.py
                        '''
                    }

                    if (hour == "12" || hour == "17") {
                        sh '''
                            python mora_saldo_cero.py
                        '''
                    }
                }
            }
        }
    }
}
