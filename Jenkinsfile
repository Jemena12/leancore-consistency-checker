pipeline {
    agent any

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
                    credentialsId: 'github-org-token',
                    url: 'https://github.com/ORG/REPO.git'
            }
        }

        stage('Setup Python') {
            steps {
                sh '''
                    python3 -m venv venv
                    . venv/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
            }
        }

        stage('Run Scripts') {
            steps {
                script {
                    def hour = new Date().format("H", TimeZone.getTimeZone('America/Bogota'))

                    sh '''
                        . venv/bin/activate
                    '''

                    if (hour == "7") {
                        echo "Ejecutando scripts de la jornada 7 AM"
                        sh '''
                            . venv/bin/activate
                            python main.py
                            python pagos_no_aplicados.py
                            python mora_saldo_cero.py
                        '''
                    }

                    if (hour == "12" || hour == "17") {
                        echo "Ejecutando mora_saldo_cero.py"
                        sh '''
                            . venv/bin/activate
                            python mora_saldo_cero.py
                        '''
                    }
                }
            }
        }
    }

    post {
        failure {
            echo "❌ Pipeline falló"
        }
        success {
            echo "✅ Pipeline ejecutado correctamente"
        }
    }
}
