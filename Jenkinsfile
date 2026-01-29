pipeline {
    agent any

    environment {
        COMPOSE_PROJECT_NAME = 'dwh-pipeline'
    }

    stages {
        stage('1. Checkout Code') {
            steps {
                checkout scm
                script {
                    sh 'git log -1 --pretty=format:"Latest commit: %h - %s"'
                }
            }
        }

        stage('2. Deploy Jenkins') {
            when { changeset "jenkin/**" }
            steps {
                script {
                    try {
                        dir('jenkin') {
                            echo "==> 🔧 Deploying Jenkins..."
                            sh "docker-compose up -d --build"
                            sh "sleep 10"
                            sh "docker-compose ps"
                        }
                    } catch (Exception e) {
                        error("❌ Jenkins deployment thất bại: ${e.message}")
                    }
                }
            }
        }

        stage('3. Smart CI/CD Airflow') {
            when { 
                changeset "airflow/**"
            }
            steps {
                script {
                    try {
                        // Di chuyển vào folder airflow
                        dir('airflow') {
                            // Kiểm tra thay đổi hệ thống
                            def systemChange = sh(
                                script: '''
                                    cd /var/jenkins_home/workspace/git-ci/airflow
                                    git diff --name-only HEAD~1 HEAD 2>/dev/null | grep -E "^airflow/(Dockerfile|requirements.txt|docker-compose)" >/dev/null 2>&1
                                ''',
                                returnStatus: true
                            ) == 0

                            if (systemChange) {
                                echo "==> 🔧 Phát hiện thay đổi Requirements/Dockerfile trong airflow/"
                                echo "==> 🐳 Đang rebuild Docker images..."
                                
                                sh "docker-compose down"
                                sh "docker-compose build --no-cache"
                                sh "docker-compose up -d"
                                
                                echo "==> ⏳ Chờ Airflow khởi động..."
                                sh "sleep 30"
                            } else {
                                echo "==> 📝 Chỉ thay đổi Code (DAGs/Scripts) trong airflow/"
                                echo "==> 🔄 Restart services để nhận DAG mới..."
                                
                                sh "docker-compose restart airflow-scheduler airflow-worker || docker-compose up -d"
                                sh "sleep 10"
                            }

                            // Health check
                            echo "==> ✅ Kiểm tra trạng thái Airflow services..."
                            sh "docker-compose ps"
                        }
                    } catch (Exception e) {
                        error("❌ Airflow deployment thất bại: ${e.message}")
                    }
                }
            }
        }

        stage('4. Other Services') {
            parallel {
                stage('MySQL') {
                    when { changeset "mysql/**" }
                    steps { 
                        script {
                            try {
                                dir('mysql') {
                                    echo "==> 🐬 Deploying MySQL..."
                                    sh "docker-compose up -d"
                                    sh "sleep 5"
                                    sh "docker-compose ps"
                                }
                            } catch (Exception e) {
                                error("❌ MySQL deployment thất bại: ${e.message}")
                            }
                        }
                    }
                }
                
                stage('Postgres') {
                    when { changeset "postgre/**" }
                    steps { 
                        script {
                            try {
                                dir('postgre') {
                                    echo "==> 🐘 Deploying PostgreSQL..."
                                    sh "docker-compose up -d"
                                    sh "sleep 5"
                                    sh "docker-compose ps"
                                }
                            } catch (Exception e) {
                                error("❌ PostgreSQL deployment thất bại: ${e.message}")
                            }
                        }
                    }
                }

                stage('Metabase') {
                    when { changeset "metabase/**" }
                    steps { 
                        script {
                            try {
                                dir('metabase') {
                                    echo "==> 📊 Deploying Metabase..."
                                    sh "docker-compose up -d"
                                    sh "sleep 5"
                                    sh "docker-compose ps"
                                }
                            } catch (Exception e) {
                                error("❌ Metabase deployment thất bại: ${e.message}")
                            }
                        }
                    }
                }
            }
        }

        stage('5. Final Health Check') {
            steps {
                script {
                    echo "==> 🏥 Kiểm tra tổng quan hệ thống..."
                    sh '''
                        echo "=========================================="
                        echo "Tất cả Docker Containers đang chạy:"
                        echo "=========================================="
                        docker ps --format "table {{.Names}}\\t{{.Status}}\\t{{.Ports}}" | grep -E "airflow|mysql|postgres|metabase|minio" || echo "Không có service nào đang chạy"
                    '''
                }
            }
        }
    }

    post {
        success {
            echo "✅ =========================================="
            echo "✅ DEPLOYMENT THÀNH CÔNG!"
            echo "✅ =========================================="
        }
        failure {
            echo "❌ =========================================="
            echo "❌ DEPLOYMENT THẤT BẠI!"
            echo "❌ Vui lòng kiểm tra logs phía trên"
            echo "❌ =========================================="
        }
        always {
            echo "==> 🧹 Dọn dẹp workspace (nếu cần)..."
        }
    }
}