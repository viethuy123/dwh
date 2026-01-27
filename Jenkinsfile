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

        stage('2. Smart CI/CD Airflow') {
            when { 
                anyOf {
                    changeset "airflow/**"
                    changeset "dags/**"
                }
            }
            steps {
                dir('airflow') {
                    script {
                        try {
                            // Kiểm tra thay đổi hệ thống (Linux-safe)
                            def systemChange = sh(
                                script: '''
                                    git diff --name-only HEAD~1 HEAD 2>/dev/null | grep -iE "Dockerfile|requirements.txt|docker-compose" >/dev/null 2>&1
                                ''',
                                returnStatus: true
                            ) == 0

                            if (systemChange) {
                                echo "==> 🔧 Phát hiện thay đổi Requirements/Dockerfile"
                                echo "==> 🐳 Đang rebuild Docker images..."
                                
                                sh "docker-compose down"
                                sh "docker-compose build --no-cache"
                                sh "docker-compose up -d"
                                
                                echo "==> ⏳ Chờ Airflow khởi động..."
                                sh "sleep 30"
                            } else {
                                echo "==> 📝 Chỉ thay đổi Code (DAGs/Scripts)"
                                echo "==> 🔄 Restart services để nhận DAG mới..."
                                
                                sh "docker-compose restart airflow-scheduler airflow-worker"
                                sh "sleep 10"
                            }

                            // Health check
                            echo "==> ✅ Kiểm tra trạng thái services..."
                            sh "docker-compose ps"
                            
                        } catch (Exception e) {
                            error("❌ Airflow deployment thất bại: ${e.message}")
                        }
                    }
                }
            }
        }

        stage('3. Other Services') {
            parallel {
                stage('MySQL') {
                    when { changeset "mysql/**" }
                    steps { 
                        dir('mysql') {
                            script {
                                try {
                                    echo "==> 🐬 Deploying MySQL..."
                                    sh "docker-compose up -d"
                                    sh "sleep 5"
                                    sh "docker-compose ps"
                                } catch (Exception e) {
                                    error("❌ MySQL deployment thất bại: ${e.message}")
                                }
                            }
                        }
                    }
                }
                
                stage('Postgres') {
                    when { changeset "postgre/**" }
                    steps { 
                        dir('postgre') {
                            script {
                                try {
                                    echo "==> 🐘 Deploying PostgreSQL..."
                                    sh "docker-compose up -d"
                                    sh "sleep 5"
                                    sh "docker-compose ps"
                                } catch (Exception e) {
                                    error("❌ PostgreSQL deployment thất bại: ${e.message}")
                                }
                            }
                        }
                    }
                }

                stage('Metabase') {
                    when { changeset "metabase/**" }
                    steps { 
                        dir('metabase') {
                            script {
                                try {
                                    echo "==> 📊 Deploying Metabase..."
                                    sh "docker-compose up -d"
                                } catch (Exception e) {
                                    error("❌ Metabase deployment thất bại: ${e.message}")
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('4. Final Health Check') {
            steps {
                script {
                    echo "==> 🏥 Kiểm tra tổng quan hệ thống..."
                    sh '''
                        echo "=========================================="
                        echo "Docker Containers Status:"
                        echo "=========================================="
                        docker ps --filter "name=dwh" --format "table {{.Names}}\\t{{.Status}}\\t{{.Ports}}"
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