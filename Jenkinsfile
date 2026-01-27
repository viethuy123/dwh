pipeline {
    agent any

    environment {
        // Có thể thêm biến môi trường nếu cần
        COMPOSE_PROJECT_NAME = 'dwh-pipeline'
    }

    stages {
        stage('1. Checkout Code') {
            steps {
                checkout scm
                script {
                    // Lấy thông tin commit để log
                    bat 'git log -1 --pretty=format:"Latest commit: %h - %s"'
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
                            // Kiểm tra thay đổi hệ thống (Windows-safe)
                            def systemChange = bat(
                                script: """
                                    @echo off
                                    git diff --name-only HEAD~1 HEAD 2>nul | findstr /I "Dockerfile requirements.txt docker-compose" >nul 2>&1
                                    exit /b %ERRORLEVEL%
                                """,
                                returnStatus: true
                            ) == 0

                            if (systemChange) {
                                echo "==> 🔧 Phát hiện thay đổi Requirements/Dockerfile"
                                echo "==> 🐳 Đang rebuild Docker images..."
                                
                                bat "docker-compose down"
                                bat "docker-compose build --no-cache"
                                bat "docker-compose up -d"
                                
                                echo "==> ⏳ Chờ Airflow khởi động..."
                                bat "timeout /t 30 /nobreak"
                            } else {
                                echo "==> 📝 Chỉ thay đổi Code (DAGs/Scripts)"
                                echo "==> 🔄 Restart services để nhận DAG mới..."
                                
                                bat "docker-compose restart airflow-scheduler airflow-worker"
                                bat "timeout /t 10 /nobreak"
                            }

                            // Health check
                            echo "==> ✅ Kiểm tra trạng thái services..."
                            bat "docker-compose ps"
                            
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
                                    bat "docker-compose up -d"
                                    bat "timeout /t 5 /nobreak"
                                    bat "docker-compose ps"
                                } catch (Exception e) {
                                    error("❌ MySQL deployment thất bại: ${e.message}")
                                }
                            }
                        }
                    }
                }
                
                stage('Postgres') {
                    when { changeset "postgre/**" }  // Hoặc "postgres/**" nếu bạn đổi tên folder
                    steps { 
                        dir('postgre') {
                            script {
                                try {
                                    echo "==> 🐘 Deploying PostgreSQL..."
                                    bat "docker-compose up -d"
                                    bat "timeout /t 5 /nobreak"
                                    bat "docker-compose ps"
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
                                    bat "docker-compose up -d"
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
                    bat """
                        @echo off
                        echo ==========================================
                        echo Docker Containers Status:
                        echo ==========================================
                        docker ps --filter "name=dwh" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
                    """
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
            // Cleanup nếu cần
            echo "==> 🧹 Dọn dẹp workspace (nếu cần)..."
        }
    }
}