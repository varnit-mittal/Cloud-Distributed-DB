pipeline {
    agent any

    environment {
        REGISTRY = "varn03"              // e.g., varn03 or ghcr.io/your-org
        ANSIBLE_PLAYBOOK = "${WORKSPACE}/ansible/deploy.yml"
        ANSIBLE_INVENTORY = "${WORKSPACE}/ansible/inventory.ini"
        DOCKER_CREDENTIALS_ID = 'dockerhub-creds'   // Jenkins credentials ID 
        MAIL_RECIPIENTS = 'varnit03mittal@gmail.com'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Generate Protobuf Files') {
            steps {
                sh '''

                echo "Installing grpcio-tools..."
                pip install grpcio grpcio-tools protobuf==3.20.3

                echo "Making genProto.sh executable..."
                chmod +x scripts/genProto.sh

                echo "Running genProto.sh..."
                ./scripts/genProto.sh
                '''
            }
        }

        stage('Build Images with docker-compose') {
            steps {
                sh '''
                  echo "Building docker images..."
                  docker compose build
                '''
            }
        }

        stage('Login to Docker Registry') {
            steps {
                withCredentials([usernamePassword(credentialsId: "${DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_PASS')]) {
                    sh '''
                      echo "$DOCKER_PASS" | docker login -u "$DOCKER_USER" --password-stdin
                    '''
                }
            }
        }

        stage('Tag Images') {
            steps {
                script {
                    COMMIT = sh(returnStdout: true, script: "git rev-parse --short HEAD").trim()
                    env.TAG = COMMIT   // store tag globally

                    sh '''
                      echo "Tagging images with ${TAG}..."
                      
                      docker tag kv-controller:latest ${REGISTRY}/kv-controller:${TAG}
                      docker tag kv-repl-worker:latest ${REGISTRY}/kv-repl-worker:${TAG}
                      docker tag kv-worker:latest ${REGISTRY}/kv-worker:${TAG}

                      # Also tag 'latest' for convenience
                      docker tag kv-controller:latest ${REGISTRY}/kv-controller:latest
                      docker tag kv-repl-worker:latest ${REGISTRY}/kv-repl-worker:latest
                      docker tag kv-worker:latest ${REGISTRY}/kv-worker:latest
                    '''
                }
            }
        }

        stage('Push Images') {
            steps {
                sh '''
                  echo "Pushing images to registry..."

                  docker push ${REGISTRY}/kv-controller:${TAG}
                  docker push ${REGISTRY}/kv-repl-worker:${TAG}
                  docker push ${REGISTRY}/kv-worker:${TAG}

                  # Push latest tags too
                  docker push ${REGISTRY}/kv-controller:latest
                  docker push ${REGISTRY}/kv-repl-worker:latest
                  docker push ${REGISTRY}/kv-worker:latest
                ''' 
            }
        }

        stage('Deploy with Ansible') {
            steps {
                script {
                    echo 'Deploying using Ansible...'
                    sh """
                        ansible-playbook -i ${ANSIBLE_INVENTORY} ${ANSIBLE_PLAYBOOK}
                    """
                }
            }
        }
    }

    post {
        success {
            mail to: "${MAIL_RECIPIENTS}",
                 subject: "✅ Jenkins Pipeline SUCCESS: SPE Scientific Calculator",
                 body: "The pipeline executed successfully.\n\n✔ Repo: ${GIT_REPO_URL}\n✔ Image: ${DOCKER_HUB_REPO}\n✔ Deployed successfully on local system."
        }
        failure {
            mail to: "${MAIL_RECIPIENTS}",
                 subject: "❌ Jenkins Pipeline FAILURE: SPE Scientific Calculator",
                 body: "The pipeline failed.\nPlease check the Jenkins logs for more details.\n\nRepo: ${GIT_REPO_URL}"
        }
    }
}
