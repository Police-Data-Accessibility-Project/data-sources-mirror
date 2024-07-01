#!/usr/bin/env groovy

/*
This script runs all health monitoring actions
*/

pipeline {

    agent {
        dockerfile {
            filename 'Dockerfile'
            args '-e DO_DATABASE_URL=$DO_DATABASE_URL -e AIRTABLE_TOKEN=$AIRTABLE_TOKEN -e AIRTABLE_BASE_ID=$AIRTABLE_BASE_ID'
        }
    }

    stages {
        stage('Run Data Soruces Mirror') {
            steps {
                echo 'Running Data Sources Mirror...'
                sh 'python mirror.py'
            }
        }
    }
}