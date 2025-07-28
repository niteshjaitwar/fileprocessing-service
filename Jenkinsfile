#!/usr/bin/env groovy
// SEE DOCUMENTATION HERE: <https://bitbucket.es.ad.adp.com/projects/ESIIBPM/repos/esiibpm-jenkins-library/browse>
@Library(['adp-jenkins@develop','esiibpm-jenkins-library@develop-ocp'])

config = [
    appName: 'ahub-fileprocessing-service',
    version: 'release-25.07.01.00',
    mavenImage: 'docker.artifactory.us.caas.oneadp.com/innerspace/maven:3.9.9-jdk-17-chainguard',
    target: "ocp-c1degt0105",
    repository: 'docker.artifactory.us.caas.oneadp.com/ibpm/${deploy_project_name}',
	imageRegistry: 'docker.artifactory.us.caas.oneadp.com',
	registryCredentials: 'svc_esiibpm_cicd'
]

automationhubMicroserviceBuild(config)
