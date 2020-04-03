#!/usr/bin/env bash
# before running this script, login to the SDP k8s cluster
# Set this variable to your project name:
PROJECT=music-demo
kubectl get secret ${PROJECT}-pravega -n ${PROJECT} -o jsonpath="{.data.keycloak\.json}" | base64 -d > ~/keycloak.json
chmod go-rw ~/keycloak.json
export pravega_client_auth_method=Bearer
export pravega_client_auth_loadDynamic=true
export KEYCLOAK_SERVICE_ACCOUNT_FILE=${HOME}/keycloak.json
