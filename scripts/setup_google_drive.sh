#!/usr/bin/env bash
# One-off Google Cloud setup so the GitHub workflow can write to Google Drive
# without any stored key (Workload Identity Federation).
#
# Run it in Cloud Shell (https://shell.cloud.google.com), which is free:
#
#   bash setup_google_drive.sh <project-id> [owner/repo]
#
# Nothing here needs a billing account: IAM, STS and the Drive API are free.
# It is safe to run twice.
set -euo pipefail

PROJECT="${1:?usage: setup_google_drive.sh <project-id> [owner/repo]}"
REPO="${2:-AntikGhalt/my-app}"
SA_NAME="istatsync-drive"
POOL="github"
PROVIDER="github-actions"

SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"

gcloud config set project "$PROJECT" >/dev/null
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')"

echo "Enabling APIs..."
gcloud services enable drive.googleapis.com iam.googleapis.com \
  iamcredentials.googleapis.com sts.googleapis.com

if ! gcloud iam service-accounts describe "$SA" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --display-name="istatsync (Google Drive)"
fi

if ! gcloud iam workload-identity-pools describe "$POOL" --location=global >/dev/null 2>&1; then
  gcloud iam workload-identity-pools create "$POOL" --location=global \
    --display-name="GitHub Actions"
fi

if ! gcloud iam workload-identity-pools providers describe "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" >/dev/null 2>&1; then
  # Only tokens issued for this repository are accepted.
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" \
    --display-name="GitHub ${REPO}" \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
    --attribute-condition="assertion.repository=='${REPO}'"
fi

gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/attribute.repository/${REPO}" \
  >/dev/null

cat <<OUT

Done. Now:

1. In Google Drive, open the shared drive and add this account as
   "Content manager":

     ${SA}

2. In GitHub, ${REPO} > Settings > Secrets and variables > Actions >
   Variables, add:

     GCP_WORKLOAD_IDENTITY_PROVIDER = projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/providers/${PROVIDER}
     GCP_SERVICE_ACCOUNT            = ${SA}
     DRIVE_FOLDER_ID                = <id of the Drive folder, last part of its URL>

OUT
