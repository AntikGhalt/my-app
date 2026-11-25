#!/usr/bin/env bash
# One-off Google Cloud setup so the GitHub workflow can write to Google Drive
# without any stored key (Workload Identity Federation).
#
# Run it in Cloud Shell (https://shell.cloud.google.com), which is free:
#
#   bash setup_google_drive.sh <project-id> [owner/repo] [repository-id]
#
# Access is tied to the repository's numeric id, which survives renames.
# Nothing here needs a billing account: IAM, STS and the Drive API are free.
# It is safe to run again, e.g. to move an older name-based setup to the id.
set -euo pipefail

PROJECT="${1:?usage: setup_google_drive.sh <project-id> [owner/repo]}"
REPO="${2:-AntikGhalt/sdmx-pipeline}"
# From https://api.github.com/repos/<owner>/<repo> ("id").
REPO_ID="${3:-1109487059}"
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

# Only tokens issued for this repository (by id, so renames are fine).
MAPPING="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_id=assertion.repository_id"
CONDITION="assertion.repository_id=='${REPO_ID}'"
if gcloud iam workload-identity-pools providers describe "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" >/dev/null 2>&1; then
  gcloud iam workload-identity-pools providers update-oidc "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" \
    --attribute-mapping="$MAPPING" --attribute-condition="$CONDITION"
else
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" \
    --display-name="GitHub repository ${REPO_ID}" \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="$MAPPING" --attribute-condition="$CONDITION"
fi

POOL_PATH="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}"
gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_PATH}/attribute.repository_id/${REPO_ID}" \
  >/dev/null
# Drop the older name-based grant, if an earlier version of this script made it.
gcloud iam service-accounts remove-iam-policy-binding "$SA" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_PATH}/attribute.repository/${REPO}" \
  >/dev/null 2>&1 || true

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
