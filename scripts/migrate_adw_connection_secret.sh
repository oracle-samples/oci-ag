#!/usr/bin/env bash
# Create a consolidated ADW connection secret from four legacy Vault secrets.
#
# This script intentionally does not read DFA's config.ini.  It uses the OCI
# CLI's normal authentication settings (for example, ~/.oci/config) and takes
# all DFA deployment values as command-line arguments.

set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage:
  scripts/migrate_adw_connection_secret.sh \
    --secret-name dfa_adw_connection \
    --compartment-id ocid1.compartment.oc1..example \
    --vault-id ocid1.vault.oc1..example \
    --region us-ashburn-1 \
    --auth api_key \
    --dfa-user-password-secret-name dfa_adw_dfa_user_password \
    --wallet-secret-name dfa_adw_wallet \
    --wallet-password-secret-name dfa_adw_wallet_password \
    --ewallet-pem-secret-name dfa_adw_ewallet_pem \
    [--profile OCI_PROFILE]

--auth accepts OCI CLI values: api_key, instance_principal, security_token,
instance_obo_user, resource_principal, or oke_workload_identity. The target
secret is never overwritten. Its OCID is printed on completion.
EOF
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_value() {
  [[ $# -ge 2 && -n $2 && $2 != --* ]] || fail "$1 requires a value"
}

SECRET_NAME=
COMPARTMENT_ID=
VAULT_ID=
OCI_REGION=
OCI_AUTH=
DFA_USER_PASSWORD_SECRET_NAME=
WALLET_SECRET_NAME=
WALLET_PASSWORD_SECRET_NAME=
EWALLET_PEM_SECRET_NAME=
OCI_PROFILE=

while [[ $# -gt 0 ]]; do
  case $1 in
    --secret-name) require_value "$1" "${2-}"; SECRET_NAME=$2; shift 2 ;;
    --compartment-id) require_value "$1" "${2-}"; COMPARTMENT_ID=$2; shift 2 ;;
    --vault-id) require_value "$1" "${2-}"; VAULT_ID=$2; shift 2 ;;
    --region) require_value "$1" "${2-}"; OCI_REGION=$2; shift 2 ;;
    --auth) require_value "$1" "${2-}"; OCI_AUTH=$2; shift 2 ;;
    --dfa-user-password-secret-name) require_value "$1" "${2-}"; DFA_USER_PASSWORD_SECRET_NAME=$2; shift 2 ;;
    --wallet-secret-name) require_value "$1" "${2-}"; WALLET_SECRET_NAME=$2; shift 2 ;;
    --wallet-password-secret-name) require_value "$1" "${2-}"; WALLET_PASSWORD_SECRET_NAME=$2; shift 2 ;;
    --ewallet-pem-secret-name) require_value "$1" "${2-}"; EWALLET_PEM_SECRET_NAME=$2; shift 2 ;;
    --profile) require_value "$1" "${2-}"; OCI_PROFILE=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown option: $1" ;;
  esac
done

for required in SECRET_NAME COMPARTMENT_ID VAULT_ID OCI_REGION OCI_AUTH \
  DFA_USER_PASSWORD_SECRET_NAME WALLET_SECRET_NAME \
  WALLET_PASSWORD_SECRET_NAME EWALLET_PEM_SECRET_NAME; do
  [[ -n ${!required} ]] || fail "a required argument is missing ($required)"
done

for command in oci jq base64; do
  command -v "$command" >/dev/null 2>&1 || fail "required command not found: $command"
done

OCI=(oci --region "$OCI_REGION" --auth "$OCI_AUTH")
[[ -n $OCI_PROFILE ]] && OCI+=(--profile "$OCI_PROFILE")

# macOS uses -D while GNU coreutils uses --decode.
decode_base64() {
  if base64 -D </dev/null >/dev/null 2>&1; then
    base64 -D
  else
    base64 --decode
  fi
}

encode_base64() {
  base64 | tr -d '\n'
}

secret_ocid_by_name() {
  local name=$1 ocid
  ocid=$("${OCI[@]}" vault secret list \
    --compartment-id "$COMPARTMENT_ID" --vault-id "$VAULT_ID" --name "$name" \
    --query 'data[0].id' --raw-output)
  [[ -n $ocid && $ocid != null ]] || fail "Vault secret not found: $name"
  printf '%s' "$ocid"
}

secret_bundle_content() {
  "${OCI[@]}" secrets secret-bundle get --secret-id "$1" \
    --query 'data."secret-bundle-content".content' --raw-output
}

legacy_text_secret() {
  secret_bundle_content "$(secret_ocid_by_name "$1")" | decode_base64
}

target_secret_ocid=$("${OCI[@]}" vault secret list \
  --compartment-id "$COMPARTMENT_ID" --vault-id "$VAULT_ID" --name "$SECRET_NAME" \
  --query 'data[0].id' --raw-output)

if [[ -n $target_secret_ocid && $target_secret_ocid != null ]]; then
  printf 'Using existing consolidated ADW connection secret %q.\n' "$SECRET_NAME"
else
  temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/dfa-adw-migration.XXXXXX")
  trap 'rm -rf "$temporary_directory"' EXIT
  legacy_text_secret "$DFA_USER_PASSWORD_SECRET_NAME" > "$temporary_directory/dfa_user_password"
  legacy_text_secret "$WALLET_PASSWORD_SECRET_NAME" > "$temporary_directory/wallet_password"
  legacy_text_secret "$EWALLET_PEM_SECRET_NAME" > "$temporary_directory/ewallet_pem"
  # The legacy wallet secret contains base64 text that was itself stored as a
  # base64 Vault secret.  Decode twice, then encode it once for the JSON bundle.
  wallet=$(secret_bundle_content "$(secret_ocid_by_name "$WALLET_SECRET_NAME")" \
    | decode_base64 | decode_base64 | encode_base64)
  connection_material=$(jq -cn \
    --rawfile dfa_user_password "$temporary_directory/dfa_user_password" \
    --arg wallet "$wallet" \
    --rawfile wallet_password "$temporary_directory/wallet_password" \
    --rawfile ewallet_pem "$temporary_directory/ewallet_pem" \
    '{dfa_user_password: $dfa_user_password, wallet: $wallet, wallet_password: $wallet_password, ewallet_pem: $ewallet_pem}')

  management_endpoint=$("${OCI[@]}" kms management vault get --vault-id "$VAULT_ID" \
    --query 'data."management-endpoint"' --raw-output)
  master_key_id=$("${OCI[@]}" kms management key list --compartment-id "$COMPARTMENT_ID" \
    --endpoint "$management_endpoint" --all | jq -r \
    '[.data[] | select(."display-name" | contains("master")) | .id][0] // empty')
  [[ -n $master_key_id && $master_key_id != null ]] || fail "No Vault key with 'master' in its name was found"

  connection_material_base64=$(printf '%s' "$connection_material" | encode_base64)
  target_secret_ocid=$("${OCI[@]}" vault secret create-base64 \
      --compartment-id "$COMPARTMENT_ID" --vault-id "$VAULT_ID" --key-id "$master_key_id" \
      --secret-name "$SECRET_NAME" \
      --description 'DFA User generated password by the DFA day0 deployment system' \
      --freeform-tags '{"Feature":"Data Feed Analytics(DFA)"}' \
      --secret-content-name dfa_user_base64 --secret-content-stage CURRENT \
      --secret-content-content "$connection_material_base64" --wait-for-state ACTIVE \
      --query 'data.id' --raw-output)
  printf 'Created consolidated ADW connection secret.\n'
fi

printf 'DFA_ADW_CONNECTION_SECRET_OCID=%s\n' "$target_secret_ocid"
