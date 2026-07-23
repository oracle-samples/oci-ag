# Release notes: 1.1

## Consolidated ADW connection secret

The installer now stores ADW connection material in a single OCI Vault secret instead of four separate secrets.

Previously, installation created individual secrets for:

- DFA user password
- Wallet (`cwallet.sso`)
- Wallet password
- Wallet PEM (`ewallet.pem`)

The installer now creates and uses one consolidated secret, named `<RESOURCE_NAME_PREFIX>_adw_connection` by default. It contains all required connection material and is configured through `DFA_ADW_CONNECTION_SECRET_OCID`.

This simplifies vault management and reduces the number of secrets created during installation. Existing function application configurations are preserved for migration compatibility.

### Upgrade requirement

For an existing deployment, run the migration script before using this release so the deployment is updated to use the consolidated ADW connection secret.

Example:

```bash
PYTHONPATH=src python scripts/migrate_adw_connection_secret.py \
  --secret-name dfa_adw_connection \
  --application-id ocid1.fnapp.oc1..example \
  --config-file config.ini \
  --section DFA
```

Replace the secret name, Function application OCID, configuration file, and section with values for the deployment. The script creates the consolidated secret only when it does not already exist, then configures its OCID on the Function application.
