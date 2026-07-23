#!/usr/bin/env python3
"""Create a consolidated ADW connection secret from the four legacy secrets.

Example:
    PYTHONPATH=src python scripts/migrate_adw_connection_secret.py \
      --secret-name dfa_adw_connection --application-id ocid1.fnapp.oc1..example \
      --config-file config.ini --section DEFAULT

The script never overwrites an existing target secret. It always writes the
target secret's OCID into the specified Function application's configuration.
"""

import argparse

from common.ocihelpers.function import DfaSetupADWFunctionConfigs
from common.ocihelpers.vault import AdwSecrets
from dfa.bootstrap.envvars import bootstrap_local_machine_environment_variables


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secret-name", required=True, help="Name for the new consolidated Vault secret.")
    parser.add_argument("--application-id", required=True, help="OCID of the Function application to update.")
    parser.add_argument("--config-file", help="Optional local DFA config.ini file to load before migration.")
    parser.add_argument("--section", help="Optional config.ini section (requires --config-file).")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.section and not args.config_file:
        raise ValueError("--section requires --config-file")
    if args.config_file:
        bootstrap_local_machine_environment_variables(args.config_file, args.section)

    secrets = AdwSecrets()
    if secrets._connection_secret_exists(args.secret_name):
        print(f"Using existing consolidated ADW connection secret {args.secret_name!r}.")
    else:
        secrets._create_connection_secret(
            args.secret_name,
            dfa_user_password=secrets.get_dfa_user_password(),
            wallet=secrets.get_wallet(),
            wallet_password=secrets.get_wallet_password(),
            ewallet_pem=secrets.get_ewallet_pem(),
        )
        print("Created consolidated ADW connection secret.")

    secret_ocid = secrets._get_secret_ocid_by_name(args.secret_name)
    DfaSetupADWFunctionConfigs().add_connection_secret_to_configuration(args.application_id, secret_ocid)
    print(f"DFA_ADW_CONNECTION_SECRET_OCID={secret_ocid}")
    print(f"Updated Function application {args.application_id}.")


if __name__ == "__main__":
    main()
