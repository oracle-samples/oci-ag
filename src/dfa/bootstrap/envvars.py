# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import configparser
import os
from typing import Mapping, Optional

from common.logger.logger import Logger

logger = Logger(__name__).get_logger()


def bootstrap_base_environment_variables(cfg):
    try:
        logger.info("Setting base environment variables")
        os.environ["DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"] = cfg[
            "DFA_ADW_DFA_USER_PASSWORD_SECRET_NAME"
        ]
        os.environ["DFA_ADW_WALLET_SECRET_NAME"] = cfg["DFA_ADW_WALLET_SECRET_NAME"]
        os.environ["DFA_ADW_WALLET_PASSWORD_SECRET_NAME"] = cfg[
            "DFA_ADW_WALLET_PASSWORD_SECRET_NAME"
        ]
        os.environ["DFA_ADW_EWALLET_PEM_SECRET_NAME"] = cfg["DFA_ADW_EWALLET_PEM_SECRET_NAME"]
        os.environ["DFA_CONN_PROTOCOL"] = cfg["DFA_CONN_PROTOCOL"]
        os.environ["DFA_CONN_HOST"] = cfg["DFA_CONN_HOST"]
        os.environ["DFA_CONN_PORT"] = cfg["DFA_CONN_PORT"]
        os.environ["DFA_CONN_SERVICE_NAME"] = cfg["DFA_CONN_SERVICE_NAME"]
        os.environ["DFA_CONN_RETRY_COUNT"] = cfg["DFA_CONN_RETRY_COUNT"]
        os.environ["DFA_CONN_RETRY_DELAY"] = cfg["DFA_CONN_RETRY_DELAY"]
        os.environ["DFA_SIGNER_TYPE"] = cfg["DFA_SIGNER_TYPE"]
        os.environ["DFA_COMPARTMENT_ID"] = cfg["DFA_COMPARTMENT_ID"]
        os.environ["DFA_NAMESPACE"] = cfg["DFA_NAMESPACE"]
        os.environ["DFA_STREAM_ID"] = cfg["DFA_STREAM_ID"]
        os.environ["DFA_STREAM_SERVICE_ENDPOINT"] = cfg["DFA_STREAM_SERVICE_ENDPOINT"]
        os.environ["DFA_VAULT_ID"] = cfg["DFA_VAULT_ID"]

        if "DFA_RECREATE_DFA_ADW_TABLES" in cfg:
            os.environ["DFA_RECREATE_DFA_ADW_TABLES"] = cfg["DFA_RECREATE_DFA_ADW_TABLES"]
    except Exception as e:
        logger.exception("Bootstrapping base environment variables failed - %s", e)
        raise Exception("Bootstrapping base environment variables failed") from e


def bootstrap_local_machine_environment_variables(
    ini_file_location: Optional[str] = None, section: Optional[str] = None
):
    logger.info("Loading environment vars for local machine testing")

    custom_configs: Mapping[str, str] = {}
    if ini_file_location:
        logger.info("INI file provided - loading custom configs from %s ", ini_file_location)
        config = configparser.ConfigParser()
        config.read(ini_file_location)

        if not section:
            logger.info('No section provided - using "first section"')
            section = config.sections()[0]

        custom_configs = dict(config[section])

    else:
        logger.info("Loading all defaults")

    for key, value in custom_configs.items():
        env_var_name = key.upper()

        os.environ[env_var_name] = value
        logger.info(
            "Overriding %s with custom value (%s) from ini", env_var_name, os.environ[env_var_name]
        )
    os.environ["DFA_SIGNER_TYPE"] = "user"
