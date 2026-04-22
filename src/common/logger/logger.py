# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import logging
import os

dfa_loggers: dict[str, logging.Logger] = {}
OCI_SDK_LOGGER_NAMES = (
    "oci",
    "oci.base_client",
    "oci.circuit_breaker",
    "oci.circuit_breaker.circuit_breaker",
    "oci.retry",
)


class Logger:
    __logger = None

    def __init__(self, app_name):
        if dfa_loggers.get(app_name):
            self.__logger = dfa_loggers[app_name]
        else:
            self.__logger = logging.getLogger(app_name)
            # Configure log level from environment; default to INFO
            level_name = os.getenv("DFA_LOG_LEVEL", "INFO").upper()
            level = getattr(logging, level_name, logging.INFO)
            self.__logger.setLevel(level)
            # Prevent duplicate propagation to root
            self.__logger.propagate = False
            # Add a stream handler only once
            if not self.__logger.handlers:
                sh = logging.StreamHandler()
                sh.setLevel(level)
                sh_formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
                sh.setFormatter(sh_formatter)
                self.__logger.addHandler(sh)

            oci_level_name = os.getenv("DFA_OCI_LOG_LEVEL", "WARNING").upper()
            oci_level = getattr(logging, oci_level_name, logging.WARNING)
            for oci_logger_name in OCI_SDK_LOGGER_NAMES:
                oci_logger = logging.getLogger(oci_logger_name)
                oci_logger.setLevel(oci_level)

            dfa_loggers[app_name] = self.__logger

    def get_logger(self):
        return self.__logger
