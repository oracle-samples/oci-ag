# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import logging
import os

dfa_loggers: dict[str, logging.Logger] = {}


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
            dfa_loggers[app_name] = self.__logger

    def get_logger(self):
        return self.__logger
