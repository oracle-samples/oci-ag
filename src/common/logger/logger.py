# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import logging

dfa_loggers = {}


class Logger:
    __logger = None

    def __init__(self, app_name):
        if dfa_loggers.get(app_name):
            self.__logger = dfa_loggers[app_name]
        else:
            self.__logger = logging.getLogger(app_name)
            self.__logger.setLevel(logging.INFO)

            sh = logging.StreamHandler()
            sh.setLevel(logging.INFO)
            sh_formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
            sh.setFormatter(sh_formatter)
            self.__logger.addHandler(sh)
            dfa_loggers[app_name] = self.__logger

    def get_logger(self):
        return self.__logger
