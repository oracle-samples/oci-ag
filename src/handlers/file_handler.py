# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at
# https://oss.oracle.com/licenses/upl/.

import io
import json
from typing import Optional

from common.logger.logger import Logger
from dfa.bootstrap.envvars import bootstrap_base_environment_variables
from dfa.etl.file_transformer import FileTransformer


def handler(ctx, data: Optional[io.BytesIO] = None):
    logger = Logger(__name__).get_logger()
    try:
        cfg = ctx.Config()
        bootstrap_base_environment_variables(cfg)

        if data is None:
            raise ValueError("No request body provided")
        body = json.loads(data.getvalue())

        if "data" not in body:
            raise Exception("Cannot process file - no data provided")

        if "additionalDetails" not in body["data"]:
            raise Exception(
                "Cannot process file - not all of the necessary details have been provided."
            )

        if "resourceName" not in body["data"]:
            raise Exception("Cannot process file - no object name provded.")

        if "bucketName" not in body["data"]["additionalDetails"]:
            raise Exception("Cannot process file - no bucket information provided.")

        if "namespace" not in body["data"]["additionalDetails"]:
            raise Exception("Cannot process file - no namespace information provided.")

        bucket_name = body["data"]["additionalDetails"]["bucketName"]
        object_name = body["data"]["resourceName"]
        namespace = body["data"]["additionalDetails"]["namespace"]

        logger.info("Creating instance of FileTransformer")
        transformer = FileTransformer(namespace, bucket_name, object_name)
        transformer.extract_data()
        transformer.transform_data()
        transformer.load_data()

    except Exception as e:
        logger.exception("File handler caught exception - %s", e)
        raise Exception("File handler exception") from e
