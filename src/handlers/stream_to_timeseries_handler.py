# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import io
import json
from typing import Optional

from common.logger.logger import Logger
from common.ocihelpers.stream import DataEnablementStream
from dfa.bootstrap.envvars import bootstrap_base_environment_variables
from dfa.etl.stream_transformer import StreamTransformer


def handler(ctx, data: Optional[io.BytesIO] = None):
    logger = Logger(__name__).get_logger()
    try:
        cfg = ctx.Config()
        bootstrap_base_environment_variables(cfg)

        if data is None:
            raise ValueError("No request body provided")
        messages = json.loads(data.getvalue())

        logger.info("Decoding connector hub source stream messages")
        messages = DataEnablementStream.decode_connector_hub_source_stream_messages(messages)
        logger.info("Sorting connector hub source stream messages")
        messages = DataEnablementStream.sort_connector_hub_source_stream_messages(messages)

        logger.info("Creating instance of the StreamTransformer")
        transformer = StreamTransformer(is_timeseries=True)
        transformer.transform_messages(messages)
        transformer.load_data()

    except Exception as e:
        logger.exception("Stream to timeseries handler caught exception - %s", e)
        raise Exception("Stream to timeseries handler exception") from e
