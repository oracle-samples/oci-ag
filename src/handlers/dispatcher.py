# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import io

from fdk import response

from common.logger.logger import Logger
from handlers import (
    audit_handler,
    file_handler,
    file_to_timeseries_handler,
    stream_handler,
    stream_to_timeseries_handler,
)


def dispatch(ctx, data: io.BytesIO = None):
    logger = Logger(__name__).get_logger()
    function_name = ctx.Config().get("DFA_FUNCTION_NAME")
    logger.info("Running %s", function_name)

    if function_name == "audit":
        audit_handler.handler(ctx, data)
    if function_name == "stream":
        stream_handler.handler(ctx, data)
    if function_name == "file":
        file_handler.handler(ctx, data)
    if function_name == "stream_to_ts":
        stream_to_timeseries_handler.handler(ctx, data)
    if function_name == "file_to_ts":
        file_to_timeseries_handler.handler(ctx, data)

    return response.Response(
        ctx,
        response_data={
            "message": "Data has been transformed and loaded into the data store successfully!"
        },
        headers={"Content-Type": "application/json"},
    )
