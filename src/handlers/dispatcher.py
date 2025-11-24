# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import io
from typing import Optional

from fdk import response

from common.logger.logger import Logger
from handlers import (
    audit_handler,
    file_handler,
    file_to_timeseries_handler,
    stream_handler,
    stream_to_timeseries_handler,
)


def dispatch(ctx, data: Optional[io.BytesIO] = None):
    logger = Logger(__name__).get_logger()
    function_name = ctx.Config().get("DFA_FUNCTION_NAME")
    logger.info("Running %s", function_name)

    if not function_name:
        logger.error("DFA_FUNCTION_NAME is not set")
        return response.Response(
            ctx,
            status_code=400,
            response_data={"error": "DFA_FUNCTION_NAME is not set"},
            headers={"Content-Type": "application/json"},
        )

    routes = {
        "audit": audit_handler.handler,
        "stream": stream_handler.handler,
        "file": file_handler.handler,
        "stream_to_ts": stream_to_timeseries_handler.handler,
        "file_to_ts": file_to_timeseries_handler.handler,
    }

    handler_fn = routes.get(function_name)
    if handler_fn is None:
        logger.error("Unknown function name %s", function_name)
        return response.Response(
            ctx,
            status_code=400,
            response_data={"error": f"Unknown function {function_name}"},
            headers={"Content-Type": "application/json"},
        )

    handler_fn(ctx, data)

    return response.Response(
        ctx,
        response_data={
            "message": "Data has been transformed and loaded into the data store successfully!"
        },
        headers={"Content-Type": "application/json"},
    )
