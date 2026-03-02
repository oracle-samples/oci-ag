# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

import importlib.util
import inspect
import os
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import Any

from common.logger.logger import Logger


class AbstractTransformer(ABC):
    logger = Logger(__name__).get_logger()

    _event_object_type = None
    _operation_type = None
    _event_timestamp = None
    _tenancy_id = None
    _service_instance_id = None
    _raw_events: list[Any] = []
    _prepared_events: list[Any] = []
    _prepared_events_df: Any = None

    def _get_raw_events(self):
        return self._raw_events

    def _append_prepared_event(self, event):
        if event is not None and len(event) > 0:
            if isinstance(event, list):
                self._prepared_events.extend(event)
            else:
                self._prepared_events.append(event)

    def _get_prepared_events(self):
        return self._prepared_events

    def _get_prepared_events_df(self):
        return self._prepared_events_df

    def get_event_object_type(self):
        return self._event_object_type

    def get_operation_type(self):
        return self._operation_type

    def is_valid_object_type(self, object_type):
        valid_object_types = [
            "IDENTITY",
            "CLOUD_GROUP",
            "CLOUD_POLICY",
            "RESOURCE",
            "POLICY_STATEMENT_RESOURCE_MAPPING",
            "GLOBAL_IDENTITY_COLLECTION",
            "ACCESS_BUNDLE",
            "PERMISSION",
            "PERMISSION_ASSIGNMENT",
            "POLICY",
            "ROLE",
            "ACCESS_GUARDRAIL",
            "APPROVAL_WORKFLOW",
            "OWNERSHIP_COLLECTION",
            "ORCHESTRATED_SYSTEM",
        ]

        if object_type in valid_object_types:
            return True

        return False

    @abstractmethod
    def _set_raw_event_data(self, event_data):
        pass

    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def transform_data(self):
        pass

    @abstractmethod
    def load_data(self):
        pass

    # Auto-wrap subclass with runtime logging
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        def _wrap_with_timing(fn_name: str, label: str):
            method = cls.__dict__.get(fn_name)
            if callable(method) and not getattr(method, "__wrapped__", None):

                @wraps(method)
                def _timed(self, *args, **kw):
                    start = perf_counter()
                    try:
                        return method(self, *args, **kw)
                    finally:
                        duration = perf_counter() - start
                        try:
                            self.logger.info("%s runtime: %.3fs", label, duration)
                        except Exception:
                            pass

                setattr(cls, fn_name, _timed)

        # Wrap key methods if overridden in subclass
        _wrap_with_timing("extract_data", "extract_data")
        _wrap_with_timing("transform_data", "transform_data")
        _wrap_with_timing("load_data", "load_data")

    def transformer_factory(self):
        class_name = f"{self.get_event_object_type().lower().title().replace('_', '')}\
{self.get_operation_type().lower().title().replace('_', '')}EventTransformer"
        transformers = Path(__file__).parent / "transformers"

        for file in os.listdir(transformers):
            full_path = os.path.join(transformers, file)
            if os.path.isfile(full_path) and file.endswith(".py") and not file.startswith("__"):
                module_name = file[:-3]  # Remove .py extension
                if module_name.lower() == self.get_event_object_type().lower():
                    try:
                        spec = importlib.util.spec_from_file_location(module_name, full_path)
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        for cls_name, cls_obj in inspect.getmembers(module):
                            if inspect.isclass(cls_obj) and cls_name == class_name:
                                return cls_obj(
                                    self.get_event_object_type(),
                                    self.get_operation_type(),
                                    self.is_timeseries,
                                )
                    except Exception as e:
                        self.logger.error("Error finding %s: %s", class_name, e)
        return None
