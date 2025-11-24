# Copyright (c) 2025, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.

FROM fnproject/python:3.12-dev AS build-stage
RUN mkdir -p /function/src
WORKDIR /function
ADD pyproject.toml /function/.
ADD src /function/src
RUN unlink /usr/bin/python3 && ln -s /usr/bin/python3.12 /usr/bin/python3
RUN python --version
RUN python -m pip install --upgrade build && \
    python -m build

FROM fnproject/python:3.12
RUN mkdir -p /function
WORKDIR /function
RUN groupadd --system --gid 1001 dfa && \
    useradd --system --uid 1001 --gid 1001 --no-create-home dfa

COPY --from=build-stage /function/dist /function/dist
COPY --from=build-stage /function/src/handlers/dispatcher.py /function/.
ADD constraints.txt /function/constraints.txt
RUN unlink /usr/bin/python3 && ln -s /usr/bin/python3.12 /usr/bin/python3
RUN python --version
RUN python -m pip install --no-cache-dir -c /function/constraints.txt /function/dist/dfa-0.1.0-py3-none-any.whl fdk

RUN chown -R dfa /function
ENV PYTHONPATH="/function"
USER dfa

ENTRYPOINT ["fdk", "dispatcher.py", "dispatch"]
