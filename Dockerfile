# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM python:3.9-slim

RUN apt-get update
RUN apt-get install -y --no-install-recommends
RUN rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip

ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
#COPY .pylintrc /usr/src/app/
#ENV PYLINTRC=/usr/src/app/.pylintrc

RUN pip install --no-cache-dir -r requirements.txt
