FROM ubuntu:18.04



# Get dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    locate \
    nano \
    sudo \
    python3 \
    python3-dev \
    python3-pip \
    python3-venv \
    python3-setuptools \
    python3-wheel \
    gcc \
    libpq-dev

RUN pip3 install \
    wheel

RUN pip3 install \
    mig-meow