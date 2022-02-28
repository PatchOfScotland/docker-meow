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
    libpq-dev \
    make

RUN python3 -m pip install pip --upgrade && \ 
    python3 -m pip install \
    wheel \
#    nbcovert \
    nbformat \
    mig-meow \
    papermill \
    setuptools \
    paramiko \
    notebook-parameterizer \
    ipykernel

RUN mkdir /scripts /results && \
    cd /scripts

COPY mig_meow/MANIFEST.in /usr/local/lib/python3.6/dist-packages/woop/
