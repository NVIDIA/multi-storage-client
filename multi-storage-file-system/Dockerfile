# This Dockerfile has two uses:
#
#   1) As the Dockerfile referenced in docker-compose.yaml in support of a dev/test environment
#   2) As the Dockerfile used to build .deb and .rpm packages for both AMD64 and ARM64 architectures

FROM ubuntu:24.04 AS base

RUN    apt-get update \
    && apt-get dist-upgrade -y

ARG TimeZone=America/Los_Angeles
RUN ln -snf /usr/share/zoneinfo/${TimeZone} /etc/localtime
RUN echo ${TimeZone} > /etc/timezone

RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y tzdata

FROM base AS buildable

RUN    apt-get update \
    && apt-get install -y \
                        build-essential \
                        curl \
                        dnsutils \
                        fio \
                        fuse3 \
                        git \
                        git-lfs \
                        iputils-ping \
                        jq \
                        libfuse3-dev \
                        librpm-dev \
                        lsof \
                        make \
                        rpm \
                        s3cmd \
                        tini \
                        tree \
                        unzip \
                        vim \
                        wget \
                        zip

RUN echo "user_allow_other" >> /etc/fuse.conf

ARG TARGETARCH
ARG GolangVersion=1.25.4
WORKDIR /tmp
RUN GolangBasename="go${GolangVersion}.linux-${TARGETARCH}.tar.gz"; \
    GolangURL="https://golang.org/dl/${GolangBasename}"; \
    wget -nv ${GolangURL}; \
    tar -C /usr/local -xzf ${GolangBasename}
ENV PATH=$PATH:/usr/local/go/bin

# RUN git clone --depth 1 --branch master https://github.com/go-delve/delve
# WORKDIR /tmp/delve
# RUN go build github.com/go-delve/delve/cmd/dlv
# RUN cp dlv /usr/local/go/bin/.

WORKDIR /tmp

RUN if [ "$TARGETARCH" = "amd64" ]; then AWS_ARCH="x86_64"; else AWS_ARCH="aarch64"; fi && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

WORKDIR /root

RUN  echo '#!/bin/bash'                  >  .bashrc_additions
RUN  echo 'export PS1="\w$ "'            >> .bashrc_additions
RUN  echo 'export GOPATH=/root/go'       >> .bashrc_additions
RUN  echo 'export GOBIN=${GOPATH}/bin'   >> .bashrc_additions
RUN  echo 'export PATH=${GOBIN}:${PATH}' >> .bashrc_additions

RUN  echo ""                             >> .bashrc
RUN  echo ". /root/.bashrc_additions"    >> .bashrc

COPY multi-storage-file-system/minio.s3cfg            .s3cfg
RUN  mkdir                                            .aws
COPY multi-storage-file-system/ais.aws.config         .aws/config
COPY multi-storage-file-system/ais.aws.credentials    .aws/credentials

WORKDIR /multi-storage-client

COPY . .

RUN git config --global --add safe.directory /multi-storage-client

WORKDIR /multi-storage-client/multi-storage-file-system

RUN (. /root/.bashrc_additions && make lint-update)

WORKDIR /

RUN rm -rf /multi-storage-client

FROM buildable AS minio

WORKDIR /

RUN git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git

WORKDIR /minio

RUN go build

RUN mkdir /data

FROM buildable AS ais

WORKDIR /

RUN git clone --depth 1 --branch v1.4.1 https://github.com/NVIDIA/aistore.git

WORKDIR /aistore

RUN (. /root/.bashrc_additions && make cli)

RUN echo '#!/bin/bash'                                                                                                   >  /root/deploy_1_1.sh
RUN echo                     'AIS_ENDPOINT="http://ais:8080" scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --aws' >> /root/deploy_1_1.sh
RUN echo 'HOSTNAME_LIST="ais" AIS_ENDPOINT="http://ais:8080" scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --aws' >> /root/deploy_1_1.sh
RUN echo 'sleep infinity'                                                                                                >> /root/deploy_1_1.sh
RUN chmod +x                                                                                                                /root/deploy_1_1.sh

FROM buildable AS dev

WORKDIR /multi-storage-client/multi-storage-file-system

COPY --from=ais /aistore/cmd/cli/autocomplete/bash /root/cmd_cli_autocomplete_bash
COPY --from=ais /root/go/bin/ais                   /root/go/bin/ais

RUN echo "source /root/cmd_cli_autocomplete_bash"  >> /root/.bashrc_additions

EXPOSE 9088
