# This Dockerfile has two uses:
#
#   1) As the Dockerfile referenced in docker-compose.yaml in support of a dev/test environment
#   2) As the Dockerfile used to build a scratch-based container having the built program
#
# To create a scratch-based container named `mscp_built` having the resultant binary at `/mscp`:
#
#   (cd ../../.. && docker build --file posix/fuse/mscp/Dockerfile --target built --tag mscp_built:$(git describe --tags --always --dirty) .)
#
# To then extract the AMD64 and/or ARM64 binariwes from that container:
#
#   docker create --name mscp_built mscp_built:<tag> --entrypoint /mscp && docker cp mscp_built:/msc-posix-linux-amd64 ./msc-posix-linux-amd64 && docker rm mscp_built
#   docker create --name mscp_built mscp_built:<tag> --entrypoint /mscp && docker cp mscp_built:/msc-posix-linux-arm64 ./msc-posix-linux-arm64 && docker rm mscp_built

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
                        fuse3 \
                        git \
                        iputils-ping \
                        jq \
                        libfuse3-dev \
                        lsof \
                        make \
                        s3cmd \
                        tini \
                        tree \
                        unzip \
                        vim \
                        wget

RUN echo "user_allow_other" >> /etc/fuse.conf

ARG TARGETARCH
ARG GolangVersion=1.25.1
WORKDIR /tmp
RUN GolangBasename="go${GolangVersion}.linux-${TARGETARCH}.tar.gz"; \
    GolangURL="https://golang.org/dl/${GolangBasename}"; \
    wget -nv ${GolangURL}; \
    tar -C /usr/local -xzf ${GolangBasename}
ENV PATH=$PATH:/usr/local/go/bin

# RUN git clone https://github.com/go-delve/delve
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
RUN  echo 'export GOPATH=${HOME}/go'     >> .bashrc_additions
RUN  echo 'export GOBIN=${GOPATH}/bin'   >> .bashrc_additions
RUN  echo 'export PATH=${GOBIN}:${PATH}' >> .bashrc_additions

RUN  echo ""                             >> .bashrc
RUN  echo ". ~/.bashrc_additions"        >> .bashrc

COPY posix/fuse/mscp/minio.s3cfg            .s3cfg
RUN  mkdir                                  .aws
COPY posix/fuse/mscp/ais.aws.config         .aws/config
COPY posix/fuse/mscp/ais.aws.credentials    .aws/credentials

WORKDIR /multi-storage-client

COPY . .

WORKDIR /multi-storage-client/posix/fuse/mscp

RUN git config --global --add safe.directory /multi-storage-client

RUN (. ~/.bashrc_additions && make lint-update)

WORKDIR /

RUN rm -rf /multi-storage-client

FROM buildable AS minio

WORKDIR /

RUN git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git

WORKDIR /minio

RUN go build

RUN mkdir /data

FROM buildable AS ais

WORKDIR /root/.aws

RUN echo "[default]"                          >  config
RUN echo "region = us-east-1"                 >> config

RUN echo "[default]"                          >  credentials
RUN echo "aws_access_key_id = minioadmin"     >> credentials
RUN echo "aws_secret_access_key = minioadmin" >> credentials

WORKDIR /

RUN git clone --depth 1 --branch v1.4.0 https://github.com/NVIDIA/aistore.git

WORKDIR /aistore

# AIStore compile failure on Linux/ARM64 workaround:
#   The steps injected below (after .bashrc_additions & before the make cli step)
#   create distinct versions of fsutils_linux.go for AMD64 and ARM64 such that the
#   compilation on ARM64 uses the correct SYSCALL constant. Once AIstore has applied
#   the final fix (and updated cmd/cli's go.mod to reference it), these steps should
#   be removed (and, of course, the branch tag being checked out updated).
RUN . ~/.bashrc_additions && \
    cd ios && \
    mv fsutils_linux.go       fsutils_linux_amd64.go && \
    cp fsutils_linux_amd64.go fsutils_linux_arm64.go && \
    sed -i 's/SYS_NEWFSTATAT/SYS_FSTATAT/' fsutils_linux_arm64.go && \
    cd ../cmd/cli && \
    echo "replace github.com/NVIDIA/aistore => ../.." >> go.mod && \
    go mod tidy && \
    cd ../.. && \
    make cli

RUN echo '#!/bin/bash'                                                                                                   >  /root/deploy_1_1.sh
RUN echo                     'AIS_ENDPOINT="http://ais:8080" scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --aws' >> /root/deploy_1_1.sh
RUN echo 'HOSTNAME_LIST="ais" AIS_ENDPOINT="http://ais:8080" scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --aws' >> /root/deploy_1_1.sh
RUN echo 'sleep infinity'                                                                                                >> /root/deploy_1_1.sh
RUN chmod +x                                                                                                                /root/deploy_1_1.sh

FROM buildable AS dev

COPY --from=ais /aistore/cmd/cli/autocomplete/bash    /root/cmd_cli_autocomplete_bash
COPY --from=ais /root/go/bin/ais                      /root/go/bin/ais
RUN echo "source ~/cmd_cli_autocomplete_bash"      >> /root/.bashrc_additions

WORKDIR /multi-storage-client/posix/fuse/mscp

FROM buildable AS build

WORKDIR /multi-storage-client

COPY . .

WORKDIR /multi-storage-client/posix/fuse/mscp

RUN (. ~/.bashrc_additions && make build artifacts)

FROM scratch AS built

WORKDIR /

COPY --from=build /multi-storage-client/posix/fuse/mscp/mscp                  /mscp
COPY --from=build /multi-storage-client/posix/fuse/mscp/msc-posix-linux-amd64 /msc-posix-linux-amd64
COPY --from=build /multi-storage-client/posix/fuse/mscp/msc-posix-linux-arm64 /msc-posix-linux-arm64
