# This Dockerfile has two uses:
#
#   1) As the Dockerfile referenced in docker-compose.yaml in support of a dev/test environment
#   2) As the Dockerfile used to build .deb and .rpm packages for both AMD64 and ARM64 architectures

# Note:
#   A regression had previously been reported in moving from Golang 1.25 to 1.26
#   leading to the pinning of Golang at 1.25.10 and AIStore at v1.4.3... Several
#   subsequent dependencies were required to be rolled back that themselves
#   depended on Golang 1.26 as well.

ARG AIStoreVersion=v1.4.7
ARG GolangVersion=1.26.4
ARG RustVersion=1.93.0
ARG TimeZone=America/Los_Angeles

FROM ubuntu:24.04 AS base

ARG TimeZone

RUN    apt-get update \
    && apt-get dist-upgrade -y

RUN ln -snf /usr/share/zoneinfo/${TimeZone} /etc/localtime
RUN echo ${TimeZone} > /etc/timezone

RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y tzdata

FROM base AS buildable

ARG GolangVersion
ARG RustVersion
ARG TARGETARCH

RUN    apt-get update \
    && apt-get install -y \
                        build-essential \
                        curl \
                        debhelper \
                        devscripts \
                        dnsutils \
                        fio \
                        fuse3 \
                        git \
                        git-lfs \
                        iputils-ping \
                        jq \
                        libaio1t64 \
                        libaio-dev \
                        libboost-all-dev \
                        libfuse3-dev \
                        librpm-dev \
                        lsof \
                        make \
                        just \
                        rpm \
                        s3cmd \
                        tini \
                        tree \
                        unzip \
                        vim \
                        wget \
                        zip \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

RUN echo "user_allow_other" >> /etc/fuse.conf

WORKDIR /tmp
RUN GO_ARCH="${TARGETARCH:-$(dpkg --print-architecture)}" && \
    case "${GO_ARCH}" in \
        amd64|arm64) ;; \
        *) echo "Unsupported Go architecture: ${GO_ARCH}" >&2; exit 1 ;; \
    esac && \
    GolangBasename="go${GolangVersion}.linux-${GO_ARCH}.tar.gz" && \
    wget -nv -O "${GolangBasename}" "https://go.dev/dl/${GolangBasename}" && \
    tar -C /usr/local -xzf "${GolangBasename}" && \
    rm -f "${GolangBasename}"
ENV PATH=$PATH:/usr/local/go/bin:/root/go/bin
ENV GOPATH=/root/go
ENV GOBIN=/root/go/bin
ENV GOTOOLCHAIN=local

# WORKDIR /tmp
# RUN git clone --depth 1 --branch master https://github.com/go-delve/delve
# WORKDIR /tmp/delve
# RUN go build github.com/go-delve/delve/cmd/dlv
# RUN cp dlv /usr/local/go/bin/.

WORKDIR /tmp
RUN wget -nv -O rustup-init.sh https://sh.rustup.rs && \
    sh rustup-init.sh -y --default-toolchain "${RustVersion}" && \
    rm -f rustup-init.sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Pre-install the `stable` toolchain (with the components rust-toolchain.toml
# requests) at build time. The repo's rust-toolchain.toml pins `channel =
# "stable"`, so an in-repo `cargo` invocation would otherwise trigger a runtime
# rustup download on first use — and that download is fragile on the container's
# overlay filesystem (it broke, mid-rename, after a host Docker/Colima upgrade).
# Baking it into the image keeps builds fully offline. The pinned ${RustVersion}
# stays the explicit default toolchain.
RUN rustup toolchain install stable --profile minimal --component rustfmt --component clippy

# WORKDIR /tmp
# RUN git clone --depth 1 https://github.com/breuner/elbencho.git
# WORKDIR /tmp/elbencho
# RUN make -j $(nproc)
# RUN make deb
# RUN apt install ./packaging/elbencho*.deb

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

FROM buildable AS ais

ARG AIStoreVersion

WORKDIR /

RUN git clone --depth 1 --branch $AIStoreVersion https://github.com/NVIDIA/aistore.git
WORKDIR /aistore
RUN (. /root/.bashrc_additions && make cli)

RUN echo '#!/bin/bash'                                                                                                   >  /root/deploy_1_1.sh
RUN echo                     'AIS_ENDPOINT="http://ais:8080" scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --aws' >> /root/deploy_1_1.sh
RUN echo 'HOSTNAME_LIST="ais" AIS_ENDPOINT="http://ais:8080" scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --aws' >> /root/deploy_1_1.sh
RUN echo 'sleep infinity'                                                                                                >> /root/deploy_1_1.sh
RUN chmod +x                                                                                                                /root/deploy_1_1.sh

FROM buildable AS fake-gcs

WORKDIR /

RUN git clone --depth 1 --branch v1.52.3 https://github.com/fsouza/fake-gcs-server.git

WORKDIR /fake-gcs-server

RUN go mod download
RUN CGO_ENABLED=0 go build -o /bin/fake-gcs-server .

RUN mkdir /data

EXPOSE 4443

ENTRYPOINT ["/bin/fake-gcs-server", "-data", "/data", "-scheme", "http"]

# FROM buildable AS garage

# WORKDIR /

# # RUN git clone --depth 1 --branch v2.2.0 https://git.deuxfleurs.fr/Deuxfleurs/garage.git
# RUN git clone --depth 1 --branch v2.2.0 https://github.com/deuxfleurs-org/garage.git

# WORKDIR /garage

# RUN cargo build --release

# ENV PATH="/garage/target/release:${PATH}"

# RUN mkdir -p /var/lib/garage/meta
# RUN mkdir -p /var/lib/garage/data

# COPY multi-storage-file-system/garage.toml /etc/garage.toml
# COPY multi-storage-file-system/garage.sh   /root/garage.sh

WORKDIR /root

FROM buildable AS minio

WORKDIR /

RUN git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git

WORKDIR /minio

RUN go build

RUN mkdir /data

FROM buildable AS versity

WORKDIR /

RUN git clone --depth 1 --branch v1.2.0 https://github.com/versity/versitygw.git

WORKDIR /versitygw

RUN make

RUN mkdir /tmp/vgw

FROM buildable AS dev

WORKDIR /multi-storage-client/multi-storage-file-system

COPY --from=ais /aistore/cmd/cli/autocomplete/bash /root/cmd_cli_autocomplete_bash
COPY --from=ais /root/go/bin/ais                   /root/go/bin/ais

RUN echo "source /root/cmd_cli_autocomplete_bash"  >> /root/.bashrc_additions

EXPOSE 9088
