## SPDX-License-Identifier: Apache-2.0

# Stage 0
## Set up and compilation stage
FROM rustlang/rust:nightly
ENV tempPkgs='\
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    curl \
    file \
    capnproto \
    '
WORKDIR /home/
COPY . ./vega
RUN set -e; \
    apt-get update -yq; \
    apt-get install -yq --no-install-recommends $tempPkgs; \
    mkdir ./.cargo; \
    cp ./vega/docker/config ./.cargo/config; \
    set -e; cd ./vega; \
    # Build executables
    cargo build
# # cargo build --release --examples; \
# cp config_files/hosts.conf /root/; \
# # mkdir /home/release; \
# # # Copy all examples binaries
# # find ./target/release/examples -exec file {} \; \
# # | grep "shared object" \
# # | cut -d: -f1 \
# # | grep -v .*- \
# # | xargs -I{} cp "{}" /home/release

# # Stage 1
# ## Self-contained build with only necessary utils and binaries
# FROM ubuntu:18.04
# WORKDIR /home/release
# RUN set -e; \
#     # Install requirements
#     apt-get update -yq; \
#     apt-get install --no-install-recommends -yq \
#     locales iputils-ping capnproto openssh-server libssl-dev; \
#     # Locales
#     locale-gen en_US.UTF-8; \
#     # Set SSH user
#     groupadd ns && useradd -ms /bin/bash -g ns vega_user; \
#     # Cleanup
#     #apt-get purge -y --auto-remove $tempPkgs; \
#     apt-get autoremove -q -y; \
#     apt-get clean -yq; \
#     rm -rf /var/lib/apt/lists/*
# COPY --from=building /home/release .
# COPY --chown=vega_user:ns ./docker/id_rsa.pub /home/vega_user/.ssh/authorized_keys
# COPY ./docker/id_rsa /root/.ssh/ 
# RUN chmod 600 /root/.ssh/id_rsa /home/vega_user/.ssh/authorized_keys
# ENV LANG=en_US.UTF-8  \
#     LANGUAGE=en_US:en \
#     LC_ALL=en_US.UTF-8
