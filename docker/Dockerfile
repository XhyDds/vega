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
    cd /home
RUN wget https://javadl.oracle.com/webapps/download/AutoDL?BundleId=248219_ce59cff5c23f4e2eaf4e778a117d4c5b -O jdk-8u371-linux-x64.tar.gz; \
    tar -xzvf jdk-8u371-linux-x64.tar.gz; \
    rm jdk-8u371-linux-x64.tar.gz
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz; \
    tar -xzvf hadoop-3.3.5.tar.gz; \
    rm hadoop-3.3.5.tar.gz
RUN echo """ \
    export JAVA_HOME=/home/jdk1.8.0_371 \
    export PATH=$JAVA_HOME/bin:$PATH \
    export HADOOP_HOME=/home/hadoop-3.3.5 \
    export PATH=.:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH \
    """ >> /root/.bashrc; \
    source /root/.bashrc; \
    echo "export CLASSPATH=${HADOOP_HOME}/etc/hadoop:`find ${HADOOP_HOME}/share/hadoop/ | awk '{path=path":"$0}END{print path}'`">>/root/.bashrc; \
    echo "export LD_LIBRARY_PATH='${HADOOP_HOME}/lib/native':$LD_LIBRARY_PATH" >> /root/.bashrc; \
    source /root/.bashrc; \
    mkdir ./.cargo; \
    cp ./vega/docker/config ./.cargo/config; \
    set -e; cd ./vega; \
    # Build executables
    cargo build --release --examples; \
    # # cargo build --release --examples; \
    cp config_files/hosts.conf /root/ 
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
