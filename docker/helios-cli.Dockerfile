FROM quay.io/jupyter/base-notebook:latest

USER root
RUN apt update && \
    apt install --no-install-recommends --yes build-essential git && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*
USER ${NB_USER}

# Keep compiler toolchain in conda env.
RUN mamba install -n base -c conda-forge gcc gxx cmake ninja

# Use upstream dependency matrix for compatibility.
COPY --chown=${NB_UID} environment-dev.yml ${HOME}/
RUN mamba env update -n base --file ${HOME}/environment-dev.yml && \
    mamba clean -a -q -y && \
    rm ${HOME}/environment-dev.yml

WORKDIR ${HOME}/helios
COPY --chown=${NB_UID} . ${HOME}/helios

# Build only native CLI executable (no python bindings/docs) to reduce memory pressure.
RUN cmake -S . -B build \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_PYTHON=OFF \
      -DBUILD_DOCS=OFF && \
    cmake --build build --target helios++ -j1
