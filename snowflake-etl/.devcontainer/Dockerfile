FROM mcr.microsoft.com/devcontainers/miniconda:0-3

USER vscode
WORKDIR /home/vscode

# Configure Snowflake CLI
RUN mkdir .snowflake
COPY .devcontainer/config.toml .snowflake/

# Create the conda environment
COPY environment.yml .
RUN conda env create \
    && conda init \
    && rm environment.yml

# Install Snowflake CLI
RUN pip install snowflake-cli
