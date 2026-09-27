FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

# The upstream image currently contains a Yarn APT source whose signing key is
# unavailable. Dev Container features run apt-get update, so remove only that
# source before the Docker-in-Docker feature is installed.
USER root
RUN set -eux; \
    for source in /etc/apt/sources.list.d/*; do \
        if [ -f "$source" ] && grep -q "dl.yarnpkg.com" "$source"; then \
            rm -f "$source"; \
        fi; \
    done; \
    if [ -f /etc/apt/sources.list ]; then \
        sed -i '/dl\.yarnpkg\.com/d' /etc/apt/sources.list; \
    fi
