# katbench image. The tag is derived from a hash of this directory, so any edit here yields a new image.
FROM node:26-trixie-slim@sha256:e6bedef217677b276c591795e169840edd9096dbf13f29df8ef8d1856b8820bc AS node

FROM rust:1.100.0@sha256:e19840879045c951c905d0eaa9887f3439c62000e429963371b376bac8d38c31

RUN apt-get update \
 && apt-get install -y --no-install-recommends fio inotify-tools procps xz-utils \
 && rm -rf /var/lib/apt/lists/*

COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
 && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
 && npm install -g --no-audit --no-fund pnpm@10.34.9 \
 && npm cache clean --force

COPY src/ /usr/local/src/katbench/
RUN cd /usr/local/src/katbench \
 && for p in kb_time kb_stat kb_open; do gcc -O2 -Wall -Werror -o /usr/local/bin/$p $p.c; done

RUN git config --system --add safe.directory '*'

ENV CI=true \
    npm_config_cache=/cache/npm \
    npm_config_update_notifier=false \
    CARGO_HOME=/cache/cargo \
    CARGO_TERM_COLOR=never
