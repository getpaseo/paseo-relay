FROM node:22-bookworm-slim

WORKDIR /load
COPY scripts/relay-load.mjs /load/relay-load.mjs
COPY deployment/load/entrypoint.sh /entrypoint

ENTRYPOINT ["/entrypoint"]
CMD ["sleep", "infinity"]
