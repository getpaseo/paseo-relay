FROM hexpm/elixir:1.20.2-erlang-29.0.3-debian-bookworm-20260126 AS build
WORKDIR /app
ENV MIX_ENV=prod
COPY mix.exs mix.lock ./
RUN mix local.hex --force && mix local.rebar --force && mix deps.get --only prod
COPY config config
COPY lib lib
RUN mix compile && mix release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 openssl ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/_build/prod/rel/paseo_relay ./
ENV HOME=/app \
    PASEO_RELAY_HOST=0.0.0.0 \
    PASEO_RELAY_PORT=4000 \
    PASEO_RELAY_INTERNAL_PORT=4001 \
    PASEO_RELAY_DRAIN=false
EXPOSE 4000 4001
ENTRYPOINT ["/app/bin/paseo_relay"]
CMD ["start"]
