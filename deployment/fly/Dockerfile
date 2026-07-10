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
COPY deployment/fly/entrypoint.sh /adapter-entrypoint
ENTRYPOINT ["/adapter-entrypoint"]
