defmodule PaseoRelay.Reroute do
  @moduledoc "Transforms a generic ownership decision into configured response headers."

  def headers({:reroute, target}, header_name) when is_binary(target) and is_binary(header_name),
    do: %{header_name => target}

  def headers(:local, _header_name), do: %{}
  def headers(:unowned, _header_name), do: %{}
end
