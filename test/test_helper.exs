ExUnit.start()

unless Node.alive?() do
  {:ok, _node} =
    Node.start(:"paseo_relay_test_#{System.unique_integer([:positive])}", :shortnames)
end
