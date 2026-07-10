defmodule PaseoRelay.OwnershipTest do
  use ExUnit.Case

  alias PaseoRelay.Ownership

  test "claims an unowned server locally and keeps later local requests local" do
    assert :local = Ownership.claim("server-a", "opaque-owner-a")
    assert :local = Ownership.resolve("server-a")
  end

  test "clears ownership when the owner dies so another request can claim it" do
    parent = self()

    owner =
      spawn(fn ->
        assert :local = Ownership.claim("server-b", "opaque-owner-b")
        send(parent, :claimed)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :claimed
    owner_record = Ownership.owner_pid("server-b")
    owner_down = Process.monitor(owner_record)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^owner_down, :process, ^owner_record, :normal}

    assert :unowned = Ownership.resolve("server-b")
    assert :local = Ownership.claim("server-b", "opaque-owner-c")
  end
end

defmodule PaseoRelay.RerouteTest do
  use ExUnit.Case, async: true

  test "renders an opaque reroute target into a configured response header" do
    assert %{"x-reroute-target" => "machine-opaque-id"} =
             PaseoRelay.Reroute.headers({:reroute, "machine-opaque-id"}, "x-reroute-target")

    assert %{} = PaseoRelay.Reroute.headers(:local, "x-reroute-target")
  end
end

defmodule PaseoRelay.DistributedOwnershipTest do
  use ExUnit.Case

  alias PaseoRelay.Ownership

  setup_all do
    unless Node.alive?() do
      {_, 0} = System.cmd("epmd", ["-daemon"])

      {:ok, _} =
        Node.start(
          String.to_atom("relay_test_" <> Integer.to_string(System.unique_integer([:positive]))),
          :shortnames
        )
    end

    :erlang.set_cookie(node(), :relay_test_cookie)

    {:ok, peer, peer_node} =
      :peer.start_link(%{
        name: :relay_peer,
        args: [~c"-setcookie", ~c"relay_test_cookie", ~c"-pa" | :code.get_path()]
      })

    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        :exit, _ -> :ok
      end
    end)

    %{peer: peer_node}
  end

  test "concurrent claims choose one owner and remote requests receive its opaque target", %{
    peer: peer
  } do
    local_owner = self()

    remote_owner =
      :rpc.call(peer, :erlang, :spawn, [:timer, :sleep, [:infinity]])

    results =
      Task.await_many([
        Task.async(fn -> Ownership.claim("server-c", "opaque-owner-a", local_owner) end),
        Task.async(fn ->
          :rpc.call(peer, Ownership, :claim, ["server-c", "opaque-owner-b", remote_owner])
        end)
      ])

    assert 1 == Enum.count(results, &(&1 == :local))

    assert Enum.any?(
             [Ownership.resolve("server-c"), :rpc.call(peer, Ownership, :resolve, ["server-c"])],
             &(&1 == :local)
           )

    assert Enum.any?(
             [Ownership.resolve("server-c"), :rpc.call(peer, Ownership, :resolve, ["server-c"])],
             &match?(
               {:reroute, winner_target}
               when winner_target in ["opaque-owner-a", "opaque-owner-b"],
               &1
             )
           )
  end

  test "a remote lookup returns the local winner's advertised opaque target", %{peer: peer} do
    assert :local = Ownership.claim("server-d", "opaque-owner-a")
    assert {:reroute, "opaque-owner-a"} = :rpc.call(peer, Ownership, :resolve, ["server-d"])
  end

  test "a remote node can claim after the current owner dies", %{peer: peer} do
    parent = self()

    local_session =
      spawn(fn ->
        send(parent, {:claimed, Ownership.claim("server-e", "opaque-owner-a")})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:claimed, :local}
    owner_record = Ownership.owner_pid("server-e")
    owner_down = Process.monitor(owner_record)
    Process.exit(local_session, :kill)
    assert_receive {:DOWN, ^owner_down, :process, ^owner_record, :normal}

    remote_session = :rpc.call(peer, :erlang, :spawn, [:timer, :sleep, [:infinity]])

    assert :local =
             :rpc.call(peer, Ownership, :claim, ["server-e", "opaque-owner-b", remote_session])

    assert {:reroute, "opaque-owner-b"} = Ownership.resolve("server-e")
  end
end
