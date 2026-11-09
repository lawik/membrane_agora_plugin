defmodule Membrane.Agora.DispatcherTest do
  use ExUnit.Case, async: true

  require Membrane.Pad, as: Pad

  alias Membrane.Agora.Dispatcher
  alias Membrane.Buffer

  defp buffer(user_id, n), do: %Buffer{payload: <<n>>, metadata: %{id: user_id}}

  defp init(opts) do
    {[], state} = Dispatcher.handle_init(%{}, struct!(Dispatcher, opts))
    state
  end

  defp feed(state, user_id, range) do
    Enum.reduce(range, {[], state}, fn n, {actions, state} ->
      {new_actions, state} = Dispatcher.handle_buffer(:input, buffer(user_id, n), %{}, state)
      {actions ++ new_actions, state}
    end)
  end

  test "queues buffers until the pad is connected, dropping the oldest above the limit" do
    state = init(queue_before_pad_connected?: true, max_queued_buffers: 3)

    {actions, state} = feed(state, "user", 1..5)
    assert actions == [notify_parent: {:add_pad, "user"}]

    {actions, state} = Dispatcher.handle_pad_added(Pad.ref(:output, "user"), %{}, state)
    pad = Pad.ref(:output, "user")

    assert Keyword.get_values(actions, :buffer) == [
             {pad, buffer("user", 3)},
             {pad, buffer("user", 4)},
             {pad, buffer("user", 5)}
           ]

    # Once connected, buffers are forwarded and nothing is kept
    assert {[buffer: {^pad, _buffer}], state} =
             Dispatcher.handle_buffer(:input, buffer("user", 6), %{}, state)

    assert state.output_pads["user"].buffered == []
  end

  test "discards buffers of unconnected pads when queueing is disabled" do
    state = init(queue_before_pad_connected?: false)

    {actions, state} = feed(state, "user", 1..5)
    assert actions == [notify_parent: {:add_pad, "user"}]
    assert state.output_pads["user"].buffered == []

    {actions, _state} = Dispatcher.handle_pad_added(Pad.ref(:output, "user"), %{}, state)
    assert Keyword.get_values(actions, :buffer) == []
  end
end
