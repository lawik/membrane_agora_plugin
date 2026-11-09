defmodule Membrane.Agora.Dispatcher do
  @moduledoc """
  This element allows for dispatching the streams received by the `Membrane.Agora.Source`
  based on the `user_id` field from the buffer's metadata.
  The `Membrane.Agora.Source` sends a single stream for all the users that are present in the
  Agora channel and enriches buffers with the `user_id` field which corresponds
  to the ID of the stream sender from the Agora channel. The `#{inspect(__MODULE__)}` element allows
  to demultiplex such a single output channel stream into multiple streams for each of the users.

  Usage:
  * attach the `#{inspect(__MODULE__)}` element to each output pad of the `Membrane.Agora.Source`.
  * when the `{:add_pad, <user_id>}` message is received from a given `#{inspect(__MODULE__)}` dispatcher,
  attach the output pad with the following name: `{:output, <user_id>}` to this particular dispatcher
  """

  use Membrane.Filter

  require Logger
  require Membrane.Logger

  def_input_pad(:input,
    accepted_format:
      any_of(
        %Membrane.H264{alignment: :au},
        %Membrane.RawAudio{sample_rate: 44_100, channels: 2, sample_format: :s16le}
      )
  )

  def_output_pad(:output,
    accepted_format:
      any_of(
        %Membrane.H264{alignment: :au},
        %Membrane.RawAudio{sample_rate: 44_100, channels: 2, sample_format: :s16le}
      ),
    availability: :on_request
  )

  def_options queue_before_pad_connected?: [
                spec: boolean(),
                default: true,
                description: """
                If true, the buffers will be queued until the corresponding pad, on which they should be sent, is connected.
                Otherwise, if there is no corresponding pad available, the buffers are discarded.
                Defaults to true.
                """
              ],
              max_queued_buffers: [
                spec: pos_integer(),
                default: 1000,
                description: """
                Maximum number of buffers queued per user while waiting for the corresponding pad
                to be connected (see `queue_before_pad_connected?`). When the limit is reached,
                the oldest queued buffers are dropped, so a user whose pad is never connected
                cannot make the queue grow without bounds.
                Defaults to 1000.
                """
              ]

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %{
       output_pads: %{},
       queue_before_pad_connected?: opts.queue_before_pad_connected?,
       max_queued_buffers: opts.max_queued_buffers,
       stream_format: nil
     }}
  end

  @impl true
  def handle_stream_format(:input, format, _ctx, state) do
    {[forward: format], %{state | stream_format: format}}
  end

  @impl true
  def handle_buffer(:input, buffer, _ctx, state) do
    user_id = buffer.metadata.id

    cond do
      user_id not in Map.keys(state.output_pads) ->
        state = add_pad(state, user_id)
        state = buffer_up(state, buffer)
        {[notify_parent: {:add_pad, user_id}], state}

      state.output_pads[user_id].is_connected ->
        {[buffer: {Pad.ref(:output, user_id), buffer}], state}

      true ->
        state = buffer_up(state, buffer)
        {[], state}
    end
  end

  @impl true
  def handle_pad_added(Pad.ref(:output, user_id), _ctx, state) do
    {notify_child_actions, state} =
      if user_id in Map.keys(state.output_pads) do
        {[], state}
      else
        {[], add_pad(state, user_id)}
      end

    state =
      update_in(state, [:output_pads, user_id, :is_connected], fn false ->
        true
      end)

    stream_format_actions =
      if state.stream_format,
        do: [stream_format: {Pad.ref(:output, user_id), state.stream_format}],
        else: []

    buffer_actions =
      Enum.reverse(state.output_pads[user_id].buffered)
      |> Enum.map(&{:buffer, {Pad.ref(:output, user_id), &1}})

    state =
      update_in(state, [:output_pads, user_id], &%{&1 | buffered: [], buffered_size: 0})

    {notify_child_actions ++ stream_format_actions ++ buffer_actions, state}
  end

  defp add_pad(state, user_id) do
    put_in(state, [:output_pads, user_id], %{
      buffered: [],
      buffered_size: 0,
      dropped: 0,
      is_connected: false
    })
  end

  defp buffer_up(%{queue_before_pad_connected?: false} = state, _buffer) do
    state
  end

  defp buffer_up(state, buffer) do
    user_id = buffer.metadata.id

    update_in(state, [:output_pads, user_id], fn
      %{buffered_size: size} = output_pad when size < state.max_queued_buffers ->
        %{output_pad | buffered: [buffer | output_pad.buffered], buffered_size: size + 1}

      %{dropped: dropped} = output_pad ->
        if dropped == 0 do
          Membrane.Logger.warning("""
          Reached the limit of #{state.max_queued_buffers} buffers queued for user #{inspect(user_id)} \
          while waiting for its output pad to be connected, dropping the oldest buffers.
          """)
        end

        # `buffered` holds the newest buffer first, so the oldest one is the last
        buffered = [buffer | List.delete_at(output_pad.buffered, -1)]
        %{output_pad | buffered: buffered, dropped: dropped + 1}
    end)
  end
end
