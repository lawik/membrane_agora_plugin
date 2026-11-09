defmodule Membrane.Agora.Sink do
  @moduledoc """
  Membrane sink that wraps Agora's Server Gateway SDK.

  """
  use Membrane.Sink

  require Membrane.Logger
  require Membrane.Pad, as: Pad

  alias Membrane.{AAC, Buffer, Opus}
  alias Membrane.Agora.Sink.Native

  def_input_pad :video,
    availability: :on_request,
    accepted_format: %Membrane.H264{alignment: :au},
    flow_control: :auto

  def_input_pad :audio,
    availability: :on_request,
    accepted_format: any_of(Membrane.AAC, Membrane.Opus),
    flow_control: :auto,
    options: [
      sample_rate: [
        spec: pos_integer(),
        default: 48_000,
        description: """
        Sample rate of the audio stream going through :audio pad.

        Used only if the audio codec is `Membrane.Opus`. If the audio codec is
        `Membrane.AAC`, sample rate value will be passed in the stream format.

        Defaults to 48 000.
        """
      ]
    ]

  def_options app_id: [
                spec: String.t(),
                description: """
                ID of an Agora application.
                """
              ],
              channel_name: [
                spec: String.t(),
                descritpion: """
                A name of a channel to which the sink should connect.
                """
              ],
              token: [
                spec: String.t(),
                descritpion: """
                A temporary token used for authorization of an access to Agora's channel.
                """
              ],
              user_id: [
                spec: String.t(),
                default: "0",
                description: """
                  User ID, must contain only numbers (0-9).

                  If set to "0" (default), the user ID will be chosen automatically.
                """
              ]

  @impl true
  def handle_init(_ctx, opts) do
    state = %{
      app_id: opts.app_id,
      token: opts.token,
      channel_name: opts.channel_name,
      user_id: opts.user_id,
      native_state: nil,
      last_frame_duration: nil,
      opus_queue: []
    }

    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    native_state =
      try do
        start_time = Membrane.Time.os_time()

        native_state =
          case Native.create(state.app_id, state.token, state.channel_name, state.user_id) do
            {:ok, native_state} ->
              native_state

            {:error, reason} ->
              raise "Failed to create the Agora #{inspect(__MODULE__)}: #{inspect(reason)}"
          end

        duration = Membrane.Time.os_time() - start_time
        duration_ms = Membrane.Time.as_milliseconds(duration, :round)
        Membrane.Logger.info("Agora SDK initialization took: #{duration_ms} ms")

        if duration_ms > 500 do
          Membrane.Logger.warning("""
          Agora SDK initialization took #{duration_ms} ms which is longer than expected.
          The initial demand made by this sink might be delayed.
          """)
        end

        native_state
      rescue
        _e in UndefinedFunctionError ->
          reraise(
            """
            Couldn't setup NIF. Perhaps you have forgotten to set LD_LIBRARY_PATH: \
            export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:#{Path.expand("#{__ENV__.file}/../../../agora_sdk")} \
            """,
            __STACKTRACE__
          )

        other_error ->
          reraise(other_error, __STACKTRACE__)
      end

    {[], %{state | native_state: native_state}}
  end

  @impl true
  def handle_stream_format(Pad.ref(:video, _id), stream_format, _ctx, state) do
    {:ok, native_state} =
      Native.update_video_stream_format(
        stream_format.height,
        stream_format.width,
        state.native_state
      )

    state = %{state | native_state: native_state}
    {[], state}
  end

  @impl true
  def handle_stream_format(Pad.ref(:audio, _id), %Membrane.AAC{} = aac, _ctx, state) do
    {:ok, native_state} =
      Native.update_audio_stream_format(
        aac.sample_rate,
        aac.channels,
        aac.samples_per_frame,
        state.native_state
      )

    {[], %{state | native_state: native_state}}
  end

  @impl true
  def handle_stream_format(Pad.ref(:audio, _id), %Opus{}, _ctx, state) do
    # when audio codec is Opus, Native.update_audio_stream_format/4 is called in handle_buffer/4
    {[], state}
  end

  @impl true
  def handle_buffer(Pad.ref(:video, _id), buffer, _ctx, state) do
    :ok =
      Native.write_video_data(
        buffer.payload,
        buffer.metadata.h264.key_frame?,
        state.native_state
      )

    {[], state}
  end

  @impl true
  def handle_buffer(Pad.ref(:audio, _id) = pad, buffer, ctx, state) do
    state =
      case ctx.pads[pad].stream_format do
        %Opus{} -> handle_opus_buffer(pad, buffer, ctx, state)
        %AAC{} -> handle_aac_buffer(buffer, state)
      end

    {[], state}
  end

  defp handle_opus_buffer(pad, buffer, ctx, state) do
    opus_queue =
      case state.opus_queue do
        [%Buffer{} = previous] ->
          previous_metadata =
            previous.metadata
            |> Map.put(:duration, buffer.pts - previous.pts)

          previous = %{previous | metadata: previous_metadata}
          [previous, buffer]

        [] ->
          [buffer]
      end

    {buffers_with_duration, opus_queue} =
      opus_queue
      |> Enum.split_while(&is_map_key(&1.metadata, :duration))

    state =
      buffers_with_duration
      |> Enum.reduce(state, fn buffer, state ->
        state =
          if buffer.metadata.duration != state.last_frame_duration do
            update_frame_duration(buffer.metadata.duration, pad, ctx, state)
          else
            state
          end

        :ok = Native.write_audio_data(buffer.payload, :opus, state.native_state)
        state
      end)

    %{state | opus_queue: opus_queue}
  end

  defp handle_aac_buffer(buffer, state) do
    :ok = Native.write_audio_data(buffer.payload, :aac, state.native_state)
    state
  end

  defp update_frame_duration(frame_duration, pad, ctx, state) do
    pad_data = ctx.pads[pad]

    sample_rate = pad_data.options.sample_rate
    samples_per_frame = (frame_duration * sample_rate) |> div(1000)

    {:ok, native_state} =
      Native.update_audio_stream_format(
        sample_rate,
        pad_data.stream_format.channels,
        samples_per_frame,
        state.native_state
      )

    %{state | native_state: native_state, last_frame_duration: frame_duration}
  end
end
