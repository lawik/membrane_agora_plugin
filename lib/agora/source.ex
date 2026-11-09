defmodule Membrane.Agora.Source do
  @moduledoc """
  Membrane source that wraps Agora's Server Gateway SDK.

  """
  use Membrane.Source

  require Membrane.Logger

  alias Membrane.Agora.Source.Native
  alias Membrane.Buffer

  def_output_pad :video,
    accepted_format: %Membrane.H264{alignment: :au},
    flow_control: :push

  def_output_pad :audio,
    accepted_format: %Membrane.RawAudio{sample_rate: 44_100, channels: 2, sample_format: :s16le},
    flow_control: :push

  def_options app_id: [
                spec: String.t(),
                description: """
                ID of an Agora application.
                """
              ],
              channel_name: [
                spec: String.t(),
                descritpion: """
                A name of a channel to which the source should connect.
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
      peers_ids: MapSet.new(),
      first_video_timestamp: nil,
      first_audio_timestamp: nil
    }

    {[], state}
  end

  @impl true
  def handle_playing(_ctx, state) do
    native_state =
      try do
        start_time = Membrane.Time.os_time()

        native_state =
          case Native.create(state.app_id, state.token, state.channel_name, state.user_id, self()) do
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

    {[
       stream_format: {:video, %Membrane.H264{}},
       stream_format:
         {:audio, %Membrane.RawAudio{channels: 2, sample_rate: 44_100, sample_format: :s16le}},
       notify_parent: :agora_connected
     ], %{state | native_state: native_state}}
  end

  @impl true
  def handle_info({:agora_video_payload, payload, id}, _ctx, state) do
    {dts, state} =
      if state.first_video_timestamp do
        {Membrane.Time.os_time() - state.first_video_timestamp, state}
      else
        {0, %{state | first_video_timestamp: Membrane.Time.os_time()}}
      end

    {[buffer: {:video, %Buffer{payload: payload, metadata: %{id: inspect(id)}, dts: dts}}], state}
  end

  @impl true
  def handle_info({:agora_audio_payload, payload, id_str}, _ctx, state) do
    {dts, state} =
      if state.first_audio_timestamp do
        {Membrane.Time.os_time() - state.first_audio_timestamp, state}
      else
        {0, %{state | first_audio_timestamp: Membrane.Time.os_time()}}
      end

    pts = dts

    {[buffer: {:audio, %Buffer{payload: payload, metadata: %{id: id_str}, pts: pts, dts: dts}}],
     state}
  end

  @impl true
  def handle_info({:user_joined, id_str}, _ctx, state) do
    peers_ids = MapSet.put(state.peers_ids, id_str)
    {[], %{state | peers_ids: peers_ids}}
  end

  @impl true
  def handle_info({:user_left, id_str}, _ctx, state) do
    peers_ids = MapSet.delete(state.peers_ids, id_str)
    state = %{state | peers_ids: peers_ids}

    if MapSet.size(peers_ids) == 0 do
      {[end_of_stream: :video, end_of_stream: :audio], state}
    else
      {[], state}
    end
  end
end
