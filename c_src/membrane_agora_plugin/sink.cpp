#include "sink.h"

// Once SIGINT is delivered twice to the process that runs the elixir script
// (e.g. with ctrl+c), the script terminates. Sometimes it results in a
// SEGFAULT being thrown from then Agora's "AgMajor" thread, over which
// we don't have control. Remember to properly terminate the process with
// SIGTERM.

UNIFEX_TERM create(UnifexEnv *env, char *appId, char *token, char *channelId,
                   char *userId) {

  // sink's native state initialization
  SinkState *state = unifex_alloc_state(env);
  auto empty_state = SinkState();
  memcpy((void *)state, (void *)&empty_state, sizeof(SinkState));

  // service creation and initialization
  state->service = createAgoraService();

  agora::base::AgoraServiceConfiguration scfg;
  scfg.appId = appId;
  scfg.enableAudioProcessor = true;
  scfg.enableAudioDevice = false;
  scfg.enableVideo = true;
  scfg.useStringUid = false;
  if (state->service->initialize(scfg) != agora::ERR_OK) {
    AG_LOG(ERROR, "Failed to initialize Agora service in sink");
    unifex_release_state(env, state);
    return create_result_error(env,
                               "Failed to initialize Agora service in sink");
  }

  // connection configuration
  agora::rtc::RtcConnectionConfiguration ccfg;
  ccfg.autoSubscribeAudio = false;
  ccfg.autoSubscribeVideo = false;
  ccfg.clientRoleType = agora::rtc::CLIENT_ROLE_BROADCASTER;
  state->connection = state->service->createRtcConnection(ccfg);

  // Cache initial i-frame at agora
  auto s = state->connection->getAgoraParameter();
  s->setBool("che.video.has_intra_request", false);

  // connecting
  auto connObserver = std::make_shared<ConnectionObserver>(state->connection);
  state->connection->registerObserver(connObserver.get());

  int connection_res = state->connection->connect(token, channelId, userId);
  if (connection_res) {
    AG_LOG(ERROR, "Failed to connect to Agora channel!");
    state->connection->unregisterObserver(connObserver.get());
    unifex_release_state(env, state);
    return create_result_error(env, "Failed to connect to Agora channel!");
  }
  // `connect` only means the request was accepted; wait (with a timeout) for
  // the connection to be established or to fail, instead of blocking the dirty
  // scheduler forever on e.g. an expired token
  if (!connObserver->waitUntilConnected()) {
    AG_LOG(ERROR, "Failed to connect to Agora channel!");
    state->connection->unregisterObserver(connObserver.get());
    unifex_release_state(env, state);
    return create_result_error(env, "Failed to connect to Agora channel!");
  }

  // senders creation
  agora::agora_refptr<agora::rtc::IMediaNodeFactory> factory =
      state->service->createMediaNodeFactory();
  if (!factory) {
    AG_LOG(ERROR, "Failed to create media node factory!");
    unifex_release_state(env, state);
    return create_result_error(env, "Failed to create media node factory!");
  }
  state->videoEncodedFrameSender = factory->createVideoEncodedImageSender();
  if (!state->videoEncodedFrameSender) {
    AG_LOG(ERROR, "Failed to create encoded video frame sender!");
    unifex_release_state(env, state);
    return create_result_error(env,
                               "Failed to create encoded video frame sender!");
  }
  state->audioEncodedFrameSender = factory->createAudioEncodedFrameSender();
  if (!state->audioEncodedFrameSender) {
    AG_LOG(ERROR, "Failed to create audio encoded frame sender!");
    unifex_release_state(env, state);
    return create_result_error(env,
                               "Failed to create audio encoded frame sender!");
  }

  // video track creation
  agora::rtc::SenderOptions senderOptions;
  state->customVideoTrack = state->service->createCustomVideoTrack(
      state->videoEncodedFrameSender, senderOptions);
  if (!state->customVideoTrack) {
    AG_LOG(ERROR, "Failed to create video track!");
    unifex_release_state(env, state);
    return create_result_error(env, "Failed to create video track!");
  }

  // audio track creation
  state->customAudioTrack = state->service->createCustomAudioTrack(
      state->audioEncodedFrameSender, agora::base::MIX_ENABLED);
  if (!state->customAudioTrack) {
    AG_LOG(ERROR, "Failed to create audio track!");
    unifex_release_state(env, state);
    return create_result_error(env, "Failed to create audio track!");
  }

  // publishing of the tracks
  state->customVideoTrack->setEnabled(true);
  state->customAudioTrack->setEnabled(true);
  state->connection->getLocalUser()->publishVideo(state->customVideoTrack);
  state->connection->getLocalUser()->publishAudio(state->customAudioTrack);

  // cleaning up
  state->connection->unregisterObserver(connObserver.get());
  connObserver.reset();
  UNIFEX_TERM res = create_result_ok(env, state);
  unifex_release_state(env, state);

  return res;
}

UNIFEX_TERM update_video_stream_format(UnifexEnv *env, int height, int width,
                                       SinkState *state) {
  state->height = height;
  state->width = width;
  return update_video_stream_format_result_ok(env, state);
}

UNIFEX_TERM write_video_data(UnifexEnv *env, UnifexPayload *payload,
                             int isKeyframe, SinkState *state) {
  agora::rtc::EncodedVideoFrameInfo videoEncodedFrameInfo;

  videoEncodedFrameInfo.width = state->width;
  videoEncodedFrameInfo.height = state->height;

  videoEncodedFrameInfo.rotation = agora::rtc::VIDEO_ORIENTATION_0;
  videoEncodedFrameInfo.codecType = agora::rtc::VIDEO_CODEC_H264;

  videoEncodedFrameInfo.frameType =
      (isKeyframe ? agora::rtc::VIDEO_FRAME_TYPE::VIDEO_FRAME_TYPE_KEY_FRAME
                  : agora::rtc::VIDEO_FRAME_TYPE::VIDEO_FRAME_TYPE_DELTA_FRAME);

  if (state->videoEncodedFrameSender->sendEncodedVideoImage(
          reinterpret_cast<uint8_t *>(payload->data), payload->size,
          videoEncodedFrameInfo) != true) {
    AG_LOG(ERROR, "Couldn't send video frame");
    return write_video_data_result_error(env, "Couldn't send video frame");
  }

  return write_video_data_result_ok(env);
}

UNIFEX_TERM update_audio_stream_format(UnifexEnv *env, int sampleRate,
                                       int numberOfChannels,
                                       int samplesPerChannelPerFrame,
                                       SinkState *state) {
  state->sampleRate = sampleRate;
  state->numberOfChannels = numberOfChannels;
  state->samplesPerChannelPerFrame = samplesPerChannelPerFrame;
  return update_audio_stream_format_result_ok(env, state);
}

UNIFEX_TERM write_audio_data(UnifexEnv *env, UnifexPayload *payload,
                             CodecAudio codec, SinkState *state) {
  agora::rtc::EncodedAudioFrameInfo audioFrameInfo;
  audioFrameInfo.sampleRateHz = state->sampleRate;
  audioFrameInfo.numberOfChannels = state->numberOfChannels;
  audioFrameInfo.samplesPerChannel = state->samplesPerChannelPerFrame;

  if (codec == CODEC_AUDIO_AAC) {
    audioFrameInfo.codec = agora::rtc::AUDIO_CODEC_TYPE::AUDIO_CODEC_AACLC;
  } else if (codec == CODEC_AUDIO_OPUS) {
    audioFrameInfo.codec = agora::rtc::AUDIO_CODEC_TYPE::AUDIO_CODEC_OPUS;
  } else {
    AG_LOG(WARNING, "Audio codec passed to sink is neither AAC nor Opus, but "
                    "only these two values are supported for now.");
  }

  if (state->audioEncodedFrameSender->sendEncodedAudioFrame(
          reinterpret_cast<uint8_t *>(payload->data), payload->size,
          audioFrameInfo) != true) {
    AG_LOG(ERROR, "Couldn't send audio frame");
    return write_video_data_result_error(env, "Couldn't send audio frame");
  }

  return write_audio_data_result_ok(env);
}

void handle_destroy_state(UnifexEnv *env, SinkState *state) {
  UNUSED(env);
  if (state->connection) {
    if (state->customVideoTrack) {
      state->connection->getLocalUser()->unpublishVideo(
          state->customVideoTrack);
      state->customVideoTrack = NULL;
    }

    if (state->customAudioTrack) {
      state->connection->getLocalUser()->unpublishAudio(
          state->customAudioTrack);
      state->customAudioTrack = NULL;
    }

    if (state->connection->disconnect()) {
      AG_LOG(ERROR, "Failed to disconnect from Agora channel!");
      return;
    }
    AG_LOG(INFO, "[Sink] Disconnected from Agora channel successfully");
    state->connection = NULL;
  }

  if (state->service) {
    state->service->release();
    AG_LOG(INFO, "[Sink] Agora service released successfully");
    state->service = NULL;
  }
}
