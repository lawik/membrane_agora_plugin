#include "./_generated/source.h"
#include "connection_observer.h"

bool ConnectionObserver::waitUntilConnected(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> unique_lock(_lock);
  bool finished = _cv.wait_for(unique_lock, timeout,
                               [this] { return _is_connected || _is_failed; });
  if (!finished) {
    AG_LOG(ERROR, "Timed out waiting for the Agora connection");
    return false;
  }
  return _is_connected && !_is_failed;
}

void ConnectionObserver::signalConnectionResult(bool connected) {
  std::unique_lock<std::mutex> unique_lock(_lock);
  if (connected) {
    _is_connected = true;
  } else {
    _is_failed = true;
  }
  _cv.notify_all();
}

void ConnectionObserver::onConnected(
    const rtc::TConnectionInfo &connectionInfo,
    rtc::CONNECTION_CHANGED_REASON_TYPE reason) {
  UNUSED(connectionInfo);
  UNUSED(reason);
  signalConnectionResult(true);
}

void ConnectionObserver::onConnecting(
    const rtc::TConnectionInfo &connectionInfo,
    rtc::CONNECTION_CHANGED_REASON_TYPE reason) {
  UNUSED(connectionInfo);
  UNUSED(reason);
}

void ConnectionObserver::onDisconnected(
    const rtc::TConnectionInfo &connectionInfo,
    rtc::CONNECTION_CHANGED_REASON_TYPE reason) {
  UNUSED(connectionInfo);
  AG_LOG(WARNING, "Disconnected from the Agora channel, reason: %d",
         static_cast<int>(reason));
  signalConnectionResult(false);
}

void ConnectionObserver::onReconnecting(
    const rtc::TConnectionInfo &connectionInfo,
    rtc::CONNECTION_CHANGED_REASON_TYPE reason) {
  UNUSED(connectionInfo);
  UNUSED(reason);
}

void ConnectionObserver::onReconnected(
    const rtc::TConnectionInfo &connectionInfo,
    rtc::CONNECTION_CHANGED_REASON_TYPE reason) {
  UNUSED(connectionInfo);
  UNUSED(reason);
}

void ConnectionObserver::onConnectionLost(
    const rtc::TConnectionInfo &connectionInfo) {
  UNUSED(connectionInfo);
  AG_LOG(WARNING, "Connection to the Agora channel lost");
  signalConnectionResult(false);
}

void ConnectionObserver::onLastmileQuality(const rtc::QUALITY_TYPE quality) {
  UNUSED(quality);
}

void ConnectionObserver::onTokenPrivilegeWillExpire(const char *token) {
  UNUSED(token);
}

void ConnectionObserver::onTokenPrivilegeDidExpire() {}

void ConnectionObserver::onConnectionFailure(
    const rtc::TConnectionInfo &connectionInfo,
    rtc::CONNECTION_CHANGED_REASON_TYPE reason) {
  UNUSED(connectionInfo);
  AG_LOG(ERROR, "Failed to connect to the Agora channel, reason: %d",
         static_cast<int>(reason));
  signalConnectionResult(false);
}

void ConnectionObserver::onUserJoined(user_id_t userId) {
  rtc::VideoSubscriptionOptions options;
  options.encodedFrameOnly = true;
  options.type = rtc::VIDEO_STREAM_TYPE::VIDEO_STREAM_LOW;
  _connection->getLocalUser()->subscribeVideo(userId, options);
  _connection->getLocalUser()->subscribeAudio(userId);

  if (_destination.has_value()) {
    UnifexEnv *env = unifex_alloc_env(NULL);
    send_user_joined(env, _destination.value(), UNIFEX_SEND_THREADED, userId);
    unifex_free_env(env);
  }
}

void ConnectionObserver::onUserLeft(user_id_t userId,
                                    rtc::USER_OFFLINE_REASON_TYPE reason) {
  UNUSED(reason);
  if (_destination.has_value()) {
    UnifexEnv *env = unifex_alloc_env(NULL);
    send_user_left(env, _destination.value(), UNIFEX_SEND_THREADED, userId);
    unifex_free_env(env);
  }
}

void ConnectionObserver::onTransportStats(const rtc::RtcStats &stats) {
  UNUSED(stats);
}

void ConnectionObserver::onLastmileProbeResult(
    const rtc::LastmileProbeResult &result) {
  UNUSED(result);
}

void ConnectionObserver::onChannelMediaRelayStateChanged(int state, int code) {
  UNUSED(state);
  UNUSED(code);
}
