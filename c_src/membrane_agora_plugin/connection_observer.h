#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>

#include <unifex/unifex.h>

#include "NGIAgoraLocalUser.h"
#include "NGIAgoraRtcConnection.h"

#include "common.h"

using namespace agora;
using namespace agora::rtc;

class ConnectionObserver : public IRtcConnectionObserver {
public:
  // How long `create` waits for the connection to be established. `connect`
  // is asynchronous and may never report success (e.g. expired token, wrong
  // app id or channel, Agora unreachable), and the NIF runs on a dirty
  // scheduler, so the wait must be bounded.
  static constexpr std::chrono::seconds CONNECT_TIMEOUT{15};

  ConnectionObserver(agora_refptr<IRtcConnection> connection)
      : _is_connected(false), _is_failed(false), _connection(connection),
        _destination(std::nullopt) {}

  ConnectionObserver(agora_refptr<IRtcConnection> connection,
                     UnifexPid destination)
      : _is_connected(false), _is_failed(false), _connection(connection),
        _destination(destination) {}

  // Blocks until the connection is established, fails or `timeout` elapses.
  // Returns true only if the connection was established.
  bool waitUntilConnected(std::chrono::milliseconds timeout = CONNECT_TIMEOUT);

public: // IRtcConnectionObserver
  void onConnected(const TConnectionInfo &connectionInfo,
                   CONNECTION_CHANGED_REASON_TYPE reason);

  void onConnecting(const TConnectionInfo &connectionInfo,
                    CONNECTION_CHANGED_REASON_TYPE reason) override;

  void onDisconnected(const TConnectionInfo &connectionInfo,
                      CONNECTION_CHANGED_REASON_TYPE reason) override;

  void onReconnecting(const TConnectionInfo &connectionInfo,
                      CONNECTION_CHANGED_REASON_TYPE reason) override;

  void onReconnected(const TConnectionInfo &connectionInfo,
                     CONNECTION_CHANGED_REASON_TYPE reason) override;

  void onConnectionLost(const TConnectionInfo &connectionInfo) override;
  void onLastmileQuality(const QUALITY_TYPE quality) override;

  void onTokenPrivilegeWillExpire(const char *token) override;

  void onTokenPrivilegeDidExpire() override;

  void onConnectionFailure(const TConnectionInfo &connectionInfo,
                           CONNECTION_CHANGED_REASON_TYPE reason) override;

  void onUserJoined(user_id_t userId) override;

  void onUserLeft(user_id_t userId, USER_OFFLINE_REASON_TYPE reason) override;

  void onTransportStats(const RtcStats &stats) override;

  void onLastmileProbeResult(const LastmileProbeResult &result) override;

  void onChannelMediaRelayStateChanged(int state, int code) override;

private:
  void signalConnectionResult(bool connected);

  std::atomic<bool> _is_connected;
  std::atomic<bool> _is_failed;
  std::condition_variable _cv;
  std::mutex _lock;
  agora_refptr<IRtcConnection> _connection;
  std::optional<UnifexPid> _destination;
};
