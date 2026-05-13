#include "utils.h"

#include <util/string/builder.h>

namespace NKvVolumeStress {

namespace {

constexpr ui16 DefaultKvPort = 2135;
const TString DataPattern = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

} // namespace

std::mt19937_64& RandomEngine() {
    thread_local std::mt19937_64 engine(std::random_device{}());
    return engine;
}

TString MakeVolumePath(const TString& database, const TString& path) {
    if (database.empty()) {
        return path;
    }
    if (database.back() == '/') {
        return database + path;
    }
    return database + "/" + path;
}

TString ParseHostPort(const TString& endpoint) {
    TString hostPort = endpoint;
    const TString scheme = "://";
    const size_t pos = hostPort.find(scheme);
    if (pos != TString::npos) {
        hostPort = hostPort.substr(pos + scheme.size());
    }

    if (hostPort.empty()) {
        return TStringBuilder() << "localhost:" << DefaultKvPort;
    }

    // Bracketed IPv6 literal: [addr] or [addr]:port
    // gRPC C++ does not accept RFC-2732 brackets — it URL-encodes them.
    // The correct gRPC format for IPv6 is "ipv6:addr:port" (no brackets).
    if (!hostPort.empty() && hostPort[0] == '[') {
        const size_t closeBracket = hostPort.find(']');
        if (closeBracket != TString::npos) {
            TString addr = hostPort.substr(1, closeBracket - 1); // strip brackets
            TString portSuffix;
            if (closeBracket + 1 < hostPort.size() && hostPort[closeBracket + 1] == ':') {
                portSuffix = hostPort.substr(closeBracket + 1); // ":port"
            } else {
                portSuffix = TStringBuilder() << ":" << DefaultKvPort;
            }
            return TStringBuilder() << "ipv6:" << addr << portSuffix;
        }
    }

    // Plain hostname or IPv4: look for ':' to detect an existing port.
    if (hostPort.find(':') == TString::npos) {
        hostPort += TStringBuilder() << ":" << DefaultKvPort;
    }

    return hostPort;
}

bool ParseUseTls(const TString& endpoint) {
    return endpoint.StartsWith("grpcs://");
}

TString GeneratePatternData(ui32 size) {
    if (!size) {
        return {};
    }

    const size_t repeats = (size + DataPattern.size() - 1) / DataPattern.size();
    TString result;
    result.reserve(repeats * DataPattern.size());
    for (size_t i = 0; i < repeats; ++i) {
        result += DataPattern;
    }
    result.resize(size);
    return result;
}

} // namespace NKvVolumeStress
