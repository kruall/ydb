#pragma once
#include "defs.h"
#include <util/stream/output.h>

namespace NKikimr {
namespace NKeyValue {

struct TRequestType {
    enum EType {
        ReadOnly = 0,
        WriteOnly = 1,
        ReadWrite = 2,
        ReadOnlyInline = 3
    };
};

inline const char* RequestTypeName(TRequestType::EType requestType) {
    switch (requestType) {
        case TRequestType::ReadOnly:
            return "ReadOnly";
        case TRequestType::WriteOnly:
            return "WriteOnly";
        case TRequestType::ReadWrite:
            return "ReadWrite";
        case TRequestType::ReadOnlyInline:
            return "ReadOnlyInline";
    }
    return "Unknown";
}

} // NKeyValue
} // NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NKeyValue::TRequestType::EType, stream, value) {
    stream << NKikimr::NKeyValue::RequestTypeName(value);
}
