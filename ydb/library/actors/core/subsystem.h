#pragma once

#include <cstddef>

namespace NActors {

    class ISubSystem {
    public:
        virtual ~ISubSystem() = default;
    };

    class TSubSystemRegistry {
    private:
        static size_t NextIndex() noexcept;

    public:
        template<class T>
        struct TItem {
            static size_t Index() noexcept {
                static const size_t value = NextIndex();
                return value;
            }
        };
    };

} // namespace NActors
