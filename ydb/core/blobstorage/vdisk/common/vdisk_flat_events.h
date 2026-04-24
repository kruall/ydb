#pragma once

#include "vdisk_events.h"

#include <ydb/library/actors/core/event_flat.h>

namespace NKikimr {

    namespace NVDiskFlat {

        using TFlatEventDefs = NActors::TEventFlatLayout;

        struct TLogoBlobIdRaw {
            ui64 RawX1 = 0;
            ui64 RawX2 = 0;
            ui64 RawX3 = 0;
        };

        struct Y_PACKED TVDiskIdRaw {
            ui32 GroupID = 0;
            ui32 GroupGeneration = 0;
            ui8 Ring = 0;
            ui8 Domain = 0;
            ui8 VDisk = 0;
        };

        struct Y_PACKED TMsgQoSRaw {
            ui32 DeadlineSeconds = 0;
            ui8 ExtQueueId = 0;
            ui8 HasDeadlineSeconds = 0;
        };

        struct TTimestampsRaw {
            ui64 SentByDSProxyUs = 0;
            ui64 ReceivedByVDiskUs = 0;
            ui64 SentByVDiskUs = 0;
            ui64 ReceivedByDSProxyUs = 0;
        };

        struct TPutFlagsRaw {
            ui16 Bits = 0;

            static constexpr ui16 IssueKeepFlagMask = 1u << 0;
            static constexpr ui16 IsZeroEntryMask = 1u << 1;
            static constexpr ui16 IgnoreBlockMask = 1u << 2;
            static constexpr ui16 NotifyIfNotReadyMask = 1u << 3;
            static constexpr ui16 HasCookieMask = 1u << 4;
            static constexpr ui16 HasChecksumMask = 1u << 5;

            bool GetIssueKeepFlag() const { return Bits & IssueKeepFlagMask; }
            bool GetIsZeroEntry() const { return Bits & IsZeroEntryMask; }
            bool GetIgnoreBlock() const { return Bits & IgnoreBlockMask; }
            bool GetNotifyIfNotReady() const { return Bits & NotifyIfNotReadyMask; }
            bool HasCookie() const { return Bits & HasCookieMask; }
            bool HasChecksum() const { return Bits & HasChecksumMask; }

            void SetIssueKeepFlag(bool value) { Set(IssueKeepFlagMask, value); }
            void SetIsZeroEntry(bool value) { Set(IsZeroEntryMask, value); }
            void SetIgnoreBlock(bool value) { Set(IgnoreBlockMask, value); }
            void SetNotifyIfNotReady(bool value) { Set(NotifyIfNotReadyMask, value); }
            void SetHasCookie(bool value) { Set(HasCookieMask, value); }
            void SetHasChecksum(bool value) { Set(HasChecksumMask, value); }

        private:
            void Set(ui16 mask, bool value) {
                Bits = value ? Bits | mask : Bits & ~mask;
            }
        };

        struct TPutItemRaw {
            TLogoBlobIdRaw BlobId;
            ui64 FullDataSize = 0;
            ui64 Cookie = 0;
            ui64 Checksum = 0;
            ui64 PayloadOffset = 0;
            ui64 PayloadSize = 0;
            ui32 ExtraBlockChecksOffset = 0;
            ui32 ExtraBlockChecksCount = 0;
            ui32 TraceIdOffset = 0;
            ui32 TraceIdSize = 0;
            TPutFlagsRaw Flags;
        };

        struct Y_PACKED TExtraBlockCheckRaw {
            ui64 TabletId = 0;
            ui32 Generation = 0;
        };

        struct TPutResultFlagsRaw {
            ui16 Bits = 0;

            static constexpr ui16 HasCookieMask = 1u << 0;
            static constexpr ui16 WrittenBeyondBarrierMask = 1u << 1;

            bool HasCookie() const { return Bits & HasCookieMask; }
            bool GetWrittenBeyondBarrier() const { return Bits & WrittenBeyondBarrierMask; }
            void SetHasCookie(bool value) { Set(HasCookieMask, value); }
            void SetWrittenBeyondBarrier(bool value) { Set(WrittenBeyondBarrierMask, value); }

        private:
            void Set(ui16 mask, bool value) {
                Bits = value ? Bits | mask : Bits & ~mask;
            }
        };

        struct TPutResultItemRaw {
            TLogoBlobIdRaw BlobId;
            ui64 Cookie = 0;
            ui32 Status = 0;
            ui32 StatusFlags = 0;
            ui32 ErrorReasonOffset = 0;
            ui32 ErrorReasonSize = 0;
            TPutResultFlagsRaw Flags;
        };

        struct TGetFlagsRaw {
            ui32 Bits = 0;

            static constexpr ui32 NotifyIfNotReadyMask = 1u << 0;
            static constexpr ui32 ShowInternalsMask = 1u << 1;
            static constexpr ui32 IndexOnlyMask = 1u << 2;
            static constexpr ui32 HasCookieMask = 1u << 3;
            static constexpr ui32 SuppressBarrierCheckMask = 1u << 4;
            static constexpr ui32 HasTabletIdMask = 1u << 5;
            static constexpr ui32 AcquireBlockedGenerationMask = 1u << 6;
            static constexpr ui32 HasForceBlockedGenerationMask = 1u << 7;
            static constexpr ui32 HasReaderTabletDataMask = 1u << 8;
            static constexpr ui32 HasForceBlockTabletDataMask = 1u << 9;
            static constexpr ui32 EnablePayloadMask = 1u << 10;

            bool GetNotifyIfNotReady() const { return Bits & NotifyIfNotReadyMask; }
            bool GetShowInternals() const { return Bits & ShowInternalsMask; }
            bool GetIndexOnly() const { return Bits & IndexOnlyMask; }
            bool HasCookie() const { return Bits & HasCookieMask; }
            bool GetSuppressBarrierCheck() const { return Bits & SuppressBarrierCheckMask; }
            bool HasTabletId() const { return Bits & HasTabletIdMask; }
            bool GetAcquireBlockedGeneration() const { return Bits & AcquireBlockedGenerationMask; }
            bool HasForceBlockedGeneration() const { return Bits & HasForceBlockedGenerationMask; }
            bool HasReaderTabletData() const { return Bits & HasReaderTabletDataMask; }
            bool HasForceBlockTabletData() const { return Bits & HasForceBlockTabletDataMask; }
            bool GetEnablePayload() const { return Bits & EnablePayloadMask; }

            void SetNotifyIfNotReady(bool value) { Set(NotifyIfNotReadyMask, value); }
            void SetShowInternals(bool value) { Set(ShowInternalsMask, value); }
            void SetIndexOnly(bool value) { Set(IndexOnlyMask, value); }
            void SetHasCookie(bool value) { Set(HasCookieMask, value); }
            void SetSuppressBarrierCheck(bool value) { Set(SuppressBarrierCheckMask, value); }
            void SetHasTabletId(bool value) { Set(HasTabletIdMask, value); }
            void SetAcquireBlockedGeneration(bool value) { Set(AcquireBlockedGenerationMask, value); }
            void SetHasForceBlockedGeneration(bool value) { Set(HasForceBlockedGenerationMask, value); }
            void SetHasReaderTabletData(bool value) { Set(HasReaderTabletDataMask, value); }
            void SetHasForceBlockTabletData(bool value) { Set(HasForceBlockTabletDataMask, value); }
            void SetEnablePayload(bool value) { Set(EnablePayloadMask, value); }

        private:
            void Set(ui32 mask, bool value) {
                Bits = value ? Bits | mask : Bits & ~mask;
            }
        };

        struct TGetQueryFlagsRaw {
            ui16 Bits = 0;

            static constexpr ui16 HasCookieMask = 1u << 0;
            static constexpr ui16 HasMaxResultsMask = 1u << 1;

            bool HasCookie() const { return Bits & HasCookieMask; }
            bool HasMaxResults() const { return Bits & HasMaxResultsMask; }
            void SetHasCookie(bool value) { Set(HasCookieMask, value); }
            void SetHasMaxResults(bool value) { Set(HasMaxResultsMask, value); }

        private:
            void Set(ui16 mask, bool value) {
                Bits = value ? Bits | mask : Bits & ~mask;
            }
        };

        struct TExtremeQueryRaw {
            TLogoBlobIdRaw BlobId;
            ui64 Shift = 0;
            ui64 Size = 0;
            ui64 Cookie = 0;
            TGetQueryFlagsRaw Flags;
        };

        struct TRangeQueryRaw {
            TLogoBlobIdRaw From;
            TLogoBlobIdRaw To;
            ui64 Cookie = 0;
            ui32 MaxResults = 0;
            TGetQueryFlagsRaw Flags;
        };

        struct TTabletDataRaw {
            ui64 Id = 0;
            ui32 Generation = 0;
        };

        struct TGetResultFlagsRaw {
            ui16 Bits = 0;

            static constexpr ui16 HasCookieMask = 1u << 0;
            static constexpr ui16 IsRangeOverflowMask = 1u << 1;

            bool HasCookie() const { return Bits & HasCookieMask; }
            bool GetIsRangeOverflow() const { return Bits & IsRangeOverflowMask; }
            void SetHasCookie(bool value) { Set(HasCookieMask, value); }
            void SetIsRangeOverflow(bool value) { Set(IsRangeOverflowMask, value); }

        private:
            void Set(ui16 mask, bool value) {
                Bits = value ? Bits | mask : Bits & ~mask;
            }
        };

        struct TGetResultItemFlagsRaw {
            ui16 Bits = 0;

            static constexpr ui16 HasCookieMask = 1u << 0;
            static constexpr ui16 HasIngressMask = 1u << 1;
            static constexpr ui16 KeepMask = 1u << 2;
            static constexpr ui16 DoNotKeepMask = 1u << 3;
            static constexpr ui16 HasBlobMask = 1u << 4;

            bool HasCookie() const { return Bits & HasCookieMask; }
            bool HasIngress() const { return Bits & HasIngressMask; }
            bool GetKeep() const { return Bits & KeepMask; }
            bool GetDoNotKeep() const { return Bits & DoNotKeepMask; }
            bool HasBlob() const { return Bits & HasBlobMask; }

            void SetHasCookie(bool value) { Set(HasCookieMask, value); }
            void SetHasIngress(bool value) { Set(HasIngressMask, value); }
            void SetKeep(bool value) { Set(KeepMask, value); }
            void SetDoNotKeep(bool value) { Set(DoNotKeepMask, value); }
            void SetHasBlob(bool value) { Set(HasBlobMask, value); }

        private:
            void Set(ui16 mask, bool value) {
                Bits = value ? Bits | mask : Bits & ~mask;
            }
        };

        struct TGetResultItemRaw {
            TLogoBlobIdRaw BlobId;
            ui64 Shift = 0;
            ui64 Size = 0;
            ui64 FullDataSize = 0;
            ui64 Cookie = 0;
            ui64 Ingress = 0;
            ui64 PayloadOffset = 0;
            ui64 PayloadSize = 0;
            ui32 Status = 0;
            ui32 PartsOffset = 0;
            ui32 PartsCount = 0;
            TGetResultItemFlagsRaw Flags;
        };

        static_assert(std::is_trivially_copyable_v<TLogoBlobIdRaw>);
        static_assert(std::is_trivially_copyable_v<TVDiskIdRaw>);
        static_assert(std::is_trivially_copyable_v<TMsgQoSRaw>);
        static_assert(std::is_trivially_copyable_v<TTimestampsRaw>);
        static_assert(std::is_trivially_copyable_v<TPutFlagsRaw>);
        static_assert(std::is_trivially_copyable_v<TPutItemRaw>);
        static_assert(std::is_trivially_copyable_v<TExtraBlockCheckRaw>);
        static_assert(std::is_trivially_copyable_v<TPutResultFlagsRaw>);
        static_assert(std::is_trivially_copyable_v<TPutResultItemRaw>);
        static_assert(std::is_trivially_copyable_v<TGetFlagsRaw>);
        static_assert(std::is_trivially_copyable_v<TGetQueryFlagsRaw>);
        static_assert(std::is_trivially_copyable_v<TExtremeQueryRaw>);
        static_assert(std::is_trivially_copyable_v<TRangeQueryRaw>);
        static_assert(std::is_trivially_copyable_v<TTabletDataRaw>);
        static_assert(std::is_trivially_copyable_v<TGetResultFlagsRaw>);
        static_assert(std::is_trivially_copyable_v<TGetResultItemFlagsRaw>);
        static_assert(std::is_trivially_copyable_v<TGetResultItemRaw>);

        inline TLogoBlobIdRaw ToRaw(const TLogoBlobID& id) {
            const ui64 *raw = id.GetRaw();
            return {.RawX1 = raw[0], .RawX2 = raw[1], .RawX3 = raw[2]};
        }

        inline TLogoBlobID FromRaw(const TLogoBlobIdRaw& raw) {
            return TLogoBlobID(raw.RawX1, raw.RawX2, raw.RawX3);
        }

        inline TVDiskIdRaw ToRaw(const TVDiskID& id) {
            return {
                .GroupID = id.GroupID.GetRawId(),
                .GroupGeneration = id.GroupGeneration,
                .Ring = id.FailRealm,
                .Domain = id.FailDomain,
                .VDisk = id.VDisk,
            };
        }

        inline TVDiskID FromRaw(const TVDiskIdRaw& raw) {
            return TVDiskID(raw.GroupID, raw.GroupGeneration, raw.Ring, raw.Domain, raw.VDisk);
        }

        inline TMsgQoSRaw MakeMsgQoSRaw(TInstant deadline, NKikimrBlobStorage::EVDiskQueueId queueId) {
            return {
                .DeadlineSeconds = deadline != TInstant::Max() ? static_cast<ui32>(deadline.Seconds()) : 0,
                .ExtQueueId = static_cast<ui8>(queueId),
                .HasDeadlineSeconds = static_cast<ui8>(deadline != TInstant::Max()),
            };
        }

    } // namespace NVDiskFlat

    using TEvVPutFlatTVDiskIdTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TVDiskIdRaw, 0>;
    using TEvVPutFlatTMsgQoSTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TMsgQoSRaw, 1>;
    using TEvVPutFlatTHandleClassTag = NVDiskFlat::TFlatEventDefs::FixedField<ui32, 2>;
    using TEvVPutFlatTTimestampsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TTimestampsRaw, 3>;
    using TEvVPutFlatTBlobIdTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TLogoBlobIdRaw, 4>;
    using TEvVPutFlatTFullDataSizeTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 5>;
    using TEvVPutFlatTCookieTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 6>;
    using TEvVPutFlatTChecksumTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 7>;
    using TEvVPutFlatTFlagsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TPutFlagsRaw, 8>;
    using TEvVPutFlatTExtraBlockChecksTag = NVDiskFlat::TFlatEventDefs::ArrayField<NVDiskFlat::TExtraBlockCheckRaw, 9>;
    using TEvVPutFlatTPayloadTag = NVDiskFlat::TFlatEventDefs::BytesField<10>;
    using TEvVPutFlatTItemsTag = NVDiskFlat::TFlatEventDefs::ArrayField<NVDiskFlat::TPutItemRaw, 11>;
    using TEvVPutFlatTTraceIdsTag = NVDiskFlat::TFlatEventDefs::BytesField<12>;

    using TEvVPutFlatTSinglePutV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        NVDiskFlat::TFlatEventDefs::WithPayloadType<ui8>,
        TEvVPutFlatTVDiskIdTag,
        TEvVPutFlatTMsgQoSTag,
        TEvVPutFlatTHandleClassTag,
        TEvVPutFlatTTimestampsTag,
        TEvVPutFlatTBlobIdTag,
        TEvVPutFlatTFullDataSizeTag,
        TEvVPutFlatTCookieTag,
        TEvVPutFlatTChecksumTag,
        TEvVPutFlatTFlagsTag,
        TEvVPutFlatTExtraBlockChecksTag,
        TEvVPutFlatTPayloadTag>;
    using TEvVPutFlatTMultiPutV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        NVDiskFlat::TFlatEventDefs::WithPayloadType<ui8>,
        TEvVPutFlatTVDiskIdTag,
        TEvVPutFlatTMsgQoSTag,
        TEvVPutFlatTHandleClassTag,
        TEvVPutFlatTTimestampsTag,
        TEvVPutFlatTCookieTag,
        TEvVPutFlatTFlagsTag,
        TEvVPutFlatTItemsTag,
        TEvVPutFlatTExtraBlockChecksTag,
        TEvVPutFlatTPayloadTag,
        TEvVPutFlatTTraceIdsTag>;
    using TEvVPutFlatTVersions = NVDiskFlat::TFlatEventDefs::Versions<TEvVPutFlatTSinglePutV1, TEvVPutFlatTMultiPutV1>;

    struct TEvBlobStorage::TEvVPutFlat
        : NActors::TEventFlat<TEvBlobStorage::TEvVPutFlat, TEvVPutFlatTVersions>
        , TEventWithRelevanceTracker
    {
        using TBase = NActors::TEventFlat<TEvBlobStorage::TEvVPutFlat, TEvVPutFlatTVersions>;

        static constexpr ui32 EventType = TEvBlobStorage::EvVPutFlat;

        using TScheme = TEvVPutFlatTVersions;
        using TSinglePutV1 = TEvVPutFlatTSinglePutV1;
        using TMultiPutV1 = TEvVPutFlatTMultiPutV1;

        using TVDiskIdTag = TEvVPutFlatTVDiskIdTag;
        using TMsgQoSTag = TEvVPutFlatTMsgQoSTag;
        using THandleClassTag = TEvVPutFlatTHandleClassTag;
        using TTimestampsTag = TEvVPutFlatTTimestampsTag;
        using TBlobIdTag = TEvVPutFlatTBlobIdTag;
        using TFullDataSizeTag = TEvVPutFlatTFullDataSizeTag;
        using TCookieTag = TEvVPutFlatTCookieTag;
        using TChecksumTag = TEvVPutFlatTChecksumTag;
        using TFlagsTag = TEvVPutFlatTFlagsTag;
        using TExtraBlockChecksTag = TEvVPutFlatTExtraBlockChecksTag;
        using TPayloadTag = TEvVPutFlatTPayloadTag;
        using TItemsTag = TEvVPutFlatTItemsTag;
        using TTraceIdsTag = TEvVPutFlatTTraceIdsTag;

        mutable NLWTrace::TOrbit Orbit;
        bool RewriteBlob = false;
        bool IsInternal = false;

        friend class NActors::TEventFlat<TEvBlobStorage::TEvVPutFlat, TEvVPutFlatTVersions>;

        static TEvVPutFlat* MakeSinglePut(const TLogoBlobID& logoBlobId, TRope buffer, const TVDiskID& vdisk,
            bool ignoreBlock, const ui64 *cookie, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls,
            bool checksumming);

        static TEvVPutFlat* MakeMultiPut(const TVDiskID& vdisk, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls,
            bool ignoreBlock, const ui64 *cookie = nullptr);

        bool IsSinglePut() const { return IsVersion<TSinglePutV1>(); }
        bool IsMultiPut() const { return IsVersion<TMultiPutV1>(); }

        void AddVPut(const TLogoBlobID& logoBlobId, const TRcBuf& buffer, ui64 *cookie, bool issueKeepFlag,
            bool ignoreBlock, bool isZeroEntry, std::vector<std::pair<ui64, ui32>> *extraBlockChecks,
            NWilson::TTraceId traceId, bool checksumming);

        TVDiskID GetVDiskID() const { return NVDiskFlat::FromRaw(Field<TVDiskIdTag>()); }
        NKikimrBlobStorage::EPutHandleClass GetHandleClass() const {
            return static_cast<NKikimrBlobStorage::EPutHandleClass>(static_cast<ui32>(Field<THandleClassTag>()));
        }
        NKikimrBlobStorage::EVDiskQueueId GetExtQueueId() const {
            return static_cast<NKikimrBlobStorage::EVDiskQueueId>(static_cast<NVDiskFlat::TMsgQoSRaw>(Field<TMsgQoSTag>()).ExtQueueId);
        }
        ui64 GetBufferBytes() const;
        ui64 GetBufferBytes(ui64 idx) const;
        TRope GetBuffer() const;
        TRope GetItemBuffer(ui64 itemIdx) const;
        ui64 GetSumBlobSize() const;
        bool Validate(TString& errorReason) const;
        TString ToString() const override;
    };

    using TEvVPutResultFlatTStatusTag = NVDiskFlat::TFlatEventDefs::FixedField<ui32, 0>;
    using TEvVPutResultFlatTVDiskIdTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TVDiskIdRaw, 1>;
    using TEvVPutResultFlatTBlobIdTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TLogoBlobIdRaw, 2>;
    using TEvVPutResultFlatTCookieTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 3>;
    using TEvVPutResultFlatTStatusFlagsTag = NVDiskFlat::TFlatEventDefs::FixedField<ui32, 4>;
    using TEvVPutResultFlatTFlagsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TPutResultFlagsRaw, 5>;
    using TEvVPutResultFlatTFreeSpaceShareTag = NVDiskFlat::TFlatEventDefs::FixedField<float, 6>;
    using TEvVPutResultFlatTIncarnationGuidTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 7>;
    using TEvVPutResultFlatTTimestampsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TTimestampsRaw, 8>;
    using TEvVPutResultFlatTMsgQoSTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TMsgQoSRaw, 9>;
    using TEvVPutResultFlatTErrorReasonTag = NVDiskFlat::TFlatEventDefs::BytesField<10>;
    using TEvVPutResultFlatTItemsTag = NVDiskFlat::TFlatEventDefs::ArrayField<NVDiskFlat::TPutResultItemRaw, 11>;

    using TEvVPutResultFlatTSinglePutResultV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        NVDiskFlat::TFlatEventDefs::WithPayloadType<ui8>,
        TEvVPutResultFlatTStatusTag,
        TEvVPutResultFlatTVDiskIdTag,
        TEvVPutResultFlatTBlobIdTag,
        TEvVPutResultFlatTCookieTag,
        TEvVPutResultFlatTStatusFlagsTag,
        TEvVPutResultFlatTFlagsTag,
        TEvVPutResultFlatTFreeSpaceShareTag,
        TEvVPutResultFlatTIncarnationGuidTag,
        TEvVPutResultFlatTTimestampsTag,
        TEvVPutResultFlatTMsgQoSTag,
        TEvVPutResultFlatTErrorReasonTag>;
    using TEvVPutResultFlatTMultiPutResultV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        NVDiskFlat::TFlatEventDefs::WithPayloadType<ui8>,
        TEvVPutResultFlatTStatusTag,
        TEvVPutResultFlatTVDiskIdTag,
        TEvVPutResultFlatTCookieTag,
        TEvVPutResultFlatTStatusFlagsTag,
        TEvVPutResultFlatTFlagsTag,
        TEvVPutResultFlatTFreeSpaceShareTag,
        TEvVPutResultFlatTIncarnationGuidTag,
        TEvVPutResultFlatTTimestampsTag,
        TEvVPutResultFlatTMsgQoSTag,
        TEvVPutResultFlatTErrorReasonTag,
        TEvVPutResultFlatTItemsTag>;
    using TEvVPutResultFlatTVersions = NVDiskFlat::TFlatEventDefs::Versions<TEvVPutResultFlatTSinglePutResultV1, TEvVPutResultFlatTMultiPutResultV1>;

    struct TEvBlobStorage::TEvVPutResultFlat
        : NActors::TEventFlat<TEvBlobStorage::TEvVPutResultFlat, TEvVPutResultFlatTVersions>
    {
        using TBase = NActors::TEventFlat<TEvBlobStorage::TEvVPutResultFlat, TEvVPutResultFlatTVersions>;

        static constexpr ui32 EventType = TEvBlobStorage::EvVPutResultFlat;

        using TScheme = TEvVPutResultFlatTVersions;
        using TSinglePutResultV1 = TEvVPutResultFlatTSinglePutResultV1;
        using TMultiPutResultV1 = TEvVPutResultFlatTMultiPutResultV1;

        using TStatusTag = TEvVPutResultFlatTStatusTag;
        using TVDiskIdTag = TEvVPutResultFlatTVDiskIdTag;
        using TBlobIdTag = TEvVPutResultFlatTBlobIdTag;
        using TCookieTag = TEvVPutResultFlatTCookieTag;
        using TStatusFlagsTag = TEvVPutResultFlatTStatusFlagsTag;
        using TFlagsTag = TEvVPutResultFlatTFlagsTag;
        using TFreeSpaceShareTag = TEvVPutResultFlatTFreeSpaceShareTag;
        using TIncarnationGuidTag = TEvVPutResultFlatTIncarnationGuidTag;
        using TTimestampsTag = TEvVPutResultFlatTTimestampsTag;
        using TMsgQoSTag = TEvVPutResultFlatTMsgQoSTag;
        using TErrorReasonTag = TEvVPutResultFlatTErrorReasonTag;
        using TItemsTag = TEvVPutResultFlatTItemsTag;

        mutable NLWTrace::TOrbit Orbit;

        friend class NActors::TEventFlat<TEvBlobStorage::TEvVPutResultFlat, TEvVPutResultFlatTVersions>;

        static TEvVPutResultFlat* MakeSinglePutResult(NKikimrProto::EReplyStatus status, const TLogoBlobID& logoBlobId,
            const TVDiskID& vdisk, const ui64 *cookie, TOutOfSpaceStatus oosStatus, ui64 incarnationGuid,
            const TString& errorReason);

        static TEvVPutResultFlat* MakeMultiPutResult(NKikimrProto::EReplyStatus status, const TVDiskID& vdisk,
            const ui64 *cookie, TOutOfSpaceStatus oosStatus, ui64 incarnationGuid, const TString& errorReason);

        bool IsSinglePutResult() const { return IsVersion<TSinglePutResultV1>(); }
        bool IsMultiPutResult() const { return IsVersion<TMultiPutResultV1>(); }

        void AddVPutResult(NKikimrProto::EReplyStatus status, const TString& errorReason, const TLogoBlobID& logoBlobId,
            ui64 *cookie, ui32 statusFlags = 0, bool writtenBeyondBarrier = false);

        NKikimrProto::EReplyStatus GetStatus() const {
            return static_cast<NKikimrProto::EReplyStatus>(static_cast<ui32>(Field<TStatusTag>()));
        }
        TString ToString() const override;
    };

    using TEvVGetFlatTVDiskIdTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TVDiskIdRaw, 0>;
    using TEvVGetFlatTMsgQoSTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TMsgQoSRaw, 1>;
    using TEvVGetFlatTHandleClassTag = NVDiskFlat::TFlatEventDefs::FixedField<ui32, 2>;
    using TEvVGetFlatTTimestampsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TTimestampsRaw, 3>;
    using TEvVGetFlatTFlagsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TGetFlagsRaw, 4>;
    using TEvVGetFlatTCookieTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 5>;
    using TEvVGetFlatTTabletIdTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 6>;
    using TEvVGetFlatTForceBlockedGenerationTag = NVDiskFlat::TFlatEventDefs::FixedField<ui32, 7>;
    using TEvVGetFlatTReaderTabletDataTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TTabletDataRaw, 8>;
    using TEvVGetFlatTForceBlockTabletDataTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TTabletDataRaw, 9>;
    using TEvVGetFlatTExtremeQueriesTag = NVDiskFlat::TFlatEventDefs::ArrayField<NVDiskFlat::TExtremeQueryRaw, 10>;
    using TEvVGetFlatTRangeQueryTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TRangeQueryRaw, 11>;
    using TEvVGetFlatTExtremeDataMarkerTag = NVDiskFlat::TFlatEventDefs::FixedField<ui8, 12>;

    using TEvVGetFlatTExtremeIndexV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        TEvVGetFlatTVDiskIdTag,
        TEvVGetFlatTMsgQoSTag,
        TEvVGetFlatTHandleClassTag,
        TEvVGetFlatTTimestampsTag,
        TEvVGetFlatTFlagsTag,
        TEvVGetFlatTCookieTag,
        TEvVGetFlatTTabletIdTag,
        TEvVGetFlatTForceBlockedGenerationTag,
        TEvVGetFlatTReaderTabletDataTag,
        TEvVGetFlatTForceBlockTabletDataTag,
        TEvVGetFlatTExtremeQueriesTag>;
    using TEvVGetFlatTExtremeDataV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        TEvVGetFlatTVDiskIdTag,
        TEvVGetFlatTMsgQoSTag,
        TEvVGetFlatTHandleClassTag,
        TEvVGetFlatTTimestampsTag,
        TEvVGetFlatTFlagsTag,
        TEvVGetFlatTCookieTag,
        TEvVGetFlatTTabletIdTag,
        TEvVGetFlatTForceBlockedGenerationTag,
        TEvVGetFlatTReaderTabletDataTag,
        TEvVGetFlatTForceBlockTabletDataTag,
        TEvVGetFlatTExtremeQueriesTag,
        TEvVGetFlatTExtremeDataMarkerTag>;
    using TEvVGetFlatTRangeIndexV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        TEvVGetFlatTVDiskIdTag,
        TEvVGetFlatTMsgQoSTag,
        TEvVGetFlatTHandleClassTag,
        TEvVGetFlatTTimestampsTag,
        TEvVGetFlatTFlagsTag,
        TEvVGetFlatTCookieTag,
        TEvVGetFlatTTabletIdTag,
        TEvVGetFlatTForceBlockedGenerationTag,
        TEvVGetFlatTReaderTabletDataTag,
        TEvVGetFlatTForceBlockTabletDataTag,
        TEvVGetFlatTRangeQueryTag>;
    using TEvVGetFlatTVersions = NVDiskFlat::TFlatEventDefs::Versions<TEvVGetFlatTExtremeIndexV1, TEvVGetFlatTExtremeDataV1, TEvVGetFlatTRangeIndexV1>;

    struct TEvBlobStorage::TEvVGetFlat
        : NActors::TEventFlat<TEvBlobStorage::TEvVGetFlat, TEvVGetFlatTVersions>
        , TEventWithRelevanceTracker
    {
        using TBase = NActors::TEventFlat<TEvBlobStorage::TEvVGetFlat, TEvVGetFlatTVersions>;

        static constexpr ui32 EventType = TEvBlobStorage::EvVGetFlat;

        using TScheme = TEvVGetFlatTVersions;
        using TExtremeIndexV1 = TEvVGetFlatTExtremeIndexV1;
        using TExtremeDataV1 = TEvVGetFlatTExtremeDataV1;
        using TRangeIndexV1 = TEvVGetFlatTRangeIndexV1;

        using TVDiskIdTag = TEvVGetFlatTVDiskIdTag;
        using TMsgQoSTag = TEvVGetFlatTMsgQoSTag;
        using THandleClassTag = TEvVGetFlatTHandleClassTag;
        using TTimestampsTag = TEvVGetFlatTTimestampsTag;
        using TFlagsTag = TEvVGetFlatTFlagsTag;
        using TCookieTag = TEvVGetFlatTCookieTag;
        using TTabletIdTag = TEvVGetFlatTTabletIdTag;
        using TForceBlockedGenerationTag = TEvVGetFlatTForceBlockedGenerationTag;
        using TReaderTabletDataTag = TEvVGetFlatTReaderTabletDataTag;
        using TForceBlockTabletDataTag = TEvVGetFlatTForceBlockTabletDataTag;
        using TExtremeQueriesTag = TEvVGetFlatTExtremeQueriesTag;
        using TRangeQueryTag = TEvVGetFlatTRangeQueryTag;

        using TForceBlockTabletData = TEvBlobStorage::TEvGet::TForceBlockTabletData;

        struct TExtremeQuery : std::tuple<TLogoBlobID, ui32, ui32, const ui64*> {
            TExtremeQuery(const TLogoBlobID& logoBlobId, ui32 sh, ui32 sz, const ui64 *cookie = nullptr)
                : std::tuple<TLogoBlobID, ui32, ui32, const ui64*>(logoBlobId, sh, sz, cookie)
            {}

            TExtremeQuery(const TLogoBlobID& logoBlobId)
                : TExtremeQuery(logoBlobId, 0, 0)
            {}
        };

        mutable NLWTrace::TOrbit Orbit;
        bool IsInternal = false;

        friend class NActors::TEventFlat<TEvBlobStorage::TEvVGetFlat, TEvVGetFlatTVersions>;

        static std::unique_ptr<TEvVGetFlat> CreateExtremeIndexQuery(const TVDiskID& vdisk, TInstant deadline,
            NKikimrBlobStorage::EGetHandleClass cls, TEvBlobStorage::TEvVGet::EFlags flags = TEvBlobStorage::TEvVGet::EFlags::None,
            TMaybe<ui64> requestCookie = {}, std::initializer_list<TExtremeQuery> queries = {},
            std::optional<TForceBlockTabletData> forceBlockTabletData = {});
        static std::unique_ptr<TEvVGetFlat> CreateExtremeDataQuery(const TVDiskID& vdisk, TInstant deadline,
            NKikimrBlobStorage::EGetHandleClass cls, TEvBlobStorage::TEvVGet::EFlags flags = TEvBlobStorage::TEvVGet::EFlags::None,
            TMaybe<ui64> requestCookie = {}, std::initializer_list<TExtremeQuery> queries = {},
            std::optional<TForceBlockTabletData> forceBlockTabletData = {});
        static std::unique_ptr<TEvVGetFlat> CreateRangeIndexQuery(const TVDiskID& vdisk, TInstant deadline,
            NKikimrBlobStorage::EGetHandleClass cls, TEvBlobStorage::TEvVGet::EFlags flags, TMaybe<ui64> requestCookie,
            const TLogoBlobID& fromId, const TLogoBlobID& toId, ui32 maxResults = 0, const ui64 *cookie = nullptr,
            std::optional<TForceBlockTabletData> forceBlockTabletData = {});

        bool IsExtremeIndexQuery() const { return IsVersion<TExtremeIndexV1>(); }
        bool IsExtremeDataQuery() const { return IsVersion<TExtremeDataV1>(); }
        bool IsRangeIndexQuery() const { return IsVersion<TRangeIndexV1>(); }
        bool IsExtremeQuery() const { return IsExtremeIndexQuery() || IsExtremeDataQuery(); }

        void AddExtremeQuery(const TLogoBlobID& logoBlobId, ui32 sh, ui32 sz, const ui64 *cookie = nullptr);

        TVDiskID GetVDiskID() const { return NVDiskFlat::FromRaw(Field<TVDiskIdTag>()); }
        NKikimrBlobStorage::EGetHandleClass GetHandleClass() const {
            return static_cast<NKikimrBlobStorage::EGetHandleClass>(static_cast<ui32>(Field<THandleClassTag>()));
        }
        NKikimrBlobStorage::EVDiskQueueId GetExtQueueId() const {
            return static_cast<NKikimrBlobStorage::EVDiskQueueId>(static_cast<NVDiskFlat::TMsgQoSRaw>(Field<TMsgQoSTag>()).ExtQueueId);
        }
        bool Validate(TString& errorReason) const;
        TString ToString() const override;
    };

    using TEvVGetResultFlatTStatusTag = NVDiskFlat::TFlatEventDefs::FixedField<ui32, 0>;
    using TEvVGetResultFlatTVDiskIdTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TVDiskIdRaw, 1>;
    using TEvVGetResultFlatTCookieTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 2>;
    using TEvVGetResultFlatTMsgQoSTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TMsgQoSRaw, 3>;
    using TEvVGetResultFlatTBlockedGenerationTag = NVDiskFlat::TFlatEventDefs::FixedField<ui32, 4>;
    using TEvVGetResultFlatTTimestampsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TTimestampsRaw, 5>;
    using TEvVGetResultFlatTIncarnationGuidTag = NVDiskFlat::TFlatEventDefs::FixedField<ui64, 6>;
    using TEvVGetResultFlatTFlagsTag = NVDiskFlat::TFlatEventDefs::FixedField<NVDiskFlat::TGetResultFlagsRaw, 7>;
    using TEvVGetResultFlatTErrorReasonTag = NVDiskFlat::TFlatEventDefs::BytesField<8>;
    using TEvVGetResultFlatTItemsTag = NVDiskFlat::TFlatEventDefs::ArrayField<NVDiskFlat::TGetResultItemRaw, 9>;
    using TEvVGetResultFlatTPayloadTag = NVDiskFlat::TFlatEventDefs::BytesField<10>;
    using TEvVGetResultFlatTPartsTag = NVDiskFlat::TFlatEventDefs::ArrayField<ui32, 11>;

    using TEvVGetResultFlatTV1 = NVDiskFlat::TFlatEventDefs::Scheme<
        NVDiskFlat::TFlatEventDefs::WithPayloadType<ui8>,
        TEvVGetResultFlatTStatusTag,
        TEvVGetResultFlatTVDiskIdTag,
        TEvVGetResultFlatTCookieTag,
        TEvVGetResultFlatTMsgQoSTag,
        TEvVGetResultFlatTBlockedGenerationTag,
        TEvVGetResultFlatTTimestampsTag,
        TEvVGetResultFlatTIncarnationGuidTag,
        TEvVGetResultFlatTFlagsTag,
        TEvVGetResultFlatTErrorReasonTag,
        TEvVGetResultFlatTItemsTag,
        TEvVGetResultFlatTPayloadTag,
        TEvVGetResultFlatTPartsTag>;
    using TEvVGetResultFlatTVersions = NVDiskFlat::TFlatEventDefs::Versions<TEvVGetResultFlatTV1>;

    struct TEvBlobStorage::TEvVGetResultFlat
        : NActors::TEventFlat<TEvBlobStorage::TEvVGetResultFlat, TEvVGetResultFlatTVersions>
    {
        using TBase = NActors::TEventFlat<TEvBlobStorage::TEvVGetResultFlat, TEvVGetResultFlatTVersions>;

        static constexpr ui32 EventType = TEvBlobStorage::EvVGetResultFlat;

        using TScheme = TEvVGetResultFlatTVersions;
        using TV1 = TEvVGetResultFlatTV1;

        using TStatusTag = TEvVGetResultFlatTStatusTag;
        using TVDiskIdTag = TEvVGetResultFlatTVDiskIdTag;
        using TCookieTag = TEvVGetResultFlatTCookieTag;
        using TMsgQoSTag = TEvVGetResultFlatTMsgQoSTag;
        using TBlockedGenerationTag = TEvVGetResultFlatTBlockedGenerationTag;
        using TTimestampsTag = TEvVGetResultFlatTTimestampsTag;
        using TIncarnationGuidTag = TEvVGetResultFlatTIncarnationGuidTag;
        using TFlagsTag = TEvVGetResultFlatTFlagsTag;
        using TErrorReasonTag = TEvVGetResultFlatTErrorReasonTag;
        using TItemsTag = TEvVGetResultFlatTItemsTag;
        using TPayloadTag = TEvVGetResultFlatTPayloadTag;
        using TPartsTag = TEvVGetResultFlatTPartsTag;

        mutable NLWTrace::TOrbit Orbit;

        friend class NActors::TEventFlat<TEvBlobStorage::TEvVGetResultFlat, TEvVGetResultFlatTVersions>;

        static TEvVGetResultFlat* Make(NKikimrProto::EReplyStatus status, const TVDiskID& vdisk,
            TMaybe<ui64> cookie, ui64 incarnationGuid);

        void MarkRangeOverflow();
        void AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID& logoBlobId, ui64 sh,
            std::variant<TRope, ui32> dataOrSize, const ui64 *cookie = nullptr, const ui64 *ingress = nullptr,
            bool keep = false, bool doNotKeep = false);
        void AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID& logoBlobId, const ui64 *cookie = nullptr,
            const ui64 *ingress = nullptr, const NMatrix::TVectorType *local = nullptr, bool keep = false,
            bool doNotKeep = false);
        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason, const TEvVGetFlat& request);

        NKikimrProto::EReplyStatus GetStatus() const {
            return static_cast<NKikimrProto::EReplyStatus>(static_cast<ui32>(Field<TStatusTag>()));
        }
        bool HasBlob(const NVDiskFlat::TGetResultItemRaw& item) const;
        ui32 GetBlobSize(const NVDiskFlat::TGetResultItemRaw& item) const;
        TRope GetBlobData(const NVDiskFlat::TGetResultItemRaw& item) const;
        TString ToString() const override;
    };

} // namespace NKikimr
