#include "skeleton_vmultiput_actor.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>
#include <ydb/core/blobstorage/base/batched_vec.h>

namespace NKikimr {
    namespace NPrivate {

        template <class TEvent>
        struct TMultiPutTraits;

        template <>
        struct TMultiPutTraits<TEvBlobStorage::TEvVMultiPut> {
            static ui64 ItemsSize(const TEvBlobStorage::TEvVMultiPut& ev) { return ev.Record.ItemsSize(); }
            static TVDiskID GetVDiskID(const TEvBlobStorage::TEvVMultiPut& ev) { return VDiskIDFromVDiskID(ev.Record.GetVDiskID()); }
            static NKikimrBlobStorage::EPutHandleClass GetHandleClass(const TEvBlobStorage::TEvVMultiPut& ev) { return ev.Record.GetHandleClass(); }
            static ui64 GetBufferBytes(const TEvBlobStorage::TEvVMultiPut& ev) { return ev.GetBufferBytes(); }
            static TLogoBlobID GetBlobId(const TEvBlobStorage::TEvVMultiPut& ev, ui64 idx) {
                return LogoBlobIDFromLogoBlobID(ev.Record.GetItems(idx).GetBlobID());
            }
            static bool HasCookie(const TEvBlobStorage::TEvVMultiPut& ev) { return ev.Record.HasCookie(); }
            static ui64 GetCookie(const TEvBlobStorage::TEvVMultiPut& ev) { return ev.Record.GetCookie(); }
            static bool HasItemCookie(const TEvBlobStorage::TEvVMultiPut& ev, ui64 idx) { return ev.Record.GetItems(idx).HasCookie(); }
            static ui64 GetItemCookie(const TEvBlobStorage::TEvVMultiPut& ev, ui64 idx) { return ev.Record.GetItems(idx).GetCookie(); }
        };

        template <>
        struct TMultiPutTraits<TEvBlobStorage::TEvVPutFlat> {
            static ui64 ItemsSize(const TEvBlobStorage::TEvVPutFlat& ev) { return ev.GetItemsCount(); }
            static TVDiskID GetVDiskID(const TEvBlobStorage::TEvVPutFlat& ev) { return ev.GetVDiskID(); }
            static NKikimrBlobStorage::EPutHandleClass GetHandleClass(const TEvBlobStorage::TEvVPutFlat& ev) { return ev.GetHandleClass(); }
            static ui64 GetBufferBytes(const TEvBlobStorage::TEvVPutFlat& ev) { return ev.GetBufferBytes(); }
            static TLogoBlobID GetBlobId(const TEvBlobStorage::TEvVPutFlat& ev, ui64 idx) {
                return NVDiskFlat::FromRaw(ev.GetItem(idx).BlobId);
            }
            static bool HasCookie(const TEvBlobStorage::TEvVPutFlat& ev) { return ev.GetFlags().HasCookie(); }
            static ui64 GetCookie(const TEvBlobStorage::TEvVPutFlat& ev) { return ev.GetCookie(); }
            static bool HasItemCookie(const TEvBlobStorage::TEvVPutFlat& ev, ui64 idx) { return ev.GetItem(idx).Flags.HasCookie(); }
            static ui64 GetItemCookie(const TEvBlobStorage::TEvVPutFlat& ev, ui64 idx) { return ev.GetItem(idx).Cookie; }
        };

        template <class TEvent>
        class TBufferVMultiPutActor : public TActorBootstrapped<TBufferVMultiPutActor<TEvent>> {
            using TBase = TActorBootstrapped<TBufferVMultiPutActor<TEvent>>;
            using TThis = TBufferVMultiPutActor<TEvent>;

            friend TActorBootstrapped<TBufferVMultiPutActor<TEvent>>;

            struct TItem {
                NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
                TString ErrorReason;
                TLogoBlobID BlobId;
                ui64 Cookie = 0;
                ui32 StatusFlags = 0;
                bool Received = false;
                bool HasCookie = false;
                bool WrittenBeyondBarrier;

                TString ToString() const {
                    return TStringBuilder()
                        << "{"
                        << " Status# " << NKikimrProto::EReplyStatus_Name(Status)
                        << " ErrorReason# " << '"' << EscapeC(ErrorReason) << '"'
                        << " BlobId# " << BlobId.ToString()
                        << " HasCookie# " << HasCookie
                        << " Cookie# " << Cookie
                        << " StatusFlags# " << NPDisk::StatusFlagsToString(StatusFlags)
                        << " Received# " << Received
                        << " WrittenBeyondBarrier# " << WrittenBeyondBarrier
                        << " }";
                }
            };

            TBatchedVec<TItem> Items;
            ui64 ReceivedResults;
            ::NMonitoring::TDynamicCounters::TCounterPtr MultiPutResMsgsPtr;

            TAutoPtr<TEventHandle<TEvent>> Event;
            TActorId LeaderId;
            TOutOfSpaceStatus OOSStatus;

            const ui64 IncarnationGuid;

            TIntrusivePtr<TVDiskContext> VCtx;

        public:
            TBufferVMultiPutActor(TActorId leaderId, const TBatchedVec<NKikimrProto::EReplyStatus> &statuses,
                    TOutOfSpaceStatus oosStatus, TAutoPtr<TEventHandle<TEvent>> &ev,
                    ::NMonitoring::TDynamicCounters::TCounterPtr multiPutResMsgsPtr,
                    ui64 incarnationGuid, TIntrusivePtr<TVDiskContext>& vCtx)
                : TBase()
                , Items(TMultiPutTraits<TEvent>::ItemsSize(*ev->Get()))
                , ReceivedResults(0)
                , MultiPutResMsgsPtr(multiPutResMsgsPtr)
                , Event(ev)
                , LeaderId(leaderId)
                , OOSStatus(oosStatus)
                , IncarnationGuid(incarnationGuid)
                , VCtx(vCtx)
            {
                Y_VERIFY_S(statuses.size() == Items.size(), VCtx->VDiskLogPrefix);
                for (ui64 idx = 0; idx < Items.size(); ++idx) {
                    Items[idx].Status = statuses[idx];
                }
            }

        private:
            void SendResponseAndDie(const TActorContext &ctx) {
                TVDiskID vdisk = TMultiPutTraits<TEvent>::GetVDiskID(*Event->Get());

                ui64 cookieValue;
                ui64 *cookie = nullptr;
                if (TMultiPutTraits<TEvent>::HasCookie(*Event->Get())) {
                    cookieValue = TMultiPutTraits<TEvent>::GetCookie(*Event->Get());
                    cookie = &cookieValue;
                }

                auto handleClass = TMultiPutTraits<TEvent>::GetHandleClass(*Event->Get());
                if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvVMultiPut>) {
                    NKikimrBlobStorage::TEvVMultiPut &vMultiPutRecord = Event->Get()->Record;
                    TInstant now = TAppData::TimeProvider->Now();
                    const NVDiskMon::TLtcHistoPtr &histoPtr = VCtx->Histograms.GetHistogram(handleClass);
                    const ui64 bufferSizeBytes = Event->Get()->GetBufferBytes();
                    auto vMultiPutResult = std::make_unique<TEvBlobStorage::TEvVMultiPutResult>(NKikimrProto::OK, vdisk, cookie,
                        now, Event->Get()->GetCachedByteSize(), &vMultiPutRecord, nullptr, MultiPutResMsgsPtr,
                        histoPtr, bufferSizeBytes, IncarnationGuid, TString());

                    for (ui64 idx = 0; idx < Items.size(); ++idx) {
                        TItem &result = Items[idx];
                        vMultiPutResult->AddVPutResult(result.Status, result.ErrorReason, result.BlobId,
                            result.HasCookie ? &result.Cookie : nullptr, result.StatusFlags, result.WrittenBeyondBarrier);
                    }

                    vMultiPutResult->Record.SetStatusFlags(OOSStatus.Flags);
                    SendVDiskResponse(ctx, Event->Sender, vMultiPutResult.release(), Event->Cookie, VCtx, handleClass);
                } else {
                    auto vPutResult = std::unique_ptr<TEvBlobStorage::TEvVPutResultFlat>(TEvBlobStorage::TEvVPutResultFlat::MakeMultiPutResult(
                        NKikimrProto::OK, vdisk, cookie, OOSStatus, IncarnationGuid, TString()));
                    for (ui64 idx = 0; idx < Items.size(); ++idx) {
                        TItem &result = Items[idx];
                        vPutResult->AddVPutResult(result.Status, result.ErrorReason, result.BlobId,
                            result.HasCookie ? &result.Cookie : nullptr, result.StatusFlags, result.WrittenBeyondBarrier);
                    }
                    SendVDiskResponse(ctx, Event->Sender, vPutResult.release(), Event->Cookie, VCtx, handleClass);
                }
                PassAway();
            }

            void Handle(TEvVMultiPutItemResult::TPtr &ev, const TActorContext &ctx) {
                TLogoBlobID blobId = ev->Get()->BlobId;
                ui64 idx = ev->Get()->ItemIdx;
                Y_VERIFY_S(idx < Items.size(), VCtx->VDiskLogPrefix
                    << "itemIdx# " << idx << " ItemsSize# " << (ui64)Items.size());

                TItem &item = Items[idx];
                Y_VERIFY_S(blobId == item.BlobId, VCtx->VDiskLogPrefix
                    << "itemIdx# " << idx << " blobId# " << blobId.ToString() << " item# " << item.ToString());

                Y_VERIFY_S(!item.Received, VCtx->VDiskLogPrefix
                    << "itemIdx# " << idx << " item# " << item.ToString());

                item.Received = true;
                item.Status = ev->Get()->Status;
                item.ErrorReason = ev->Get()->ErrorReason;
                item.WrittenBeyondBarrier = ev->Get()->WrittenBeyondBarrier;

                ReceivedResults++;

                if (ReceivedResults == Items.size()) {
                    SendResponseAndDie(ctx);
                }
            }

            void Bootstrap() {
                for (ui64 idx = 0; idx < TMultiPutTraits<TEvent>::ItemsSize(*Event->Get()); ++idx) {
                    TItem &item = Items[idx];

                    TLogoBlobID blobId = TMultiPutTraits<TEvent>::GetBlobId(*Event->Get(), idx);
                    item.BlobId = blobId;

                    item.HasCookie = TMultiPutTraits<TEvent>::HasItemCookie(*Event->Get(), idx);
                    if (item.HasCookie) {
                        item.Cookie = TMultiPutTraits<TEvent>::GetItemCookie(*Event->Get(), idx);
                    }

                    if (item.Status != NKikimrProto::OK) {
                        item.Received = true;
                        ReceivedResults++;
                    }
                }

                this->Become(&TThis::StateWait);
            }

            void PassAway() override {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::ActorDied, 0, LeaderId, this->SelfId(), nullptr, 0));
                TBase::PassAway();
            }

            STRICT_STFUNC(StateWait,
                HFunc(TEvVMultiPutItemResult, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
        };

    } // NPrivate

    IActor* CreateSkeletonVMultiPutActor(TActorId leaderId, const TBatchedVec<NKikimrProto::EReplyStatus> &statuses,
            TOutOfSpaceStatus oosStatus, TEvBlobStorage::TEvVMultiPut::TPtr &ev,
            TActorIDPtr skeletonFrontIDPtr, ::NMonitoring::TDynamicCounters::TCounterPtr counterPtr,
            ui64 incarnationGuid, TIntrusivePtr<TVDiskContext>& vCtx) {
        Y_UNUSED(skeletonFrontIDPtr);
        return new NPrivate::TBufferVMultiPutActor<TEvBlobStorage::TEvVMultiPut>(leaderId, statuses, oosStatus, ev,
                counterPtr, incarnationGuid, vCtx);
    }

    IActor* CreateSkeletonVPutFlatMultiActor(TActorId leaderId, const TBatchedVec<NKikimrProto::EReplyStatus> &statuses,
            TOutOfSpaceStatus oosStatus, TEvBlobStorage::TEvVPutFlat::TPtr &ev,
            ::NMonitoring::TDynamicCounters::TCounterPtr counterPtr,
            ui64 incarnationGuid, TIntrusivePtr<TVDiskContext>& vCtx) {
        return new NPrivate::TBufferVMultiPutActor<TEvBlobStorage::TEvVPutFlat>(leaderId, statuses, oosStatus, ev,
                counterPtr, incarnationGuid, vCtx);
    }

} // NKikimr
