#include "query_base.h"

#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>

using namespace NKikimrServices;

namespace NKikimr {

    class TLevelIndexExtremeQueryFlatBase {
    protected:
        struct TQuery {
            TLogoBlobID LogoBlobID;
            ui32 PartId = 0;
            ui64 Shift = 0;
            ui64 Size = 0;
            ui64 CookieVal = 0;
            bool HasCookie = false;
        };

        std::shared_ptr<TQueryCtx> QueryCtx;
        const TActorId ParentId;
        TLogoBlobsSnapshot LogoBlobsSnapshot;
        TBarriersSnapshot BarriersSnapshot;
        TReadBatcherCtxPtr BatcherCtx;
        TEvBlobStorage::TEvVGetFlat::TPtr OrigEv;
        const bool ShowInternals;
        std::unique_ptr<TEvBlobStorage::TEvVGetResultFlat> Result;
        TQueryResultSizeTracker ResultSize;
        const TActorId ReplSchedulerId;
        NWilson::TSpan Span;

        std::unique_ptr<TLogoBlobsSnapshot::TForwardIterator> ForwardIt;
        TVector<TQuery> Queries;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;
        bool BlobInIndex = false;

        TQuery *FetchNextQuery() {
            const ui32 queryNum = Queries.size();
            if (queryNum == OrigEv->Get()->GetExtremeQueriesCount()) {
                return nullptr;
            }

            const auto query = OrigEv->Get()->GetExtremeQuery(queryNum);
            TQuery q;
            q.LogoBlobID = NVDiskFlat::FromRaw(query.BlobId);
            q.PartId = q.LogoBlobID.PartId();
            q.LogoBlobID = TLogoBlobID(q.LogoBlobID, 0);
            q.CookieVal = query.Cookie;
            q.HasCookie = query.Flags.HasCookie();
            q.Shift = query.Shift;
            q.Size = query.Size;
            Queries.push_back(q);
            ForwardIt->Seek(q.LogoBlobID);
            BlobInIndex = ForwardIt->Valid() && ForwardIt->GetCurKey().LogoBlobID() == q.LogoBlobID;

            return &Queries.back();
        }

        void Prepare() {
            Y_ABORT_UNLESS(OrigEv->Get()->GetExtremeQueriesCount() > 0);
            ForwardIt = std::make_unique<TLogoBlobsSnapshot::TForwardIterator>(QueryCtx->HullCtx, &LogoBlobsSnapshot);
            Queries.reserve(OrigEv->Get()->GetExtremeQueriesCount());
            BarriersEssence = BarriersSnapshot.CreateEssence(QueryCtx->HullCtx);
            BarriersSnapshot.Destroy();
        }

        ui8 PDiskPriority() const {
            switch (OrigEv->Get()->GetHandleClass()) {
                case NKikimrBlobStorage::EGetHandleClass::AsyncRead:
                    return NPriRead::HullOnlineOther;
                case NKikimrBlobStorage::EGetHandleClass::FastRead:
                case NKikimrBlobStorage::EGetHandleClass::Discover:
                    return NPriRead::HullOnlineRt;
                case NKikimrBlobStorage::EGetHandleClass::LowRead:
                    return NPriRead::HullLow;
                default:
                    Y_ABORT("Unexpected case");
            }
        }

        template<typename TMerger>
        bool IsBlobDeleted(const TLogoBlobID &id, const TMerger &merger) {
            const auto &status = BarriersEssence->Keep(id, merger.GetMemRec(), {}, QueryCtx->HullCtx->AllowKeepFlags,
                true /*allowGarbageCollection*/);
            return !status.KeepData;
        }

        template <class T>
        void SendResponseAndDie(const TActorContext &ctx, T *self) {
            bool hasNotYet = false;

            if (ResultSize.IsOverflow()) {
                auto msg = VDISKP(QueryCtx->HullCtx->VCtx->VDiskLogPrefix,
                            "TEvVGetResultFlat: Result message is too large; size# %" PRIu64 " orig# %s;"
                            " VDISK CAN NOT REPLY ON TEvVGetFlat REQUEST",
                            ResultSize.GetSize(), OrigEv->Get()->ToString().data());
                Result->MakeError(NKikimrProto::ERROR, msg, *OrigEv->Get());
                LOG_CRIT(ctx, NKikimrServices::BS_VDISK_GET, msg);
                Span.EndError(std::move(msg));
            } else {
                ui64 total = 0;
                for (ui32 i = 0; i < Result->GetItemsCount(); ++i) {
                    const auto item = Result->GetItem(i);
                    total += Result->GetBlobSize(item);
                    hasNotYet = hasNotYet || static_cast<NKikimrProto::EReplyStatus>(item.Status) == NKikimrProto::NOT_YET;
                }
                QueryCtx->MonGroup.GetTotalBytes() += total;

                LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_GET,
                        VDISKP(QueryCtx->HullCtx->VCtx->VDiskLogPrefix, "TEvVGetResultFlat: %s", Result->ToString().data()));
                Span.EndOk();
            }

            if (hasNotYet && ReplSchedulerId) {
                LOG_DEBUG(ctx, NKikimrServices::BS_VDISK_GET,
                        VDISKP(QueryCtx->HullCtx->VCtx->VDiskLogPrefix,
                            "TEvVGetFlat NOT_YET enrichment is not implemented"));
            }
            SendVDiskResponse(ctx, OrigEv->Sender, Result.release(), OrigEv->Cookie, QueryCtx->HullCtx->VCtx,
                OrigEv->Get()->GetHandleClass());

            ctx.Send(ParentId, new TEvents::TEvGone);
            self->Die(ctx);
        }

        TLevelIndexExtremeQueryFlatBase(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGetFlat::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResultFlat> result,
                TActorId replSchedulerId,
                const char* name)
            : QueryCtx(queryCtx)
            , ParentId(parentId)
            , LogoBlobsSnapshot(std::move(logoBlobsSnapshot))
            , BarriersSnapshot(std::move(barrierSnapshot))
            , BatcherCtx(new TReadBatcherCtx(QueryCtx->HullCtx->VCtx, QueryCtx->PDiskCtx, "TEvVGetFlat",
                    QueryCtx->ReplMonGroup))
            , OrigEv(ev)
            , ShowInternals(OrigEv->Get()->GetFlags().GetShowInternals())
            , Result(std::move(result))
            , ReplSchedulerId(replSchedulerId)
            , Span(TWilson::VDiskTopLevel, std::move(OrigEv->TraceId), name)
        {
            Y_DEBUG_ABORT_UNLESS(Result);
        }
    };

    class TLevelIndexExtremeQueryFlatIndexOnly
        : public TLevelIndexExtremeQueryFlatBase
        , public TActorBootstrapped<TLevelIndexExtremeQueryFlatIndexOnly>
    {
        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob>;
        friend class TLevelIndexExtremeQueryFlatBase;
        friend class TActorBootstrapped<TLevelIndexExtremeQueryFlatIndexOnly>;
        TIndexRecordMerger Merger;

        void Bootstrap(const TActorContext &ctx) {
            Prepare();
            MainCycle(ctx);
        }

        void MainCycle(const TActorContext &ctx) {
            TQuery *query = nullptr;
            while ((query = FetchNextQuery()) && !ResultSize.IsOverflow()) {
                Y_VERIFY_S(query->PartId == 0, QueryCtx->HullCtx->VCtx->VDiskLogPrefix);
                const ui64 *cookiePtr = query->HasCookie ? &query->CookieVal : nullptr;
                ResultSize.AddLogoBlobIndex();
                if (!BlobInIndex) {
                    Result->AddResult(NKikimrProto::NODATA, query->LogoBlobID, cookiePtr);
                } else {
                    ForwardIt->PutToMerger(&Merger);
                    Merger.Finish();
                    TIngress ingress = Merger.GetMemRec().GetIngress();

                    NMatrix::TVectorType mustHave = ingress.PartsWeMustHaveLocally(QueryCtx->HullCtx->VCtx->Top.get(),
                            QueryCtx->HullCtx->VCtx->ShortSelfVDisk, query->LogoBlobID);
                    NMatrix::TVectorType actuallyHave = ingress.LocalParts(QueryCtx->HullCtx->VCtx->Top->GType);
                    NMatrix::TVectorType missingParts = mustHave - actuallyHave;

                    auto status = mustHave.Empty() ? NKikimrProto::NODATA :
                        IsBlobDeleted(query->LogoBlobID, Merger) ? NKikimrProto::NODATA :
                        missingParts.Empty() ? NKikimrProto::OK : NKikimrProto::NOT_YET;

                    const ui64 ingressRaw = ingress.Raw();
                    const ui64 *pingr = ShowInternals ? &ingressRaw : nullptr;

                    const int mode = ingress.GetCollectMode(TIngress::IngressMode(QueryCtx->HullCtx->VCtx->Top->GType));
                    const bool keep = (mode & CollectModeKeep) && !(mode & CollectModeDoNotKeep);
                    const bool doNotKeep = mode & CollectModeDoNotKeep;
                    const NMatrix::TVectorType local = ingress.LocalParts(QueryCtx->HullCtx->VCtx->Top->GType);

                    Result->AddResult(status, query->LogoBlobID, cookiePtr, pingr, &local, keep, doNotKeep);
                    Merger.Clear();
                }
            }

            SendResponseAndDie(ctx, this);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULLQUERY_EXTREME_INDEX_ONLY;
        }

        TLevelIndexExtremeQueryFlatIndexOnly(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGetFlat::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResultFlat> result,
                TActorId replSchedulerId)
            : TLevelIndexExtremeQueryFlatBase(queryCtx, parentId, std::move(logoBlobsSnapshot),
                    std::move(barrierSnapshot), ev, std::move(result), replSchedulerId,
                    "VDisk.LevelIndexExtremeQueryFlatIndexOnly")
            , TActorBootstrapped<TLevelIndexExtremeQueryFlatIndexOnly>()
            , Merger(QueryCtx->HullCtx->VCtx->Top->GType)
        {}
    };

    class TLevelIndexExtremeQueryFlatData
        : public TLevelIndexExtremeQueryFlatBase
        , public TActorBootstrapped<TLevelIndexExtremeQueryFlatData>
    {
        using TRecordMergerCallback = ::NKikimr::TRecordMergerCallback<TKeyLogoBlob, TMemRecLogoBlob, TReadBatcher>;
        friend class TLevelIndexExtremeQueryFlatBase;
        friend class TActorBootstrapped<TLevelIndexExtremeQueryFlatData>;

        const TBlobStorageGroupType GType;
        TReadBatcher Batcher;
        TRecordMergerCallback Merger;
        TActiveActors ActiveActors;

        void Bootstrap(const TActorContext &ctx) {
            Prepare();
            MainCycle(ctx);
        }

        void Finish(const TActorContext &ctx) {
            TReadBatcherResult::TIterator rit(&Batcher.GetResult());
            for (rit.SeekToFirst(); rit.Valid(); rit.Next()) {
                const NReadBatcher::TDataItem *it = rit.Get();
                const TQuery *query = static_cast<const TQuery*>(it->Cookie);
                const ui64 *cookiePtr = query->HasCookie ? &query->CookieVal : nullptr;

                ui64 ingr = it->Ingress.Raw();
                ui64 *pingr = (ShowInternals ? &ingr : nullptr);

                const int mode = it->Ingress.GetCollectMode(TIngress::IngressMode(QueryCtx->HullCtx->VCtx->Top->GType));
                const bool keep = (mode & CollectModeKeep) && !(mode & CollectModeDoNotKeep);
                const bool doNotKeep = mode & CollectModeDoNotKeep;

                switch (it->GetType()) {
                    case NReadBatcher::TDataItem::ET_CLEAN:
                        Y_ABORT_S(QueryCtx->HullCtx->VCtx->VDiskLogPrefix << "Impossible case");
                    case NReadBatcher::TDataItem::ET_NODATA:
                        Result->AddResult(NKikimrProto::NODATA, it->Id, cookiePtr, pingr);
                        break;
                    case NReadBatcher::TDataItem::ET_ERROR:
                        Result->AddResult(NKikimrProto::ERROR, it->Id, cookiePtr, pingr);
                        break;
                    case NReadBatcher::TDataItem::ET_NOT_YET:
                        Result->AddResult(NKikimrProto::NOT_YET, it->Id, query->Shift, static_cast<ui32>(query->Size),
                            cookiePtr, pingr, keep, doNotKeep);
                        break;
                    case NReadBatcher::TDataItem::ET_SETDISK:
                    case NReadBatcher::TDataItem::ET_SETMEM:
                    {
                        struct TProcessor {
                            std::unique_ptr<TEvBlobStorage::TEvVGetResultFlat>& Result;
                            TLogoBlobID Id;
                            ui64 Shift;
                            ui64 Size;
                            const ui64 *CookiePtr;
                            const ui64 *IngrPtr;
                            const bool Keep;
                            const bool DoNotKeep;
                            void operator()(NReadBatcher::TReadError) const {
                                Result->AddResult(NKikimrProto::CORRUPTED, Id, Shift, static_cast<ui32>(Size), CookiePtr,
                                    IngrPtr, Keep, DoNotKeep);
                            }
                            void operator()(TRcBuf&& buffer) const {
                                Result->AddResult(NKikimrProto::OK, Id, Shift, TRope(std::move(buffer)), CookiePtr,
                                    IngrPtr, Keep, DoNotKeep);
                            }
                            void operator()(const TRope& data) const {
                                Result->AddResult(NKikimrProto::OK, Id, Shift, TRope(data), CookiePtr,
                                    IngrPtr, Keep, DoNotKeep);
                            }
                        } processor{Result, it->Id, query->Shift, query->Size, cookiePtr, pingr, keep, doNotKeep};
                        rit.GetData(processor);
                        break;
                    }
                }
            }

            QueryCtx->PDiskReadBytes += Batcher.GetPDiskReadBytes();
            SendResponseAndDie(ctx, this);
        }

        void HandleReadCompletion(TEvents::TEvCompleted::TPtr& ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            Finish(ctx);
        }

        void MainCycle(const TActorContext &ctx) {
            TQuery *query = nullptr;
            while ((query = FetchNextQuery()) && !ResultSize.IsOverflow()) {
                const TLogoBlobID &fullId = query->LogoBlobID;
                const TLogoBlobID partId = TLogoBlobID(fullId, query->PartId);
                bool found = false;
                TMaybe<TIngress> ingress;

                ResultSize.AddLogoBlobIndex();
                if (BlobInIndex) {
                    ResultSize.AddLogoBlobData(GType.PartSize(partId), query->Shift, query->Size);
                    Batcher.StartTraverse(fullId, query, query->PartId, query->Shift, query->Size);
                    ForwardIt->PutToMerger(&Merger);
                    Merger.Finish();
                    ingress = Merger.GetMemRec().GetIngress();
                    if (IsBlobDeleted(fullId, Merger)) {
                        Batcher.AbortTraverse();
                    } else {
                        Batcher.FinishTraverse(*ingress);
                        found = true;
                    }
                    Merger.Clear();
                }

                if (!found) {
                    Batcher.PutNoData(partId, ingress, query);
                }
            }

            if (ResultSize.IsOverflow()) {
                SendResponseAndDie(ctx, this);
            } else {
                std::unique_ptr<IActor> a(Batcher.CreateAsyncDataReader(ctx.SelfID, PDiskPriority(), Span.GetTraceId(), false));
                if (a) {
                    auto aid = ctx.Register(a.release());
                    ActiveActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                } else {
                    Finish(ctx);
                }
                Become(&TThis::StateFunc);
            }

            BarriersEssence.Reset();
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Span.EndError("TEvPoisonPill");
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvCompleted, HandleReadCompletion)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULLQUERY_EXTREME_DATA;
        }

        TLevelIndexExtremeQueryFlatData(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGetFlat::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResultFlat> result,
                TActorId replSchedulerId)
            : TLevelIndexExtremeQueryFlatBase(queryCtx, parentId, std::move(logoBlobsSnapshot),
                    std::move(barrierSnapshot), ev, std::move(result), replSchedulerId,
                    "VDisk.LevelIndexExtremeQueryFlatData")
            , TActorBootstrapped<TLevelIndexExtremeQueryFlatData>()
            , GType(QueryCtx->HullCtx->VCtx->Top->GType)
            , Batcher(BatcherCtx)
            , Merger(&Batcher, GType)
        {}
    };

    IActor *CreateLevelIndexQueryActor(
                    std::shared_ptr<TQueryCtx> &queryCtx,
                    TReadQueryKeepChecker &&keepChecker,
                    const TActorContext &ctx,
                    THullDsSnap &&fullSnap,
                    const TActorId &parentId,
                    TEvBlobStorage::TEvVGetFlat::TPtr &ev,
                    std::unique_ptr<TEvBlobStorage::TEvVGetResultFlat> result,
                    TActorId replSchedulerId) {
        if (queryCtx->HullCtx->BarrierValidation) {
            TLogoBlobsSnapshot::TIndexForwardIterator it(fullSnap.HullCtx, &fullSnap.LogoBlobsSnap);
            for (ui32 i = 0; i < ev->Get()->GetExtremeQueriesCount(); ++i) {
                const auto item = ev->Get()->GetExtremeQuery(i);
                const TLogoBlobID id = NVDiskFlat::FromRaw(item.BlobId);
                const TLogoBlobID full = id.FullID();

                it.Seek(full);
                if (it.Valid() && it.GetCurKey() == full) {
                    const TIngress& ingress = it.GetMemRec().GetIngress();
                    const bool keep = ingress.KeepUnconditionally(TIngress::IngressMode(queryCtx->HullCtx->VCtx->Top->GType));
                    TString explanation;
                    if (!ev->Get()->GetFlags().GetSuppressBarrierCheck() && !keepChecker(full, keep, &explanation)) {
                        LOG_INFO(ctx, NKikimrServices::BS_HULLRECS,
                                VDISKP(queryCtx->HullCtx->VCtx->VDiskLogPrefix,
                                    "Db# LogoBlobs getting blob beyond the barrier id# %s ingress# %s barrier# %s",
                                    id.ToString().data(), ingress.ToString(queryCtx->HullCtx->VCtx->Top.get(),
                                    queryCtx->HullCtx->VCtx->ShortSelfVDisk, id).data(), explanation.data()));
                    }
                }
            }
        }

        if (ev->Get()->IsExtremeIndexQuery()) {
            return new TLevelIndexExtremeQueryFlatIndexOnly(queryCtx, parentId,
                    std::move(fullSnap.LogoBlobsSnap), std::move(fullSnap.BarriersSnap), ev, std::move(result), replSchedulerId);
        } else if (ev->Get()->IsExtremeDataQuery()) {
            return new TLevelIndexExtremeQueryFlatData(queryCtx, parentId,
                    std::move(fullSnap.LogoBlobsSnap), std::move(fullSnap.BarriersSnap), ev, std::move(result), replSchedulerId);
        } else {
            return nullptr;
        }
    }

} // NKikimr
