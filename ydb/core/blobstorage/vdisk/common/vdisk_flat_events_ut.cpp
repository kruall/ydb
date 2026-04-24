#include "vdisk_flat_events.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TVDiskFlatEventsTest) {

        TVDiskID VDiskId() {
            return TVDiskID(1, 2, 3, 4, 5);
        }

        TLogoBlobID BlobId(ui32 cookie = 1, ui32 size = 3) {
            return TLogoBlobID(42, 1, 2, 0, size, cookie);
        }

        Y_UNIT_TEST(VPutSingle) {
            ui64 cookie = 17;
            THolder<TEvBlobStorage::TEvVPutFlat> ev(TEvBlobStorage::TEvVPutFlat::MakeSinglePut(
                BlobId(), TRope(TString("abc")), VDiskId(), false, &cookie, TInstant::Max(),
                NKikimrBlobStorage::TabletLog, true));

            TString error;
            UNIT_ASSERT(ev->Validate(error));
            UNIT_ASSERT(ev->IsSinglePut());
            UNIT_ASSERT_VALUES_EQUAL(ev->GetVDiskID(), VDiskId());
            UNIT_ASSERT_VALUES_EQUAL(ev->GetBufferBytes(), 3);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetBuffer().ConvertToString(), "abc");
        }

        Y_UNIT_TEST(VPutMulti) {
            THolder<TEvBlobStorage::TEvVPutFlat> ev(TEvBlobStorage::TEvVPutFlat::MakeMultiPut(
                VDiskId(), TInstant::Max(), NKikimrBlobStorage::TabletLog, false));

            ui64 cookie = 18;
            ev->AddVPut(BlobId(1, 3), TRcBuf(TString("abc")), &cookie, true, false, false, nullptr, {}, true);
            ev->AddVPut(BlobId(2, 4), TRcBuf(TString("defg")), nullptr, false, false, false, nullptr, {}, true);

            TString error;
            UNIT_ASSERT(ev->Validate(error));
            UNIT_ASSERT(ev->IsMultiPut());
            UNIT_ASSERT_VALUES_EQUAL(ev->GetBufferBytes(), 7);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetBufferBytes(0), 3);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetBufferBytes(1), 4);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetItemBuffer(0).ConvertToString(), "abc");
            UNIT_ASSERT_VALUES_EQUAL(ev->GetItemBuffer(1).ConvertToString(), "defg");
        }

        Y_UNIT_TEST(VGetExtremeAndRange) {
            ui64 cookie = 19;
            auto extreme = TEvBlobStorage::TEvVGetFlat::CreateExtremeDataQuery(
                VDiskId(), TInstant::Max(), NKikimrBlobStorage::FastRead,
                TEvBlobStorage::TEvVGet::EFlags::None, {}, {{BlobId(), 1, 2, &cookie}});

            TString error;
            UNIT_ASSERT(extreme->Validate(error));
            UNIT_ASSERT(extreme->IsExtremeDataQuery());

            auto range = TEvBlobStorage::TEvVGetFlat::CreateRangeIndexQuery(
                VDiskId(), TInstant::Max(), NKikimrBlobStorage::AsyncRead,
                TEvBlobStorage::TEvVGet::EFlags::None, {}, BlobId(1), BlobId(2), 10, &cookie);

            UNIT_ASSERT(range->Validate(error));
            UNIT_ASSERT(range->IsRangeIndexQuery());
        }

        Y_UNIT_TEST(VGetResultPayload) {
            THolder<TEvBlobStorage::TEvVGetResultFlat> ev(TEvBlobStorage::TEvVGetResultFlat::Make(
                NKikimrProto::OK, VDiskId(), {}, 123));

            ui64 cookie = 20;
            ev->AddResult(NKikimrProto::OK, BlobId(), 0, TRope(TString("payload")), &cookie);

            auto items = ev->Array<TEvBlobStorage::TEvVGetResultFlat::TItemsTag>();
            UNIT_ASSERT_VALUES_EQUAL(items.size(), 1);
            const auto item = items.Get(0);
            UNIT_ASSERT(ev->HasBlob(item));
            UNIT_ASSERT_VALUES_EQUAL(ev->GetBlobSize(item), 7);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetBlobData(item).ConvertToString(), "payload");
        }

    } // Y_UNIT_TEST_SUITE

} // namespace NKikimr
