#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>

#include "common.h"
#include "test_utils.h"

using namespace test_util;

namespace fs = std::filesystem;

TEST_CASE("scan nonexistent partition", "[scan]")
{
    fs::path dir = fs::temp_directory_path() / fs::path("standby-test");
    fs::remove_all(dir);
    fs::create_directories(dir);

    eloqstore::KvOptions opts;
    opts.store_path = {dir.string()};

    eloqstore::TableIdent tbl_id{"empty-scan", 0};

    eloqstore::EloqStore store(opts);
    REQUIRE(store.Start() == eloqstore::KvError::NoError);
    eloqstore::ScanRequest scan_req;
    std::string start_key = "0";
    std::string end_key = "1";
    scan_req.SetArgs(tbl_id, start_key, end_key);

    store.ExecSync(&scan_req);
    REQUIRE(scan_req.Error() == eloqstore::KvError::NoError);

    store.Stop();

    fs::remove_all(dir);
}

TEST_CASE("delete scan", "[scan]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.SetValueSize(400);
    verify.WriteRnd(1, 100);
    verify.WriteRnd(1, 100);
    verify.Delete(50, 70);
    verify.Scan(100, 200);
    verify.Delete(0, 1000);
    verify.Upsert(100, 200);
}

TEST_CASE("complex scan", "[scan]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    verify.Upsert(1, 1000);
    verify.Upsert(2000, 3000);
    verify.Upsert(800, 1200);
    verify.Delete(2200, 2300);
    verify.Read(1);
    verify.Read(900);
    verify.Read(1100);
    verify.Read(5000);
    verify.Scan(1, 1000);
    verify.Scan(1000, 4000);
    verify.Scan(0, 100);
    verify.Delete(0, 200);
    verify.Delete(100, 300);
    verify.Scan(0, 100);
    verify.Scan(0, 500);
}

TEST_CASE("random write and scan", "[scan]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store);
    constexpr uint64_t max_val = 1000;
    for (int i = 0; i < 10; i++)
    {
        verify.WriteRnd(1, max_val, 0, 20);
        for (int j = 0; j < 5; j++)
        {
            uint64_t start = std::rand() % max_val;
            verify.Scan(start, start + 100);
        }
    }
}

TEST_CASE("paginate the scan results", "[scan]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(100);
    verify.WriteRnd(0, 10000, 0, 90);
    // Paginate by entries amount.
    verify.Scan(0, 10000, 1000);
    // Paginate by result size.
    verify.Scan(0, 10000, SIZE_MAX, 10000);
    // Paginate by entries amount and size.
    verify.Scan(0, 10000, 100, 5000);

    // Paginate with overflow value.
    verify.SetValueSize(10000);
    verify.Upsert(1, 3);
    verify.Scan(0, 10, SIZE_MAX, 1000);
}

TEST_CASE("read floor", "[read]")
{
    eloqstore::EloqStore *store = InitStore(mem_store_opts);
    MapVerifier verify(test_tbl_id, store, false);
    verify.SetValueSize(1000);
    verify.Upsert(2, 12);
    verify.WriteRnd(20, 50, 0, 30);

    // overflow value
    verify.SetValueSize(10000);
    verify.Upsert(15);

    for (int i = 51; i >= 0; i--)
    {
        verify.Floor(Key(i));
    }

    verify.WriteRnd(5, 50);
    for (int i = 51; i >= 0; i--)
    {
        verify.Floor(Key(i));
    }
}
