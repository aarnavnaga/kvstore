#include <gtest/gtest.h>

#include "common/file.h"
#include "raft/file_storage.h"
#include "server/kv_state_machine.h"
#include "test_util.h"

namespace kv {
namespace {

using raft::Entry;
using raft::FileLogStorage;

std::vector<Entry> Entries(raft::Index from, raft::Index to, raft::Term term) {
  std::vector<Entry> out;
  for (raft::Index i = from; i <= to; ++i) {
    out.push_back(Entry{term, i, raft::EntryType::kNormal, "e" + std::to_string(i)});
  }
  return out;
}

TEST(FileLogStorage, PersistsHardStateAndEntriesAcrossReopen) {
  test::TempDir dir;
  {
    std::unique_ptr<FileLogStorage> s;
    ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
    ASSERT_TRUE(s->SaveHardState({7, 2}).ok());
    ASSERT_TRUE(s->Append(Entries(1, 50, 3)).ok());
  }
  std::unique_ptr<FileLogStorage> s;
  ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
  EXPECT_EQ(s->LoadHardState(), (raft::HardState{7, 2}));
  EXPECT_EQ(s->LastIndex(), 50u);
  EXPECT_EQ(s->TermAt(50), 3u);
  std::vector<Entry> got;
  s->GetEntries(10, 13, SIZE_MAX, &got);
  ASSERT_EQ(got.size(), 3u);
  EXPECT_EQ(got[0].data, "e10");
}

TEST(FileLogStorage, TruncatesConflictingSuffixDurably) {
  test::TempDir dir;
  {
    std::unique_ptr<FileLogStorage> s;
    ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
    ASSERT_TRUE(s->Append(Entries(1, 10, 1)).ok());
    // A new leader's log diverges at 6: entries 6..10 of term 1 must go.
    ASSERT_TRUE(s->Append(Entries(6, 8, 2)).ok());
    EXPECT_EQ(s->LastIndex(), 8u);
  }
  std::unique_ptr<FileLogStorage> s;
  ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
  EXPECT_EQ(s->LastIndex(), 8u);
  EXPECT_EQ(s->TermAt(5), 1u);
  EXPECT_EQ(s->TermAt(6), 2u);
  EXPECT_EQ(s->TermAt(8), 2u);
}

TEST(FileLogStorage, DropsTornTailOnRecovery) {
  test::TempDir dir;
  {
    std::unique_ptr<FileLogStorage> s;
    ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
    ASSERT_TRUE(s->Append(Entries(1, 20, 1)).ok());
  }
  // Chop a few bytes off the end, as a crash mid-append would.
  const std::string path = JoinPath(dir.path(), "raft.log");
  std::string contents;
  ASSERT_TRUE(ReadFileToString(path, &contents).ok());
  ASSERT_TRUE(WriteFileAtomic(path, contents.substr(0, contents.size() - 3)).ok());

  std::unique_ptr<FileLogStorage> s;
  ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
  EXPECT_EQ(s->LastIndex(), 19u);
  // And appends continue cleanly after the cut.
  ASSERT_TRUE(s->Append(Entries(20, 25, 2)).ok());
  s.reset();
  ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
  EXPECT_EQ(s->LastIndex(), 25u);
  EXPECT_EQ(s->TermAt(20), 2u);
}

TEST(FileLogStorage, CompactAndResetToSnapshot) {
  test::TempDir dir;
  std::unique_ptr<FileLogStorage> s;
  ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
  ASSERT_TRUE(s->Append(Entries(1, 100, 1)).ok());
  ASSERT_TRUE(s->Compact({60, 1}).ok());
  EXPECT_EQ(s->FirstIndex(), 61u);
  EXPECT_EQ(s->TermAt(60), 1u);
  s.reset();
  ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
  EXPECT_EQ(s->FirstIndex(), 61u);
  EXPECT_EQ(s->LastIndex(), 100u);

  ASSERT_TRUE(s->ResetToSnapshot({500, 4}).ok());
  s.reset();
  ASSERT_TRUE(FileLogStorage::Open(dir.path(), &s).ok());
  EXPECT_EQ(s->LastIndex(), 500u);
  EXPECT_EQ(s->TermAt(500), 4u);
  ASSERT_TRUE(s->Append(Entries(501, 502, 4)).ok());
}

std::string Cmd(net::MsgType op, uint64_t client, uint64_t seq, const std::string& key,
                const std::string& value = "") {
  return server::EncodeCommand(server::Command{op, client, seq, key, value});
}

TEST(KvStateMachine, AppliesInOrderAndDeduplicatesRetries) {
  test::TempDir dir;
  std::unique_ptr<server::KvStateMachine> sm;
  ASSERT_TRUE(server::KvStateMachine::Open(storage::Options{.sync = false}, dir.path(), &sm).ok());

  std::vector<Entry> entries = {
      {1, 1, raft::EntryType::kNoop, ""},
      {1, 2, raft::EntryType::kNormal, Cmd(net::MsgType::kPut, 9, 1, "k", "v1")},
      {1, 3, raft::EntryType::kNormal, Cmd(net::MsgType::kGet, 0, 0, "k")},
      {1, 4, raft::EntryType::kNormal, Cmd(net::MsgType::kPut, 8, 1, "k", "v2")},
      // Client 9 retries its seq-1 write after client 8 overwrote the key. Applying it
      // again would make "v1" reappear; it must be acknowledged but skipped.
      {1, 5, raft::EntryType::kNormal, Cmd(net::MsgType::kPut, 9, 1, "k", "v1")},
      {1, 6, raft::EntryType::kNormal, Cmd(net::MsgType::kGet, 0, 0, "k")},
  };
  std::vector<server::ApplyResult> results;
  ASSERT_TRUE(sm->Apply(entries, &results).ok());
  EXPECT_EQ(results[2].value, "v1");  // read sees the write before it in the same batch
  EXPECT_TRUE(results[4].status.ok());
  EXPECT_EQ(results[5].value, "v2");
  EXPECT_EQ(sm->applied_index(), 6u);
  const uint64_t hash = sm->apply_hash();

  // Applied index, hash and sessions survive a restart.
  sm.reset();
  ASSERT_TRUE(server::KvStateMachine::Open(storage::Options{.sync = false}, dir.path(), &sm).ok());
  EXPECT_EQ(sm->applied_index(), 6u);
  EXPECT_EQ(sm->apply_hash(), hash);
  ASSERT_TRUE(
      sm->Apply({{2, 7, raft::EntryType::kNormal, Cmd(net::MsgType::kPut, 9, 1, "k", "v1")}},
                &results)
          .ok());
  std::string v;
  ASSERT_TRUE(sm->Get("k", &v).ok());
  EXPECT_EQ(v, "v2") << "duplicate applied after restart";
}

}  // namespace
}  // namespace kv
