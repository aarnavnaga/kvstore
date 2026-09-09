// Crash test: run crash_writer, SIGKILL it at a random point, reopen the store and check
// that every write it acknowledged is there. Repeats for many rounds against the same
// directory, so later rounds also recover from the debris of earlier ones.

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <thread>

#include "storage/db.h"
#include "test_util.h"

namespace kv::storage {
namespace {

// RunAndKill starts crash_writer at first_op, kills it after delay, and returns the last
// operation it acknowledged (or nullopt if it acknowledged none).
std::optional<long> RunAndKill(const std::string& dir, long first_op,
                               std::chrono::milliseconds delay) {
  int fds[2];
  if (::pipe(fds) != 0) {
    ADD_FAILURE() << "pipe failed";
    return std::nullopt;
  }
  const pid_t pid = ::fork();
  if (pid == 0) {
    ::dup2(fds[1], STDOUT_FILENO);
    ::close(fds[0]);
    ::close(fds[1]);
    const std::string first = std::to_string(first_op);
    ::execl(KV_CRASH_WRITER, KV_CRASH_WRITER, dir.c_str(), first.c_str(),
            static_cast<char*>(nullptr));
    ::_exit(127);
  }
  ::close(fds[1]);
  std::this_thread::sleep_for(delay);
  ::kill(pid, SIGKILL);

  std::string out;
  char buf[4096];
  ssize_t n = 0;
  while ((n = ::read(fds[0], buf, sizeof(buf))) > 0) {
    out.append(buf, static_cast<size_t>(n));
  }
  ::close(fds[0]);
  int status = 0;
  ::waitpid(pid, &status, 0);
  EXPECT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL)
      << "writer exited on its own; status " << status;

  // Only complete lines count: a line cut off by the kill was never fully acknowledged.
  const size_t end = out.rfind('\n');
  if (end == std::string::npos) {
    return std::nullopt;
  }
  const size_t start = out.rfind('\n', end - 1);
  return std::stol(out.substr(start == std::string::npos ? 0 : start + 1, end));
}

// Apply op i of crash_writer's workload to the model.
void ApplyOp(long i, std::map<std::string, std::string>* model) {
  if (i % 7 == 0 && i >= 3) {
    model->erase("key" + std::to_string(i - 3));
  } else {
    (*model)["key" + std::to_string(i)] = "value" + std::to_string(i);
  }
}

std::string KeyTouchedBy(long i) {
  return "key" + std::to_string(i % 7 == 0 && i >= 3 ? i - 3 : i);
}

TEST(Crash, KillNineRecoversEveryAcknowledgedWrite) {
  const uint64_t seed = test::TestSeed();
  const int64_t rounds = test::EnvInt("KV_CRASH_ROUNDS", 25);
  SCOPED_TRACE("KV_SEED=" + std::to_string(seed));
  std::mt19937_64 rng(seed);

  test::TempDir dir;
  std::map<std::string, std::string> model;
  long next_op = 0;
  long total_acked = 0;

  for (int64_t round = 0; round < rounds; ++round) {
    const auto delay = std::chrono::milliseconds(50 + rng() % 400);
    const std::optional<long> last = RunAndKill(dir.path(), next_op, delay);
    if (last) {
      for (long i = next_op; i <= *last; ++i) {
        ApplyOp(i, &model);
      }
      total_acked += *last - next_op + 1;
      next_op = *last + 1;
    }

    // The one operation in flight at the kill may or may not have landed; every key it
    // could have touched is exempt from this round's check. The next round re-runs it.
    const std::string in_flight = KeyTouchedBy(next_op);

    std::unique_ptr<DB> db;
    const Status open = DB::Open(Options{}, dir.path(), &db);
    ASSERT_TRUE(open.ok()) << "round " << round << ": " << open.ToString();
    for (const auto& [key, value] : model) {
      if (key == in_flight) {
        continue;
      }
      std::string got;
      const Status s = db->Get(key, &got);
      ASSERT_TRUE(s.ok()) << "round " << round << ": acknowledged " << key << " lost";
      ASSERT_EQ(got, value) << "round " << round;
    }
    // Acknowledged deletes must stay deleted.
    for (long i = 0; i < next_op; ++i) {
      const std::string key = "key" + std::to_string(i);
      if (model.count(key) == 0 && key != in_flight) {
        std::string got;
        ASSERT_TRUE(db->Get(key, &got).IsNotFound())
            << "round " << round << ": deleted " << key << " came back";
      }
    }
  }
  std::printf("Crash: %lld rounds, %ld acknowledged ops verified\n", static_cast<long long>(rounds),
              total_acked);
  EXPECT_GT(total_acked, 0);
}

}  // namespace
}  // namespace kv::storage
