#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <fstream>
#include <iostream>

#include "Arena.h"
#include "FDBLoanerTypes.h"
#include "Knobs.h"
#include "Platform.h"
#include "Trace.h"
#include "crc32c.h"
#include "network.h"
#include "fdbrpc/crc32c.h"
#include "bindings/flow/fdb_flow.h"

#include "flow/actorcompiler.h" // This must be the last include

THREAD_FUNC networkThread(void* api) {
	// This is the fdb_flow network we're running on a thread
	((FDB::API*)api)->runNetwork();
	THREAD_RETURN;
}

template <class T>
struct Counter {
	explicit Counter(const char* name) : name(name), value(0), timeLastLogged(g_network->now()) {}
	void log(TraceEvent& e) {
		if (g_network->now() > timeLastLogged) {
			e.detail((name + std::string("Hz")).c_str(),
			         static_cast<double>(value - valueLastLogged) / (g_network->now() - timeLastLogged));
		}
		e.detail(name, value);
		timeLastLogged = g_network->now();
		valueLastLogged = value;
	}
	void add(T v) { value += v; }

	T getValue() const { return value; }
	T getValueLastLogged() const { return valueLastLogged; }
	double getTimeLastLogged() const { return timeLastLogged; }

private:
	const char* name;
	T value = 0;
	T valueLastLogged = 0;
	double timeLastLogged = 0;
};

Counter<int64_t> bytesCompared("BytesCompared");
Counter<int64_t> futureVersions("FutureVersions");
Counter<int64_t> transactionsTooOld("TransactionsTooOld");
Counter<int64_t> transactionRetries("TransactionsRetries");
Counter<int64_t> successfulReads("SuccessfulReads");

int64_t timeout_ms = 60000;
int64_t bytes_per_read = 2000000;
int64_t log_interval_ms = 1000;

ACTOR static Future<Void> readKeyRange(Reference<FDB::Database> db, FDB::Key begin, FDB::Key end,
                                       PromiseStream<Optional<FDB::KeyValue>> outKvs, int64_t* queueSize,
                                       bool accessSystemKeys = false) {
	state FDB::GetRangeLimits limit = FDB::GetRangeLimits(FDB::GetRangeLimits::ROW_LIMIT_UNLIMITED, bytes_per_read);
	state Future<Void> everySecond = delay(1);
	state Reference<FDB::Transaction> tr = db->createTransaction();
	tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
	tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_LOCK_AWARE);
	if (accessSystemKeys) {
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_ACCESS_SYSTEM_KEYS);
	}
	state Future<FDB::FDBStandalone<FDB::RangeResultRef>> readFuture =
	    tr->getRange(FDB::KeyRangeRef(begin, end), limit, /*snapshot*/ true);
	loop {
		try {
			state FDB::FDBStandalone<FDB::RangeResultRef> kvs = wait(readFuture);
			successfulReads.add(1);
			if (everySecond.isReady()) {
				tr->reset();
				tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
				tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_LOCK_AWARE);
				if (accessSystemKeys) {
					tr->setOption(FDBTransactionOption::FDB_TR_OPTION_ACCESS_SYSTEM_KEYS);
				}
				everySecond = delay(1);
			}
			if (kvs.more) {
				ASSERT(kvs.size() > 0);
				begin = FDB::keyAfter(kvs.back().key);
				readFuture = tr->getRange(FDB::KeyRangeRef(begin, end), limit, /*snapshot*/ true);
			}
			while (*queueSize > 1e9) {
				TraceEvent("QueueSizeTooLarge").detail("QueueSize", *queueSize);
				wait(delay(1));
			}
			for (const auto& kv : kvs) {
				outKvs.send(Optional<FDB::KeyValue>(FDB::KeyValue(kv)));
				*queueSize += kv.expectedSize();
			}
			if (!kvs.more) {
				outKvs.send(Optional<FDB::KeyValue>());
				return Never();
			}
		} catch (Error& e) {
			TraceEvent("ReadKeyRangeError").error(e);
			if (e.code() == error_code_transaction_too_old) transactionsTooOld.add(1);
			if (e.code() == error_code_future_version) futureVersions.add(1);
			wait(tr->onError(e));
			transactionRetries.add(1);
			tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
			tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_LOCK_AWARE);
			if (accessSystemKeys) {
				tr->setOption(FDBTransactionOption::FDB_TR_OPTION_ACCESS_SYSTEM_KEYS);
			}
			readFuture = tr->getRange(FDB::KeyRangeRef(begin, end), limit, /*snapshot*/ true);
		}
	}
}

ACTOR static Future<Void> asyncCompare(std::string clusterFile1, std::string clusterFile2,
                                       FutureStream<Optional<FDB::KeyValue>> kvs1,
                                       FutureStream<Optional<FDB::KeyValue>> kvs2, bool* compareSuccess,
                                       int64_t* queueSize1, int64_t* queueSize2) {
	loop {
		state Optional<FDB::KeyValue> kv1 = waitNext(kvs1);
		*queueSize1 -= kv1.present() ? kv1.get().expectedSize() : 0;
		state Optional<FDB::KeyValue> kv2 = waitNext(kvs2);
		*queueSize2 -= kv2.present() ? kv2.get().expectedSize() : 0;
		if (kv1 != kv2) {
			*compareSuccess = false;
			if (kv1.present()) {
				TraceEvent("ClustersNotEqual")
				    .detail("ClusterFile1", clusterFile1.c_str())
				    .detail("Key1", kv1.get().key.printable().c_str())
				    .detail("Value1", kv1.get().value.printable().c_str());
			}
			if (kv2.present()) {
				TraceEvent("ClustersNotEqual")
				    .detail("ClusterFile2", clusterFile2.c_str())
				    .detail("Key2", kv2.get().key.printable().c_str())
				    .detail("Value2", kv2.get().value.printable().c_str());
			}
			if (kv1.present() && (!kv2.present() || kv2.get().key >= kv1.get().key)) {
				printf("%s\t%s: %s\n", clusterFile1.c_str(), kv1.get().key.printable().c_str(),
				       kv1.get().value.printable().c_str());
			} else {
				printf("%s\tnot found\n", clusterFile1.c_str());
			}
			if (kv2.present() && (!kv1.present() || kv1.get().key >= kv2.get().key)) {
				printf("%s\t%s: %s\n", clusterFile2.c_str(), kv2.get().key.printable().c_str(),
				       kv2.get().value.printable().c_str());
			} else {
				printf("%s\tnot found\n", clusterFile2.c_str());
			}
			return Void();
		}
		if (!kv1.present() && !kv2.present()) {
			return Void();
		}
		bytesCompared.add(kv1.get().key.size() + kv1.get().value.size());
	}
}

void set_timeout(FDB::Database* db) {
	auto arg = makeString(8);
	memcpy(mutateString(arg), &timeout_ms, 8);
	db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_TIMEOUT, arg);
}

ACTOR static Future<bool> compareKeyRange(FDB::API* fdb, std::string clusterFile1, std::string clusterFile2,
                                          FDB::Key begin, FDB::Key end) {
	state bool compareSuccess = true;
	state PromiseStream<Optional<FDB::KeyValue>> kvs1;
	state int64_t queueSize1 = 0;
	state PromiseStream<Optional<FDB::KeyValue>> kvs2;
	state int64_t queueSize2 = 0;
	state Reference<FDB::Database> db1 = fdb->createDatabase(clusterFile1);
	set_timeout(db1.getPtr());
	state Reference<FDB::Database> db2 = fdb->createDatabase(clusterFile2);
	set_timeout(db2.getPtr());
	choose {
		when(wait(readKeyRange(db1, begin, end, kvs1, &queueSize1))) { ASSERT(false); }
		when(wait(readKeyRange(db2, begin, end, kvs2, &queueSize2))) { ASSERT(false); }
		when(wait(recurring(
		    []() {
			    TraceEvent te("CompareKeyRangeMetrics");
			    if (g_network->now() - bytesCompared.getTimeLastLogged() > 0) {
				    printf("Bytes/s: %f\n", double(bytesCompared.getValue() - bytesCompared.getValueLastLogged()) /
				                                (g_network->now() - bytesCompared.getTimeLastLogged()));
			    }
			    bytesCompared.log(te);
			    futureVersions.log(te);
			    transactionsTooOld.log(te);
			    transactionRetries.log(te);
			    successfulReads.log(te);
		    },
		    1e-3 * log_interval_ms))) {
			ASSERT(false);
		}
		when(wait(asyncCompare(clusterFile1, clusterFile2, kvs1.getFuture(), kvs2.getFuture(), &compareSuccess,
		                       &queueSize1, &queueSize2))) {}
	}
	TraceEvent("CompareKeyRange")
	    .detail("ClusterFile1", clusterFile1.c_str())
	    .detail("ClusterFile2", clusterFile2.c_str())
	    .detail("Begin", begin.printable().c_str())
	    .detail("End", end.printable().c_str())
	    .detail("Result", compareSuccess);
	return compareSuccess;
}

uint8_t fromHexDigit(uint8_t c) {
	if ('0' <= c && c <= '9') {
		return c - '0';
	}
	if ('a' <= c && c <= 'f') {
		return c - 'a' + 10;
	}
	if ('A' <= c && c <= 'F') {
		return c - 'A' + 10;
	}
	ASSERT(false);
	throw internal_error();
}

std::string fromPrintable(const std::string& in) {
	std::string result;
	result.reserve(in.size());
	for (auto iter = in.begin(); iter != in.end(); ++iter) {
		if (*iter == '\\') {
			if (++iter == in.end()) ASSERT(false);
			if (*iter == '\\') {
				result.push_back('\\');
			} else if (*iter == 'x') {
				if (++iter == in.end()) ASSERT(false);
				ASSERT(*iter)
				uint8_t b = 16 * fromHexDigit(*iter);
				if (++iter == in.end()) ASSERT(false);
				b += fromHexDigit(*iter);
				result.push_back(b);
			} else {
				ASSERT(false);
			}
		} else {
			result.push_back(*iter);
		}
	}
	return result;
}

std::string toPrintable(StringRef in) {
	std::string result;
	result.reserve(in.size() * 4);
	for (auto iter = in.begin(); iter != in.end(); ++iter) {
		result.push_back('\\');
		result.push_back('x');
		result.push_back("0123456789abcdef"[*iter / 16]);
		result.push_back("0123456789abcdef"[*iter % 16]);
	}
	return result;
}

ACTOR Future<Void> collectRanges(Standalone<VectorRef<std::pair<StringRef, StringRef>>>* ranges,
                                 FutureStream<Optional<FDB::KeyValue>> kvs, int64_t* queueSize) {
	state StringRef lastKey;
	loop {
		Optional<FDB::KeyValue> kv = waitNext(kvs);
		if (!kv.present()) break;
		*queueSize -= kv.get().expectedSize();
		StringRef key{ ranges->arena(), kv.get().key.removePrefix(LiteralStringRef("\xff/keyServers/")) };
		ranges->push_back(ranges->arena(), { lastKey, key });
		lastKey = key;
	}
	ranges->push_back(ranges->arena(), { lastKey, LiteralStringRef("\xff") });
	return Void();
}

ACTOR Future<bool> genMakefileActor(FDB::API* fdb, std::string clusterFile1, std::string clusterFile2) {
	state PromiseStream<Optional<FDB::KeyValue>> kvs1;
	state int64_t queueSize1 = 0;
	state Standalone<VectorRef<std::pair<StringRef, StringRef>>> ranges;
	state Reference<FDB::Database> db = fdb->createDatabase(clusterFile1);
	set_timeout(db.getPtr());
	choose {
		when(wait(readKeyRange(db, LiteralStringRef("\xff/keyServers/\x00"), LiteralStringRef("\xff/keyServers/\xff"),
		                       kvs1, &queueSize1,
		                       /*accessSystemKeys*/ true))) {
			ASSERT(false);
		}
		when(wait(collectRanges(&ranges, kvs1.getFuture(), &queueSize1))) {}
	}
	deterministicRandom()->randomShuffle(ranges);
	std::string uid = deterministicRandom()->randomUniqueID().toString();
	std::ofstream out;
	auto outFileName = format("compare_%s.mk", uid.c_str());
	out.open(outFileName);
	out << ".PHONY: all clean\n";
	out << "all: ";
	for (int i = 0; i < ranges.size(); ++i) {
		if (i > 0) {
			out << " ";
		}
		out << format("shard_%d_%s", i, uid.c_str());
	}
	out << "\n";
	out << format("clean:\n\trm -f shard_*_%s\n", uid.c_str());
	int i = 0;
	for (const auto& [begin, end] : ranges) {
		out << format("shard_%d_%s:\n", i, uid.c_str());
		out << format("\tcompare_key_range %s %s \"%s\" \"%s\"\n", clusterFile1.c_str(), clusterFile2.c_str(),
		              toPrintable(begin).c_str(), toPrintable(end).c_str());
		out << format("\t@touch shard_%d_%s\n", i, uid.c_str());
		++i;
	}
	out.close();
	printf("Wrote %s\n", outFileName.c_str());
	return true;
}

ACTOR static void mainActor(std::function<Future<bool>(FDB::API*)> f) {
	state FDB::API* fdb = nullptr;
	state THREAD_HANDLE clientNetThread;
	try {
		g_network = newNet2(false);
		ASSERT(!FDB::API::isAPIVersionSelected());
		try {
			FDB::API::getInstance();
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_api_version_unset);
		}

		int apiVersion = 300;

		fdb = FDB::API::selectAPIVersion(apiVersion);
		ASSERT(FDB::API::isAPIVersionSelected());
		ASSERT(fdb->getAPIVersion() == apiVersion);
		fdb->setupNetwork();
		clientNetThread = startThread(networkThread, fdb);
		TraceEvent::setNetworkThread();
		selectTraceFormatter("json");
		openTraceFile(NetworkAddress(), 10 << 20, 10 * 10 << 20);
		bool result = wait(f(fdb));
		fdb->stopNetwork();
		waitThread(clientNetThread);
		g_network->stop();
		flushAndExit(result ? 0 : 1);
	} catch (Error& e) {
		fprintf(stderr, "Error: %s\n", e.name());
		TraceEvent(SevError, "CompareKeyRangeError").error(e);
		if (fdb) {
			fdb->stopNetwork();
			waitThread(clientNetThread);
		}
		flushAndExit(1);
	}
}

static const char* helpText = R""(
Usage:

%s --help
  Show this message and exit

%s --version
  Show the source version this binary was built from and exit

%s [OPTIONS] --gen <cluster_file1> <cluster_file2>
  Generate a Makefile to parallelize compare_key_range calls

%s [OPTIONS] <cluster_file1> <cluster_file2> <begin> <end>
  Compare the contents of db's at <cluster_file1> and <cluster_file2> for key range <begin> to <end>

OPTIONS:
  --timeout_ms <timeout_ms>
    All transactions will be given a timeout of <timeout_ms>. Default is %lld ms.
  --bytes_per_read <bytes_per_read>
    Each reads will aim to read <bytes_per_read> bytes. Default is %lld.
  --log_interval_ms <log_interval_ms>
    Log metrics every <log_interval_ms> ms. Default is %lld.
)"";

void printUsage(FILE* f, const char* program_name) {
	fprintf(f, helpText, program_name, program_name, program_name, program_name, timeout_ms, bytes_per_read,
	        log_interval_ms);
}

int main(int argc, char** argv) {
	try {
		platformInit();
		registerCrashHandler();
		bool help = false;
		bool version = false;
		bool gen = false;
		std::vector<std::string> positional_args;
		for (int i = 1; i < argc; ++i) {
			if (std::string_view{ argv[i] } == "--help") {
				help = true;
			} else if (std::string_view{ argv[i] } == "--version") {
				version = true;
			} else if (std::string_view{ argv[i] } == "--gen") {
				gen = true;
			} else if (std::set<std::string_view>{ "--timeout_ms", "--bytes_per_read", "--log_interval_ms" }.count(
			               std::string_view{ argv[i] })) {
				if (i + 1 >= argc) {
					printUsage(stdout, argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				if (std::string_view{ argv[i] } == "--timeout_ms")
					timeout_ms = boost::lexical_cast<int64_t>(argv[i + 1]);
				else if (std::string_view{ argv[i] } == "--bytes_per_read")
					bytes_per_read = boost::lexical_cast<int64_t>(argv[i + 1]);
				else if (std::string_view{ argv[i] } == "--log_interval_ms")
					log_interval_ms = boost::lexical_cast<int64_t>(argv[i + 1]);
				else
					ASSERT(false);
				++i;
			} else {
				positional_args.push_back(argv[i]);
			}
		}
		if (help) {
			printUsage(stdout, argv[0]);
			flushAndExit(FDB_EXIT_SUCCESS);
		}
		if (version) {
			printf("source version %s\n", CURRENT_GIT_VERSION);
			flushAndExit(FDB_EXIT_SUCCESS);
		}
		if (gen) {
			if (positional_args.size() != 2) {
				printUsage(stderr, argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			mainActor([=](FDB::API* fdb) { return genMakefileActor(fdb, positional_args[0], positional_args[1]); });
		} else {
			if (positional_args.size() != 4) {
				printUsage(stderr, argv[0]);
				flushAndExit(FDB_EXIT_ERROR);
			}
			mainActor([=](FDB::API* fdb) {
				return compareKeyRange(fdb, positional_args[0], positional_args[1],
				                       FDB::Key(fromPrintable(positional_args[2])),
				                       FDB::Key(fromPrintable(positional_args[3])));
			});
		}
		g_network->run();
		flushAndExit(FDB_EXIT_SUCCESS);
	} catch (Error& e) {
		fprintf(stderr, "Error: %s\n", e.name());
		TraceEvent(SevError, "MainError").error(e);
		flushAndExit(FDB_EXIT_MAIN_ERROR);
	} catch (std::exception& e) {
		fprintf(stderr, "std::exception: %s\n", e.what());
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		flushAndExit(FDB_EXIT_MAIN_EXCEPTION);
	}
}
