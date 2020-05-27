#include <algorithm>

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

ACTOR static Future<Void> readKeyRange(Reference<FDB::Database> db, FDB::Key begin, FDB::Key end,
                                       PromiseStream<Optional<FDB::KeyValue>> outKvs, int64_t* queueSize) {
	state FDB::GetRangeLimits limit =
	    FDB::GetRangeLimits(FDB::GetRangeLimits::ROW_LIMIT_UNLIMITED, FLOW_KNOBS->PACKET_WARNING);
	state Future<Void> everySecond = delay(1);
	state Reference<FDB::Transaction> tr = db->createTransaction();
	tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
	tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_LOCK_AWARE);
	state Future<FDB::FDBStandalone<FDB::RangeResultRef>> readFuture =
	    tr->getRange(FDB::KeyRangeRef(begin, end), limit, /*snapshot*/ true);
	loop {
		try {
			state FDB::FDBStandalone<FDB::RangeResultRef> kvs = wait(readFuture);
			if (everySecond.isReady()) {
				tr->reset();
				tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
				tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_LOCK_AWARE);
				readFuture = tr->getRange(FDB::KeyRangeRef(begin, end), limit, /*snapshot*/ true);
				everySecond = delay(1);
			}
			if (kvs.size() == 0) {
				outKvs.send(Optional<FDB::KeyValue>());
				return Never();
			}
			begin = FDB::keyAfter(kvs.back().key);
			readFuture = tr->getRange(FDB::KeyRangeRef(begin, end), limit, /*snapshot*/ true);
			while (*queueSize > 1e9) {
				TraceEvent("QueueSizeTooLarge").detail("QueueSize", *queueSize);
				wait(delay(1));
			}
			for (const auto& kv : kvs) {
				outKvs.send(Optional<FDB::KeyValue>(FDB::KeyValue(kv)));
				*queueSize += kv.expectedSize();
			}
		} catch (Error& e) {
			TraceEvent("ReadKeyRangeError").error(e);
			wait(tr->onError(e));
			tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
			tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_LOCK_AWARE);
			readFuture = tr->getRange(FDB::KeyRangeRef(begin, end), limit, /*snapshot*/ true);
		}
	}
}

ACTOR static Future<Void> asyncCompare(std::string clusterFile1, std::string clusterFile2,
                                       FutureStream<Optional<FDB::KeyValue>> kvs1,
                                       FutureStream<Optional<FDB::KeyValue>> kvs2, bool* compareSuccess,
                                       int64_t* queueSize1, int64_t* queueSize2) {
	state int64_t bytesCompared = 0;
	state int64_t lastBytesCompared = 0;
	state double lastLogged = g_network->now();
	state Future<Void> logFuture = delay(1);
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
		bytesCompared += kv1.get().key.size() + kv1.get().value.size();
		if (logFuture.isReady()) {
			logFuture = delay(1);
			if (g_network->now() - lastLogged > 0) {
				printf("Bytes/s: %f\n", double(bytesCompared - lastBytesCompared) / (g_network->now() - lastLogged));
			}
			lastLogged = g_network->now();
			lastBytesCompared = bytesCompared;
		}
	}
}

ACTOR static Future<bool> compareKeyRange(FDB::API* fdb, std::string clusterFile1, std::string clusterFile2,
                                          FDB::Key begin, FDB::Key end) {
	state bool compareSuccess = true;
	PromiseStream<Optional<FDB::KeyValue>> kvs1;
	state int64_t queueSize1 = 0;
	PromiseStream<Optional<FDB::KeyValue>> kvs2;
	state int64_t queueSize2 = 0;
	choose {
		when(wait(readKeyRange(fdb->createDatabase(clusterFile1), begin, end, kvs1, &queueSize1))) { ASSERT(false); }
		when(wait(readKeyRange(fdb->createDatabase(clusterFile2), begin, end, kvs2, &queueSize2))) { ASSERT(false); }
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

ACTOR static void mainActor(std::string clusterFile1, std::string clusterFile2, std::string begin, std::string end) {
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

		FDB::API* fdb = FDB::API::selectAPIVersion(apiVersion);
		ASSERT(FDB::API::isAPIVersionSelected());
		ASSERT(fdb->getAPIVersion() == apiVersion);
		fdb->setupNetwork();
		startThread(networkThread, fdb);
		TraceEvent::setNetworkThread();
		selectTraceFormatter("json");
		openTraceFile(NetworkAddress(), 10 << 20, 10 * 10 << 20);
		bool result = wait(compareKeyRange(fdb, clusterFile1, clusterFile2, FDB::Key(begin), FDB::Key(end)));
		g_network->stop();
		flushAndExit(result ? 0 : 1);
	} catch (Error& e) {
		fprintf(stderr, "Error: %s\n", e.name());
		TraceEvent(SevError, "CompareKeyRangeError").error(e);
		flushAndExit(1);
	}
}

void printUsage(FILE* f, const char* program_name) {
	fprintf(f, "Usage:\n  %s [--help|--version] <cluster_file1> <cluster_file2> <begin> <end>", program_name);
}

int main(int argc, char** argv) {
	try {
		platformInit();
		registerCrashHandler();
		setThreadLocalDeterministicRandomSeed(1);
		bool help = false;
		bool version = false;
		for (int i = 1; i < argc; ++i) {
			if (std::string_view{ argv[i] } == "--help") help = true;
			if (std::string_view{ argv[i] } == "--version") version = true;
		}
		if (help) {
			printUsage(stdout, argv[0]);
			flushAndExit(FDB_EXIT_SUCCESS);
		}
		if (version) {
			printf("source version %s\n", CURRENT_GIT_VERSION);
			flushAndExit(FDB_EXIT_SUCCESS);
		}
		if (argc != 5) {
			printUsage(stderr, argv[0]);
			flushAndExit(FDB_EXIT_ERROR);
		}
		mainActor(argv[1], argv[2], fromPrintable(argv[3]), fromPrintable(argv[4]));
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
