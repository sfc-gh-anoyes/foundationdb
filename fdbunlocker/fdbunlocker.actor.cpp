/*
 * fdbunlocker.actor.cpp
 */

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"

#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/network.h"
#include "flow/SignalSafeUnwind.h"

#include "flow/actorcompiler.h" // This must be the last #include.

void printUsage(const char* name) {
	printf("usage: %s --connfile <CONNFILE> (--lockuid <LOCKUID> | --force) [--version <VERSION]\n", name);
}

Database getDatabase(std::string clusterFileName) {
	std::pair<std::string, bool> resolvedClusterFile = ClusterConnectionFile::lookupClusterFileName(clusterFileName);
	Reference<ClusterConnectionFile> ccf(new ClusterConnectionFile(resolvedClusterFile.first));
	return Database::createDatabase(ccf, -1, false);
}

ACTOR Future<Optional<UID>> getLockUID(Transaction* tr) {
	Optional<Value> val = wait(tr->get(databaseLockedKey));
	if (val.present()) {
		return BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned());
	}
	return {};
}

ACTOR Future<Void> advanceVersion(Database db, Version v) {
	state Transaction tr(db);
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			Version rv = wait(tr.getReadVersion());
			if (rv <= v) {
				tr.set(minRequiredCommitVersionKey, BinaryWriter::toValue(v + 1, Unversioned()));
				wait(tr.commit()); // this commit will always throw an error, because recovery is forced
			} else {
				return Void();
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> unlockDB(std::string clusterFileName, Optional<Version> version, Optional<UID> lockUID) {
	try {
		state Database db = getDatabase(clusterFileName);
		if (version.present()) {
			wait(timeoutError(advanceVersion(db, version.get()), 10.0));
		}
		state Transaction tr(db); // Does not retry if transaction fails
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		if (!lockUID.present()) {
			// if we are forcing an unlock, find the current lock UID, if there is one
			Optional<UID> _lockUID = wait(getLockUID(&tr));
			lockUID = _lockUID;
		}
		if (lockUID.present()) {
			// if database is locked, unlock using current lock UID
			wait(timeoutError(unlockDatabase(&tr, lockUID.get()), 10.0));
			wait(timeoutError(tr.commit(), 10.0));
			printf("Database unlocked at version: %ld.\n", tr.getCommittedVersion());
		}
		else {
			printf("Database already unlocked.\n");
		}
		g_network->stop();
		return Void();
	} catch (Error& e) {
		printf("Error occurred while attempting to unlock database: %s (%d).\n", e.what(), e.code());
		g_network->stop();
		throw e;
	}
}

int main(int argc, char** argv) {
	Optional<std::string> clusterFileName;
	Optional<UID> lockUID;
	Optional<Version> version;
	bool force = false;

	for (int i = 1; i < argc; ++i) {
		std::string arg(argv[i]);
		if (arg == "-C" || arg == "--connfile") {
			if (clusterFileName.present()) {
				printUsage(argv[0]);
				return 1;
			}
			if (i + 1 >= argc) {
				printf("Expecting an argument after %s\n", argv[i]);
				return 1;
			}
			clusterFileName = std::string(argv[++i]);
		} else if (arg == "-L" || arg == "--lockuid") {
			if (lockUID.present() || force) {
				printUsage(argv[0]);
				return 1;
			}
			if (i + 1 >= argc) {
				printf("Expecting an argument after %s\n", argv[i]);
				return 1;
			}
			std::string lockUIDString(argv[++i]);
			if (lockUIDString.size() != 32 || !all_of(lockUIDString.begin(), lockUIDString.end(), &isxdigit)) {
				printf("LockUID is not a valid UID.\n");
				return 1;
			}
			lockUID = UID::fromString(lockUIDString);
		} else if (arg == "-f" || arg == "--force") {
			if (lockUID.present() || force) {
				printUsage(argv[0]);
				return 1;
			}
			force = true;
		} else if (arg == "-v" || arg == "--version") {
			if (version.present()) {
				printUsage(argv[0]);
				return 1;
			}
			if (i + 1 >= argc) {
				printf("Expecting an argument after %s\n", argv[i]);
				return 1;
			}
			version = std::stol(std::string(argv[++i]));
		} else {
			printf("Unexpected argument: %s\n", argv[i]);
		}
	}
	if (!(force ^ lockUID.present()) || !clusterFileName.present()) {
		printUsage(argv[0]);
		return 1;
	}

	try {
		platformInit();
		initSignalSafeUnwind();
		registerCrashHandler();
		setupNetwork();
		auto f = unlockDB(clusterFileName.get(), version, force ? Optional<UID>{} : lockUID);
		runNetwork();
	} catch (Error& e) {
		printf("ERROR: %s (%d)\n", e.what(), e.code());
	}
}
