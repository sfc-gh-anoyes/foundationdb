/*
 * ApplyMetadataMutation.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FDBSERVER_APPLYMETADATAMUTATION_H
#define FDBSERVER_APPLYMETADATAMUTATION_H
#pragma once

#include "fdbclient/MutationList.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/Notified.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogProtocolMessage.h"

// A set of prefixes. Supports fast "does this key start with any of these prefixes?" queries.
struct AllocatedPrefixes {
	bool startsWithAllocatedPrefix(KeyRef key) const {
		Node* current = root.get();
		for (const auto& c : key) {
			const auto& node = current->children[c];
			if (!node) return false;
			current = node.get();
			if (current->allocated) return true;
		}
		return false;
	}
	void add(KeyRef prefix) {
		Node* current = root.get();
		for (const auto& c : prefix) {
			auto& node = current->children[c];
			if (!node) node = std::make_unique<Node>(false);
			current = node.get();
		}
		current->allocated = true;
	}
	void clear(KeyRef prefix) {
		// We could reclaim more memory by resetting nodes which aren't on a path to an allocated node /shrug
		Node* current = root.get();
		for (const auto& c : prefix) {
			auto& node = current->children[c];
			if (!node) return;
			if (node->allocated) {
				node.reset();
				return;
			}
			current = node.get();
		}
	}

	std::vector<std::string> allocatedPrefixes() const {
		std::vector<std::string> out;
		std::string prefix;
		allocatedPrefixesHelper(root.get(), &prefix, &out);
		return out;
	}

private:
	struct Node {
		std::unique_ptr<Node> children[256];
		explicit Node(bool allocated) : allocated(allocated) {}
		bool allocated;
	};
	void allocatedPrefixesHelper(Node* current, std::string* prefix, std::vector<std::string>* out) const {
		if (current->allocated) {
			out->push_back(*prefix);
		} else {
			for (int i = 0; i < 256; ++i) {
				if (current->children[i]) {
					prefix->push_back(i);
					allocatedPrefixesHelper(current->children[i].get(), prefix, out);
					prefix->pop_back();
				}
			}
		}
	}
	// A key matches an allocated prefix iff following (a prefix of) the key's path in tree leads to an allocated node.
	// It's essentially a restricted finite state machine
	std::unique_ptr<Node> root{ std::make_unique<Node>(false) };
};

inline bool isMetadataMutation(MutationRef const& m) {
	// FIXME: This is conservative - not everything in system keyspace is necessarily processed by applyMetadataMutations
	return (m.type == MutationRef::SetValue && m.param1.size() && m.param1[0] >= uint8_t('\xfe') &&
	        !m.param1.startsWith(nonMetadataSystemKeys.begin)) ||
	       (m.type == MutationRef::ClearRange && m.param2.size() && m.param2[0] >= uint8_t('\xfe') &&
	        !nonMetadataSystemKeys.contains(KeyRangeRef(m.param1, m.param2)));
}

struct applyMutationsData {
	Future<Void> worker;
	Version endVersion;
	Reference<KeyRangeMap<Version>> keyVersion;
};

Reference<StorageInfo> getStorageInfo(UID id, std::map<UID, Reference<StorageInfo>>* storageCache, IKeyValueStore* txnStateStore);

void applyMetadataMutations(UID const& dbgid, Arena& arena, VectorRef<MutationRef> const& mutations,
                            IKeyValueStore* txnStateStore, LogPushData* toCommit, bool* confChange,
                            Reference<ILogSystem> logSystem = Reference<ILogSystem>(), Version popVersion = 0,
                            KeyRangeMap<std::set<Key>>* vecBackupKeys = NULL,
                            KeyRangeMap<ServerCacheInfo>* keyInfo = NULL,
                            std::map<Key, applyMutationsData>* uid_applyMutationsData = NULL,
                            RequestStream<CommitTransactionRequest> commit = RequestStream<CommitTransactionRequest>(),
                            Database cx = Database(), NotifiedVersion* commitVersion = NULL,
                            std::map<UID, Reference<StorageInfo>>* storageCache = NULL,
                            std::map<Tag, Version>* tag_popped = NULL, bool initialCommit = false,
                            AllocatedPrefixes* allocatedPrefixes = nullptr);

#endif
