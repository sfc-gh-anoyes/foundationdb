/*
 * LargeTransactions.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include <boost/lexical_cast.hpp>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/ClusterRecruitmentInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 1

struct LargeTransactionsWorkload : TestWorkload {
	uint64_t mutationSize;
	uint64_t mutationCount;
	uint64_t dataSize;
	int mutationSizeRange;
	int mutationCountRange;
	int duration;
	int keyRange;
	int pushes;
	double pushRate;
	double interval;
	std::unique_ptr<int64_t[]> data;
	LargeTransactionsWorkload(WorkloadContext const& wcx)

	  : TestWorkload(wcx), pushes(0) {

		std::string workloadName = "LargeTransactionsWorkload";
		// by default, get as close to the max transaction size
		// and mutation size as possible
		mutationSize =
		    getOption(options, LiteralStringRef("mutationSize"), uint64_t(CLIENT_KNOBS->VALUE_SIZE_LIMIT - 1001));
		mutationCount =
		    getOption(options, LiteralStringRef("mutationCount"),
		              uint64_t((CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT - 25) / (CLIENT_KNOBS->VALUE_SIZE_LIMIT + 1000)));
		mutationCountRange = getOption(options, LiteralStringRef("mutationCountRange"), 25);
		mutationSizeRange = getOption(options, LiteralStringRef("mutationSizeRange"), 1000);
		keyRange = getOption(options, LiteralStringRef("keyRange"), 4);
		duration = getOption(options, LiteralStringRef("duration"), 10);
		pushRate = getOption(options, LiteralStringRef("pushRate"),
		                     0.1); // interval is calculated with a +=(interval/2) jitter
		interval = (double)1 / pushRate;
	}

	std::string description() override { return "LargeTransactions"; }
	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			populateData();
		}
		return Void();
	}
	Future<Void> start(Database const& cx) override { return clientId == 0 ? largeClient(cx, this) : Void(); }
	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			TraceEvent("LargeTransactionsWorkloadResult").detail("Pushes", pushes);
			if (pushes > 0) {
				return true;
			} else {
				TraceEvent(SevError, "LargeTransactionsWorkloadNoPushes");
				return false;
			}
		} else {
			return true;
		}
	}
	void getMetrics(std::vector<PerfMetric>& m) override { m.push_back(PerfMetric("Pushes", pushes, false)); }

	Key getKey() { return StringRef("LargeKey" + deterministicRandom()->randomAlphaNumeric(keyRange)); }

	void populateData() {
		dataSize = (mutationSize + mutationSizeRange) * mutationCount;
		uint64_t dataInt64s = dataSize / sizeof(int64_t);
		std::unique_ptr<int64_t[]> buf(new int64_t[dataInt64s]);
		data = std::move(buf);
		// populate with random data
		TraceEvent("LargeTransactionsWorkloadSetup")
		    .detail("DataSize", dataSize)
		    .detail("MutSizeRange", mutationSizeRange)
		    .detail("PushInterval", interval)
		    .detail("MutationCount", mutationCount);
		for (uint64_t i = 0; i < dataInt64s; i++) {
			data[i] = 0xdeadbeefdeadbeef;
		}
	}

	ACTOR Future<Void> largeClient(Database cx, LargeTransactionsWorkload* self) {
		state double startTime = g_network->now();
		state StringRef values((uint8_t*)self->data.get(), self->dataSize);
		loop {
			wait(delay(deterministicRandom()->random01() * self->interval));
			state Transaction tr(cx);
			loop {
				try {
					uint64_t mutBegin = 0;
					uint64_t mutEnd = 0;
					uint64_t mutCount = self->mutationCount;
					if (self->mutationCountRange) {
						mutCount += deterministicRandom()->randomInt(0, self->mutationCountRange);
					}

					for (int i = 0; i < self->mutationCount; i++) {
						mutBegin = mutEnd;
						mutEnd += self->mutationSize;
						if (self->mutationSizeRange) {
							mutEnd += deterministicRandom()->randomInt(0, self->mutationSizeRange);
						}
						Key key = self->getKey();
						ValueRef largeValue = values.substr(mutBegin, mutEnd - mutBegin);
						TraceEvent("LargeTransactionsSetKey").detail("Key", key).detail("MSize", mutEnd - mutBegin);
						tr.set(key, largeValue);
					}
					state uint32_t trSize = tr.getSize();
					wait(tr.commit());
					TraceEvent("LargeTransactionsPushFinished")
					    .detail("Version", tr.getCommittedVersion())
					    .detail("TransactionSize", trSize);
					self->pushes++;
					break;
				} catch (Error& e) {
					if (e.code() == error_code_database_locked && g_network->now() - startTime > self->duration) break;
					wait(tr.onError(e));
				}
			}
			if (g_network->now() - startTime > self->duration) {
				TraceEvent("LargeTransactionsClientFinished");
				return Void();
			}
			wait(delay(self->interval / 2));
		}
	}
};

WorkloadFactory<LargeTransactionsWorkload> LargeTransactionsWorkloadFactory("LargeTransactions");
