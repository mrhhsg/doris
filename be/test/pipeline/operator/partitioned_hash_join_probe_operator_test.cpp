// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "pipeline/exec/partitioned_hash_join_probe_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "partitioned_hash_join_test_helper.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/partitioned_hash_join_sink_operator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "testutil/mock/mock_operators.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
namespace {
// Match spill partition bit slicing used by hash join spill.
uint32_t spill_partition_index(uint32_t hash, uint32_t level) {
    return (hash >> (level * kHashJoinSpillBitsPerLevel)) & (kHashJoinSpillFanout - 1);
}

// Follow build-side split hierarchy to decide the final partition for a hash.
HashJoinSpillPartitionId find_partition_for_hash(
        uint32_t hash, const HashJoinSpillBuildPartitionMap& build_partitions) {
    HashJoinSpillPartitionId id {0, spill_partition_index(hash, 0)};
    auto it = build_partitions.find(id.key());
    while (it != build_partitions.end() && it->second.is_split &&
           id.level < kHashJoinSpillMaxDepth) {
        const auto child_index = spill_partition_index(hash, id.level + 1);
        id = id.child(child_index);
        it = build_partitions.find(id.key());
    }
    return id;
}

size_t partition_rows(const HashJoinSpillPartition& partition) {
    size_t rows = 0;
    if (partition.accumulating_block) {
        rows += partition.accumulating_block->rows();
    }
    for (const auto& block : partition.blocks) {
        rows += block.rows();
    }
    return rows;
}
} // namespace

class PartitionedHashJoinProbeOperatorTest : public testing::Test {
public:
    void SetUp() override { _helper.SetUp(); }
    void TearDown() override { _helper.TearDown(); }

protected:
    PartitionedHashJoinTestHelper _helper;
};

TEST_F(PartitionedHashJoinProbeOperatorTest, debug_string) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    auto debug_string = local_state->debug_string(0);
    std::cout << "debug string: " << debug_string << std::endl;

    shared_state->is_spilled = false;
    debug_string = local_state->debug_string(0);
    std::cout << "debug string: " << debug_string << std::endl;

    ASSERT_TRUE(local_state->operator_profile()->pretty_print().find("ExecTime") !=
                std::string::npos);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, InitAndOpen) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    auto local_state = PartitionedHashJoinProbeLocalState::create_shared(
            _helper.runtime_state.get(), probe_operator.get());

    auto shared_state = std::make_shared<PartitionedHashJoinSharedState>();
    LocalStateInfo info {.parent_profile = _helper.operator_profile.get(),
                         .scan_ranges = {},
                         .shared_state = shared_state.get(),
                         .shared_state_map = {},
                         .task_idx = 0};
    auto st = local_state->init(_helper.runtime_state.get(), info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    // before opening, should setup probe_operator's partitioner.
    const auto& tnode = probe_operator->_tnode;
    auto child = std::make_shared<MockChildOperator>();
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    child->_row_descriptor = row_desc;

    probe_operator->_inner_sink_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_child = nullptr;
    probe_operator->_inner_probe_operator->_build_side_child = nullptr;

    st = probe_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    st = probe_operator->prepare(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "prepare failed: " << st.to_string();

    st = local_state->open(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "open failed: " << st.to_string();

    local_state->_shared_state->inner_shared_state = std::make_shared<MockHashJoinSharedState>();
    local_state->_shared_state->inner_runtime_state = std::make_unique<MockRuntimeState>();
    local_state->_shared_state->inner_runtime_state->set_desc_tbl(
            &(_helper.runtime_state->desc_tbl()));
    local_state->_shared_state->inner_runtime_state->resize_op_id_to_local_state(-100);

    auto mock_inner_sink_operator = probe_operator->_inner_sink_operator;
    probe_operator->_inner_sink_operator = std::make_shared<HashJoinBuildSinkOperatorX>(
            _helper.obj_pool.get(), 0, 0, tnode, _helper.runtime_state->desc_tbl());
    EXPECT_TRUE(probe_operator->_inner_sink_operator->set_child(mock_inner_sink_operator->child()));

    st = probe_operator->_inner_sink_operator->init(tnode, _helper.runtime_state.get());
    ASSERT_TRUE(st) << "init inner sink operator failed: " << st.to_string();

    auto inner_probe_state = std::make_unique<HashJoinProbeLocalState>(
            _helper.runtime_state.get(), probe_operator->_inner_probe_operator.get());

    st = inner_probe_state->init(local_state->_shared_state->inner_runtime_state.get(), info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    local_state->_shared_state->inner_runtime_state->emplace_local_state(
            probe_operator->_inner_probe_operator->operator_id(), std::move(inner_probe_state));

    auto inner_sink_state = std::make_unique<HashJoinBuildSinkLocalState>(
            probe_operator->_inner_sink_operator.get(), _helper.runtime_state.get());

    LocalSinkStateInfo sink_info {0,  _helper.operator_profile.get(),
                                  -1, local_state->_shared_state->inner_shared_state.get(),
                                  {}, {}};
    st = probe_operator->_inner_sink_operator->prepare(
            local_state->_shared_state->inner_runtime_state.get());
    ASSERT_TRUE(st) << "prepare failed: " << st.to_string();

    st = inner_sink_state->init(local_state->_shared_state->inner_runtime_state.get(), sink_info);
    ASSERT_TRUE(st) << "init failed: " << st.to_string();

    local_state->_shared_state->inner_runtime_state->emplace_sink_local_state(
            0, std::move(inner_sink_state));

    local_state->_shared_state->is_spilled = false;
    local_state->update_profile_from_inner();

    local_state->_shared_state->is_spilled = true;
    local_state->update_profile_from_inner();

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();

    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();

    auto debug_string = local_state->debug_string(0);
    std::cout << "debug string: " << debug_string << std::endl;

    // close is reentrant.
    st = local_state->close(_helper.runtime_state.get());
    ASSERT_TRUE(st) << "close failed: " << st.to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, spill_probe_blocks) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // create probe blocks
    for (int32_t i = 0; i != PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; ++i) {
        if (i % 2 == 0) {
            continue;
        }

        vectorized::Block block = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(
                {1 * i, 2 * i, 3 * i});
        HashJoinSpillPartitionId partition_id {0, static_cast<uint32_t>(i)};
        auto& partition = local_state->_shared_state->probe_partitions[partition_id.key()];
        partition.id = partition_id;
        partition.blocks.emplace_back(std::move(block));
    }

    std::vector<int32_t> large_data(3 * 1024 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    vectorized::Block large_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

    std::vector<int32_t> small_data(3 * 1024);
    std::iota(small_data.begin(), small_data.end(), 3 * 1024 * 1024);
    vectorized::Block small_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(small_data);

    // add a large block to the last partition
    HashJoinSpillPartitionId last_partition_id {
            0, PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT - 1};
    auto& last_partition = local_state->_shared_state->probe_partitions[last_partition_id.key()];
    last_partition.id = last_partition_id;
    last_partition.accumulating_block =
            vectorized::MutableBlock::create_unique(std::move(large_block));

    // add a small block to the first partition
    HashJoinSpillPartitionId first_partition_id {0, 0};
    auto& first_partition = local_state->_shared_state->probe_partitions[first_partition_id.key()];
    first_partition.id = first_partition_id;
    first_partition.accumulating_block =
            vectorized::MutableBlock::create_unique(std::move(small_block));

    local_state->_shared_state->is_spilled = false;
    local_state->update_profile_from_inner();

    local_state->_shared_state->is_spilled = true;
    auto st = local_state->spill_probe_blocks(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "spill probe blocks failed: " << st.to_string();

    local_state->update_profile_from_inner();

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    // Cleanup spill streams from probe_partitions
    for (auto& [key, partition] : local_state->_shared_state->probe_partitions) {
        if (partition.spill_stream) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(partition.spill_stream);
            partition.spill_stream.reset();
        }
    }

    auto* write_rows_counter = local_state->custom_profile()->get_counter("SpillWriteRows");
    ASSERT_EQ(write_rows_counter->value(),
              (PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT / 2) * 3 + 3 * 1024 * 1024);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDisk) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register a spill stream for testing
    HashJoinSpillPartitionId partition_id {0, 0};
    auto& partition = local_state->_shared_state->probe_partitions[partition_id.key()];
    partition.id = partition_id;
    auto& spill_stream = partition.spill_stream;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spill_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spill_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spill_stream->spill_eof().ok());
    }

    // Test recovery
    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                         partition_id.key(), has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    // Verify recovered data
    auto& probe_blocks = partition.blocks;
    ASSERT_FALSE(probe_blocks.empty());
    ASSERT_EQ(probe_blocks[0].rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 1);

    // Verify stream cleanup
    ASSERT_EQ(partition.spill_stream, nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskLargeData) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register a spill stream for testing
    HashJoinSpillPartitionId partition_id {0, 0};
    auto& partition = local_state->_shared_state->probe_partitions[partition_id.key()];
    partition.id = partition_id;
    auto& spill_stream = partition.spill_stream;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spill_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        // create block larger than 32MB(4 * (8 * 1024 * 1024 + 10))
        std::vector<int32_t> large_data(8 * 1024 * 1024 + 10);
        std::iota(large_data.begin(), large_data.end(), 0);
        vectorized::Block large_block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

        ASSERT_TRUE(
                spill_stream->spill_block(_helper.runtime_state.get(), large_block, false).ok());

        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spill_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spill_stream->spill_eof().ok());
    }

    // Test recovery
    bool has_data = true;
    while (has_data) {
        ASSERT_TRUE(local_state
                            ->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                             partition_id.key(), has_data)
                            .ok());
    }

    std::cout << "profile: " << local_state->custom_profile()->pretty_print() << std::endl;

    // Verify recovered data
    auto& probe_blocks = partition.blocks;
    ASSERT_FALSE(probe_blocks.empty());
    ASSERT_EQ(probe_blocks[0].rows(), 8 * 1024 * 1024 + 10);
    ASSERT_EQ(probe_blocks[1].rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryProbeRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3 + 8 * 1024 * 1024 + 10);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 2);

    // Verify stream cleanup
    ASSERT_EQ(partition.spill_stream, nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskEmpty) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test multiple cases
    HashJoinSpillPartitionId partition_id {0, 0};
    auto& partition = local_state->_shared_state->probe_partitions[partition_id.key()];
    partition.id = partition_id;
    auto& spilled_stream = partition.spill_stream;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());
    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                         partition_id.key(), has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    ASSERT_TRUE(partition.blocks.empty()) << "probe blocks not empty: " << partition.blocks.size();

    ASSERT_TRUE(spilled_stream == nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverProbeBlocksFromDiskError) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test multiple cases
    HashJoinSpillPartitionId partition_id {0, 0};
    auto& partition = local_state->_shared_state->probe_partitions[partition_id.key()];
    partition.id = partition_id;
    auto& spilling_stream = partition.spill_stream;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilling_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_probe",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spilling_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spilling_stream->spill_eof().ok());
    }

    SpillableDebugPointHelper dp_helper("fault_inject::spill_stream::read_next_block");
    bool has_data = false;
    auto status = local_state->recover_probe_blocks_from_disk(_helper.runtime_state.get(),
                                                              partition_id.key(), has_data);

    ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilling_stream);
    spilling_stream.reset();

    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.to_string().find("fault_inject spill_stream read_next_block") !=
                std::string::npos)
            << "unexpected error: " << status.to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDisk) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Create and register spill stream with test data
    const uint32_t test_partition = 0;
    HashJoinSpillPartitionId test_partition_id {0, test_partition};
    auto& build_partition = local_state->_shared_state->build_partitions[test_partition_id.key()];
    build_partition.id = test_partition_id;
    auto& spilled_stream = build_partition.spill_stream;

    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write test data
    {
        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spilled_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
        ASSERT_TRUE(spilled_stream->spill_eof().ok());
    }

    // Test recovery
    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    // Verify recovered data
    ASSERT_TRUE(local_state->_recovered_build_block != nullptr);
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildRows");
    ASSERT_EQ(recovery_rows_counter->value(), 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 1);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, need_more_input_data) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->is_spilled = true;
    local_state->_child_eos = false;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()),
              !local_state->_child_eos);

    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()),
              !local_state->_child_eos);

    local_state->_shared_state->is_spilled = false;
    auto inner_operator = std::dynamic_pointer_cast<MockHashJoinProbeOperator>(
            probe_operator->_inner_probe_operator);

    inner_operator->need_more_data = true;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()), true);
    inner_operator->need_more_data = false;
    ASSERT_EQ(probe_operator->need_more_input_data(_helper.runtime_state.get()), false);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, revocable_mem_size) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()), 0);

    local_state->_child_eos = false;
    auto block1 = vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    HashJoinSpillPartitionId partition_id {0, 0};
    auto& partition = shared_state->probe_partitions[partition_id.key()];
    partition.id = partition_id;
    partition.blocks.emplace_back(block1);
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()),
              block1.allocated_bytes());
    auto block2 =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 5, 6, 7});
    partition.accumulating_block = vectorized::MutableBlock::create_unique(std::move(block2));

    // block2 is small, so it should not be counted
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()),
              block1.allocated_bytes());

    // Create large input block (> 32k)
    std::vector<int32_t> large_data(9 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    vectorized::Block large_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

    const auto large_size = large_block.allocated_bytes();
    partition.accumulating_block = vectorized::MutableBlock::create_unique(std::move(large_block));
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()),
              block1.allocated_bytes() + large_size);

    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->revocable_mem_size(_helper.runtime_state.get()), 0);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, get_reserve_mem_size) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->is_spilled = true;
    local_state->_child_eos = false;

    local_state->_need_to_setup_internal_operators = false;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM);

    local_state->_need_to_setup_internal_operators = true;
    ASSERT_GT(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM);

    const auto default_reserve_size =
            _helper.runtime_state->minimum_operator_memory_required_bytes() +
            probe_operator->get_child()->get_reserve_mem_size(_helper.runtime_state.get());
    local_state->_shared_state->is_spilled = false;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              default_reserve_size);

    local_state->_shared_state->is_spilled = true;
    local_state->_child_eos = true;
    ASSERT_EQ(probe_operator->get_reserve_mem_size(_helper.runtime_state.get()),
              default_reserve_size);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskEmpty) {
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test empty stream
    const uint32_t test_partition = 0;
    HashJoinSpillPartitionId test_partition_id {0, test_partition};
    auto& build_partition = local_state->_shared_state->build_partitions[test_partition_id.key()];
    ASSERT_EQ(build_partition.id.level, test_partition_id.level);
    ASSERT_EQ(build_partition.id.path, test_partition_id.path);
    auto& spilled_stream = build_partition.spill_stream;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    bool has_data = false;
    ASSERT_TRUE(local_state
                        ->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                         test_partition, has_data)
                        .ok());
    ASSERT_TRUE(has_data);

    ASSERT_EQ(spilled_stream, nullptr);
    ASSERT_TRUE(local_state->_recovered_build_block == nullptr);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskLargeData) {
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test empty stream
    const uint32_t test_partition = 0;
    HashJoinSpillPartitionId test_partition_id {0, test_partition};
    auto& build_partition = local_state->_shared_state->build_partitions[test_partition_id.key()];
    auto& spilled_stream = build_partition.spill_stream;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    // Write some test data to spill stream
    {
        // create block larger than 32MB(4 * (8 * 1024 * 1024 + 10))
        std::vector<int32_t> large_data(8 * 1024 * 1024 + 10);
        std::iota(large_data.begin(), large_data.end(), 0);
        vectorized::Block large_block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

        ASSERT_TRUE(
                spilled_stream->spill_block(_helper.runtime_state.get(), large_block, false).ok());

        vectorized::Block block =
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
        ASSERT_TRUE(spilled_stream->spill_block(_helper.runtime_state.get(), block, false).ok());
    }
    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    bool has_data = false;
    do {
        ASSERT_TRUE(local_state
                            ->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                             test_partition, has_data)
                            .ok());

        ASSERT_TRUE(local_state->_recovered_build_block);
    } while (has_data);

    ASSERT_EQ(spilled_stream, nullptr);

    // Verify recovered data
    ASSERT_EQ(local_state->_recovered_build_block->rows(), 8 * 1024 * 1024 + 10 + 3);

    // Verify counters
    auto* recovery_rows_counter =
            local_state->custom_profile()->get_counter("SpillRecoveryBuildRows");
    ASSERT_EQ(recovery_rows_counter->value(), 8 * 1024 * 1024 + 10 + 3);
    auto* recovery_blocks_counter =
            local_state->custom_profile()->get_counter("SpillReadBlockCount");
    ASSERT_EQ(recovery_blocks_counter->value(), 2);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, RecoverBuildBlocksFromDiskError) {
    // Similar setup code as above...
    // Similar setup as above...
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Test empty stream
    const uint32_t test_partition = 0;
    HashJoinSpillPartitionId test_partition_id {0, test_partition};
    auto& build_partition = local_state->_shared_state->build_partitions[test_partition_id.key()];
    auto& spilled_stream = build_partition.spill_stream;
    ASSERT_TRUE(ExecEnv::GetInstance()
                        ->spill_stream_mgr()
                        ->register_spill_stream(
                                _helper.runtime_state.get(), spilled_stream,
                                print_id(_helper.runtime_state->query_id()), "hash_build",
                                probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
                                std::numeric_limits<size_t>::max(), local_state->operator_profile())
                        .ok());

    ASSERT_TRUE(spilled_stream->spill_eof().ok());

    ASSERT_TRUE(local_state->_recovered_build_block == nullptr);

    // Test error handling with fault injection
    SpillableDebugPointHelper dp_helper(
            "fault_inject::partitioned_hash_join_probe::recover_build_blocks");
    bool has_data = false;
    auto status = local_state->recover_build_blocks_from_disk(_helper.runtime_state.get(),
                                                              test_partition, has_data);

    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.to_string().find("fault_inject partitioned_hash_join_probe "
                                        "recover_build_blocks failed") != std::string::npos)
            << "incorrect recover build blocks status: " << status.to_string();
}

TEST_F(PartitionedHashJoinProbeOperatorTest, GetBlockTestNonSpill) {
    // Setup operators and pipeline
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup child data
    auto input_block = vectorized::Block::create_unique();
    input_block->swap(vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));

    auto probe_side_source_operator =
            std::dynamic_pointer_cast<MockChildOperator>(probe_operator->get_child());
    probe_side_source_operator->set_block(std::move(*input_block));

    local_state->_shared_state->is_spilled = false;

    // Test non empty input block path
    {
        vectorized::Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok());
        ASSERT_FALSE(eos);
    }

    // Test empty input block case
    {
        auto empty_block = vectorized::Block::create_unique();
        probe_side_source_operator->set_block(std::move(*empty_block));

        vectorized::Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(output_block.rows(), 0);
    }

    // Test end of stream case
    {
        probe_side_source_operator->set_eos();
        vectorized::Block output_block;
        bool eos = false;

        auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "get block failed: " << st.to_string();
        ASSERT_TRUE(eos);
    }
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushEmptyBlock) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Setup local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    _helper.create_probe_local_state(_helper.runtime_state.get(), probe_operator.get(),
                                     shared_state);

    // Create empty input block
    vectorized::Block empty_block;

    // Test pushing empty block without EOS
    auto st = probe_operator->push(_helper.runtime_state.get(), &empty_block, false);
    ASSERT_TRUE(st.ok());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushPartitionData) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);
    // Setup row descriptor and partitioner
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create test input block
    vectorized::Block input_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5});

    // Test pushing data
    auto st = probe_operator->push(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok());

    // Verify partitioned blocks
    int64_t total_partitioned_rows = 0;
    for (auto& [key, partition] : local_state->_shared_state->probe_partitions) {
        if (partition.accumulating_block) {
            total_partitioned_rows += partition.accumulating_block->rows();
        }
    }
    ASSERT_EQ(total_partitioned_rows, 5); // All rows should be partitioned
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushWithEOS) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup row descriptor and partitioner
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create test data and push with EOS
    vectorized::Block input_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});

    auto st = probe_operator->push(_helper.runtime_state.get(), &input_block, false);
    ASSERT_TRUE(st.ok()) << "Push failed: " << st.to_string();

    input_block.clear();
    st = probe_operator->push(_helper.runtime_state.get(), &input_block, true);
    ASSERT_TRUE(st.ok()) << "Push failed: " << st.to_string();

    // Verify all data is moved to probe blocks due to EOS
    int64_t total_probe_block_rows = 0;
    for (auto& [key, partition] : local_state->_shared_state->probe_partitions) {
        for (const auto& block : partition.blocks) {
            total_probe_block_rows += block.rows();
        }
    }
    ASSERT_EQ(total_probe_block_rows, 3); // All rows should be in probe blocks

    // Verify partitioned blocks are cleared
    for (auto& [key, partition] : local_state->_shared_state->probe_partitions) {
        ASSERT_EQ(partition.accumulating_block, nullptr);
    }
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PushLargeBlock) {
    // Setup test environment
    auto [probe_operator, sink_operator] = _helper.create_operators();

    // Initialize local state
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // Setup row descriptor and partitioner
    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    // Create large input block (> 2M rows)
    std::vector<int32_t> large_data(3 * 1024 * 1024);
    std::iota(large_data.begin(), large_data.end(), 0);
    vectorized::Block large_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(large_data);

    // Push large block
    auto st = probe_operator->push(_helper.runtime_state.get(), &large_block, false);
    ASSERT_TRUE(st.ok());

    // Verify some partitions have blocks moved to probe_blocks due to size threshold
    bool found_probe_blocks = false;
    size_t partitioned_rows_count = 0;
    for (auto& [key, partition] : local_state->_shared_state->probe_partitions) {
        if (!partition.blocks.empty()) {
            for (auto& block : partition.blocks) {
                if (!block.empty()) {
                    partitioned_rows_count += block.rows();
                    found_probe_blocks = true;
                }
            }
        }
        if (partition.accumulating_block && !partition.accumulating_block->empty()) {
            partitioned_rows_count += partition.accumulating_block->rows();
            found_probe_blocks = true;
        }
    }

    ASSERT_EQ(partitioned_rows_count, large_block.rows());
    ASSERT_TRUE(found_probe_blocks);

    // Verify bytes counter
    auto* probe_blocks_bytes = local_state->custom_profile()->get_counter("ProbeBloksBytesInMem");
    ASSERT_GT(probe_blocks_bytes->value(), 0);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullBasic) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_need_to_setup_internal_operators = true;
    local_state->_partition_cursor = 0;

    vectorized::Block test_block;
    bool eos = false;

    auto st = probe_operator->pull(_helper.runtime_state.get(), &test_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();
    ASSERT_FALSE(eos) << "First pull should not be eos";

    ASSERT_EQ(1, local_state->_partition_cursor) << "Partition cursor should be 1";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullMultiplePartitions) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    for (uint32_t i = 0; i < PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; i++) {
        HashJoinSpillPartitionId partition_id {0, i};
        auto& partition = shared_state->probe_partitions[partition_id.key()];
        partition.id = partition_id;
        partition.blocks.emplace_back(
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));
    }

    vectorized::Block output_block;
    bool eos = false;

    for (uint32_t i = 0; i < PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT; i++) {
        local_state->_partition_cursor = i;
        local_state->_need_to_setup_internal_operators = true;

        auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "Pull failed for partition " << i;

        if (i == PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT - 1) {
            ASSERT_TRUE(eos) << "Last partition should be eos";
        } else {
            ASSERT_FALSE(eos) << "Non-last partition should not be eos";
        }
    }
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullWithDiskRecovery) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->is_spilled = true;

    HashJoinSpillPartitionId partition_id {0, 0};
    auto& spilled_stream =
            local_state->_shared_state->build_partitions[partition_id.key()].spill_stream;
    auto& probe_partition = local_state->_shared_state->probe_partitions[partition_id.key()];
    probe_partition.id = partition_id;
    auto& spilling_stream = probe_partition.spill_stream;

    local_state->_need_to_setup_internal_operators = true;

    auto st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), spilled_stream,
            print_id(_helper.runtime_state->query_id()), "hash_probe_spilled",
            probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
            std::numeric_limits<size_t>::max(), local_state->operator_profile());

    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();
    st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), spilling_stream,
            print_id(_helper.runtime_state->query_id()), "hash_probe", probe_operator->node_id(),
            std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
            local_state->operator_profile());

    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();

    vectorized::Block spill_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3});
    st = spilled_stream->spill_block(_helper.runtime_state.get(), spill_block, true);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();
    st = spilling_stream->spill_block(_helper.runtime_state.get(), spill_block, true);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();

    vectorized::Block output_block;
    bool eos = false;

    st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();

    st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);

    ASSERT_TRUE(st.ok()) << "Pull failed: " << st.to_string();
    ASSERT_FALSE(eos) << "Should not be eos during disk recovery";

    ASSERT_GT(local_state->_recovery_probe_rows->value(), 0)
            << "Should have recovered some rows from disk";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, PullWithEmptyPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();
    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    // 设置空分区
    local_state->_partition_cursor = 0;
    local_state->_need_to_setup_internal_operators = true;

    vectorized::Block output_block;
    bool eos = false;

    auto st = probe_operator->pull(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "Pull failed for empty partition";
    ASSERT_FALSE(eos) << "Should not be eos for first empty partition";

    // 验证分区游标已更新
    ASSERT_EQ(1, local_state->_partition_cursor)
            << "Partition cursor should move to next after empty partition";
}

TEST_F(PartitionedHashJoinProbeOperatorTest, SplitProbePartitionCreatesChildren) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    std::vector<int32_t> data(100);
    std::iota(data.begin(), data.end(), 0);
    vectorized::Block block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(data);
    HashJoinSpillPartitionId partition_id {0, 0};
    auto& partition = shared_state->probe_partitions[partition_id.key()];
    partition.id = partition_id;
    partition.accumulating_block = vectorized::MutableBlock::create_unique(std::move(block));

    auto st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state,
                                                     partition_id);
    ASSERT_TRUE(st.ok()) << "split failed: " << st.to_string();

    auto parent_it = shared_state->probe_partitions.find(partition_id.key());
    ASSERT_TRUE(parent_it != shared_state->probe_partitions.end());
    ASSERT_TRUE(parent_it->second.is_split);
    ASSERT_FALSE(parent_it->second.children.empty());
    ASSERT_EQ(parent_it->second.accumulating_block, nullptr);
    ASSERT_TRUE(local_state->_base_partition_split[0]);

    // Child blocks should preserve total rows after split.
    size_t total_rows = 0;
    for (const auto& child_id : parent_it->second.children) {
        auto child_it = shared_state->probe_partitions.find(child_id.key());
        ASSERT_TRUE(child_it != shared_state->probe_partitions.end());
        for (const auto& child_block : child_it->second.blocks) {
            total_rows += child_block.rows();
        }
    }
    ASSERT_EQ(total_rows, 100);
    ASSERT_EQ(local_state->_pending_partitions.size(), parent_it->second.children.size());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, SplitProbePartitionRespectsMaxDepth) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    HashJoinSpillPartitionId partition_id {kHashJoinSpillMaxDepth, 0};
    auto& partition = shared_state->probe_partitions[partition_id.key()];
    partition.id = partition_id;
    partition.blocks.emplace_back(
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));

    auto st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state,
                                                     partition_id);
    ASSERT_TRUE(st.ok()) << "split failed: " << st.to_string();
    ASSERT_TRUE(partition.children.empty());
    ASSERT_FALSE(partition.blocks.empty());
    ASSERT_TRUE(local_state->_pending_partitions.empty());
}

TEST_F(PartitionedHashJoinProbeOperatorTest, SplitBuildPartitionCreatesChildren) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor build_row_desc(_helper.runtime_state->desc_tbl(), {1});
    const auto& tnode = probe_operator->_tnode;
    local_state->_build_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].right}, build_row_desc);

    vectorized::Block build_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5});

    auto build_partition_id = HashJoinSpillPartitionId {0, 0};
    auto& build_partition = shared_state->build_partitions[build_partition_id.key()];
    ASSERT_EQ(build_partition.id.level, build_partition_id.level);
    ASSERT_EQ(build_partition.id.path, build_partition_id.path);
    build_partition.build_block = vectorized::MutableBlock::create_unique(std::move(build_block));

    HashJoinSpillPartitionId partition_id {0, 0};
    auto st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state,
                                                     partition_id);
    ASSERT_TRUE(st.ok()) << "split failed: " << st.to_string();

    auto parent_it = shared_state->build_partitions.find(partition_id.key());
    ASSERT_TRUE(parent_it != shared_state->build_partitions.end());
    ASSERT_TRUE(parent_it->second.is_split);
    ASSERT_FALSE(parent_it->second.children.empty());

    size_t total_rows = 0;
    for (const auto& child_id : parent_it->second.children) {
        auto child_it = shared_state->build_partitions.find(child_id.key());
        ASSERT_TRUE(child_it != shared_state->build_partitions.end());
        if (child_it->second.build_block) {
            total_rows += child_it->second.build_block->rows();
        }
    }
    ASSERT_EQ(total_rows, 5);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, ProbeFollowsMultiLevelBuildSplit) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    std::vector<int32_t> data(2048);
    std::iota(data.begin(), data.end(), 0);
    vectorized::Block block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(data);
    ASSERT_TRUE(
            local_state->_partitioner->do_partitioning(_helper.runtime_state.get(), &block).ok());

    const auto* hashes = local_state->_partitioner->get_channel_ids().get<uint32_t>();
    const uint32_t base_index = spill_partition_index(hashes[0], 0);
    std::array<size_t, kHashJoinSpillFanout> child_counts {};
    for (uint32_t i = 0; i < block.rows(); ++i) {
        if (spill_partition_index(hashes[i], 0) != base_index) {
            continue;
        }
        child_counts[spill_partition_index(hashes[i], 1)]++;
    }

    uint32_t split_child_index = 0;
    bool found_child = false;
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        if (child_counts[i] > 0) {
            split_child_index = i;
            found_child = true;
            break;
        }
    }
    ASSERT_TRUE(found_child) << "No rows found for base partition child split";

    auto& build_partitions = shared_state->build_partitions;
    HashJoinSpillPartitionId parent_id {0, base_index};
    auto& parent = build_partitions[parent_id.key()];
    parent.id = parent_id;
    parent.is_split = true;
    parent.children.clear();
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        HashJoinSpillPartitionId child_id = parent_id.child(i);
        HashJoinSpillBuildPartition build_partition;
        build_partition.id = child_id;
        build_partitions.try_emplace(child_id.key(), std::move(build_partition));
        parent.children.emplace_back(child_id);
    }
    HashJoinSpillPartitionId split_child_id = parent_id.child(split_child_index);
    auto& split_child = build_partitions[split_child_id.key()];
    split_child.id = split_child_id;
    split_child.is_split = true;
    split_child.children.clear();
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        HashJoinSpillPartitionId grandchild_id = split_child_id.child(i);
        HashJoinSpillBuildPartition build_partition;
        build_partition.id = grandchild_id;
        build_partitions.try_emplace(grandchild_id.key(), std::move(build_partition));
        split_child.children.emplace_back(grandchild_id);
    }

    auto st = probe_operator->push(_helper.runtime_state.get(), &block, true);
    ASSERT_TRUE(st.ok()) << "push failed: " << st.to_string();

    size_t expected_level2_rows = 0;
    for (uint32_t i = 0; i < block.rows(); ++i) {
        auto id = find_partition_for_hash(hashes[i], build_partitions);
        if (id.level >= 2) {
            expected_level2_rows++;
        }
    }
    ASSERT_GT(expected_level2_rows, 0u);

    size_t actual_level2_rows = 0;
    for (const auto& [_, partition] : shared_state->probe_partitions) {
        if (partition.id.level >= 2) {
            actual_level2_rows += partition_rows(partition);
        }
    }
    ASSERT_EQ(actual_level2_rows, expected_level2_rows);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, SplitProbeSpillStreamsAcrossLevels) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor row_desc(_helper.runtime_state->desc_tbl(), {0});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, row_desc);

    std::vector<int32_t> values(4096);
    std::iota(values.begin(), values.end(), 0);
    vectorized::Block full_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(values);
    ASSERT_TRUE(local_state->_partitioner->do_partitioning(_helper.runtime_state.get(), &full_block)
                        .ok());

    const auto* hashes = local_state->_partitioner->get_channel_ids().get<uint32_t>();
    std::vector<int32_t> parent_values;
    for (uint32_t i = 0; i < full_block.rows(); ++i) {
        if (spill_partition_index(hashes[i], 0) == 0) {
            parent_values.emplace_back(values[i]);
        }
    }
    ASSERT_FALSE(parent_values.empty());

    vectorized::Block parent_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(parent_values);

    vectorized::SpillStreamSPtr parent_stream;
    auto st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), parent_stream, print_id(_helper.runtime_state->query_id()),
            "hash_probe_parent", probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
            std::numeric_limits<size_t>::max(), local_state->operator_profile());
    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();
    st = parent_stream->spill_block(_helper.runtime_state.get(), parent_block, false);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();
    st = parent_stream->spill_eof();
    ASSERT_TRUE(st) << "Spill eof failed: " << st.to_string();

    HashJoinSpillPartitionId root_id {0, 0};
    auto& root_partition = shared_state->probe_partitions[root_id.key()];
    root_partition.id = root_id;
    root_partition.spill_stream = parent_stream;

    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split failed: " << st.to_string();
    ASSERT_EQ(root_partition.spill_stream, nullptr);

    auto& partitions = shared_state->probe_partitions;
    auto parent_it = partitions.find(root_id.key());
    ASSERT_TRUE(parent_it != partitions.end());
    ASSERT_TRUE(parent_it->second.is_split);
    ASSERT_FALSE(parent_it->second.children.empty());

    HashJoinSpillPartitionId child_id {};
    bool found_child_stream = false;
    for (const auto& candidate : parent_it->second.children) {
        auto child_it = partitions.find(candidate.key());
        if (child_it != partitions.end() && child_it->second.spill_stream &&
            child_it->second.spill_stream->get_written_bytes() > 0) {
            child_id = candidate;
            found_child_stream = true;
            break;
        }
    }
    ASSERT_TRUE(found_child_stream);

    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split failed: " << st.to_string();

    auto child_it = partitions.find(child_id.key());
    ASSERT_TRUE(child_it != partitions.end());
    ASSERT_TRUE(child_it->second.is_split);
    ASSERT_FALSE(child_it->second.children.empty());
    ASSERT_EQ(child_it->second.spill_stream, nullptr);

    bool found_grandchild_stream = false;
    for (const auto& candidate : child_it->second.children) {
        auto grandchild_it = partitions.find(candidate.key());
        if (grandchild_it != partitions.end() && grandchild_it->second.spill_stream &&
            grandchild_it->second.spill_stream->get_written_bytes() > 0) {
            found_grandchild_stream = true;
            break;
        }
    }
    ASSERT_TRUE(found_grandchild_stream);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, SplitBuildSpillStreamsAcrossLevels) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor build_row_desc(_helper.runtime_state->desc_tbl(), {1});
    const auto& tnode = probe_operator->_tnode;
    local_state->_build_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].right}, build_row_desc);

    std::vector<int32_t> values(4096);
    std::iota(values.begin(), values.end(), 0);
    vectorized::Block full_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(values);
    ASSERT_TRUE(local_state->_build_partitioner
                        ->do_partitioning(_helper.runtime_state.get(), &full_block)
                        .ok());

    const auto* hashes = local_state->_build_partitioner->get_channel_ids().get<uint32_t>();
    std::vector<int32_t> parent_values;
    for (uint32_t i = 0; i < full_block.rows(); ++i) {
        if (spill_partition_index(hashes[i], 0) == 0) {
            parent_values.emplace_back(values[i]);
        }
    }
    ASSERT_FALSE(parent_values.empty());

    vectorized::Block parent_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(parent_values);

    vectorized::SpillStreamSPtr parent_stream;
    auto st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), parent_stream, print_id(_helper.runtime_state->query_id()),
            "hash_build_parent", probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
            std::numeric_limits<size_t>::max(), local_state->operator_profile());
    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();
    st = parent_stream->spill_block(_helper.runtime_state.get(), parent_block, false);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();
    st = parent_stream->spill_eof();
    ASSERT_TRUE(st) << "Spill eof failed: " << st.to_string();

    HashJoinSpillPartitionId root_id {0, 0};
    auto& build_partition = shared_state->build_partitions[root_id.key()];
    build_partition.spill_stream = parent_stream;
    st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split failed: " << st.to_string();
    ASSERT_EQ(build_partition.spill_stream, nullptr);

    auto& partitions = shared_state->build_partitions;
    auto parent_it = partitions.find(root_id.key());
    ASSERT_TRUE(parent_it != partitions.end());
    ASSERT_TRUE(parent_it->second.is_split);
    ASSERT_FALSE(parent_it->second.children.empty());

    HashJoinSpillPartitionId child_id {};
    bool found_child_stream = false;
    for (const auto& candidate : parent_it->second.children) {
        auto child_it = partitions.find(candidate.key());
        if (child_it != partitions.end() && child_it->second.spill_stream &&
            child_it->second.spill_stream->get_written_bytes() > 0) {
            child_id = candidate;
            found_child_stream = true;
            break;
        }
    }
    ASSERT_TRUE(found_child_stream);

    st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split failed: " << st.to_string();

    auto child_it = partitions.find(child_id.key());
    ASSERT_TRUE(child_it != partitions.end());
    ASSERT_TRUE(child_it->second.is_split);
    ASSERT_FALSE(child_it->second.children.empty());
    ASSERT_EQ(child_it->second.spill_stream, nullptr);

    bool found_grandchild_stream = false;
    for (const auto& candidate : child_it->second.children) {
        auto grandchild_it = partitions.find(candidate.key());
        if (grandchild_it != partitions.end() && grandchild_it->second.spill_stream &&
            grandchild_it->second.spill_stream->get_written_bytes() > 0) {
            found_grandchild_stream = true;
            break;
        }
    }
    ASSERT_TRUE(found_grandchild_stream);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, MultiLevelBuildSplitKeepsProbeAligned) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor probe_row_desc(_helper.runtime_state->desc_tbl(), {0});
    RowDescriptor build_row_desc(_helper.runtime_state->desc_tbl(), {1});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, probe_row_desc);
    local_state->_build_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].right}, build_row_desc);

    std::vector<int32_t> values(8192);
    std::iota(values.begin(), values.end(), 0);
    vectorized::Block probe_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(values);
    vectorized::Block build_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(values);

    ASSERT_TRUE(local_state->_build_partitioner
                        ->do_partitioning(_helper.runtime_state.get(), &build_block)
                        .ok());
    const auto* build_hashes = local_state->_build_partitioner->get_channel_ids().get<uint32_t>();

    const uint32_t base_index = spill_partition_index(build_hashes[0], 0);
    std::array<size_t, kHashJoinSpillFanout> child_counts {};
    for (uint32_t i = 0; i < build_block.rows(); ++i) {
        if (spill_partition_index(build_hashes[i], 0) != base_index) {
            continue;
        }
        child_counts[spill_partition_index(build_hashes[i], 1)]++;
    }

    uint32_t split_child_index = 0;
    bool found_child = false;
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        if (child_counts[i] > 0) {
            split_child_index = i;
            found_child = true;
            break;
        }
    }
    ASSERT_TRUE(found_child);

    auto& build_partitions = shared_state->build_partitions;
    HashJoinSpillPartitionId parent_id {0, base_index};
    auto& parent = build_partitions[parent_id.key()];
    parent.id = parent_id;
    parent.is_split = true;
    parent.children.clear();
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        HashJoinSpillPartitionId child_id = parent_id.child(i);
        HashJoinSpillBuildPartition build_partition;
        build_partition.id = child_id;
        build_partitions.try_emplace(child_id.key(), std::move(build_partition));
        parent.children.emplace_back(child_id);
    }
    HashJoinSpillPartitionId child_id = parent_id.child(split_child_index);
    auto& child = build_partitions[child_id.key()];
    child.id = child_id;
    child.is_split = true;
    child.children.clear();
    for (uint32_t i = 0; i < kHashJoinSpillFanout; ++i) {
        HashJoinSpillPartitionId grandchild_id = child_id.child(i);
        HashJoinSpillBuildPartition build_partition;
        build_partition.id = grandchild_id;
        build_partitions.try_emplace(grandchild_id.key(), std::move(build_partition));
        child.children.emplace_back(grandchild_id);
    }

    auto st = probe_operator->push(_helper.runtime_state.get(), &probe_block, true);
    ASSERT_TRUE(st.ok()) << "push failed: " << st.to_string();

    ASSERT_TRUE(
            local_state->_partitioner->do_partitioning(_helper.runtime_state.get(), &probe_block)
                    .ok());
    const auto* probe_hashes = local_state->_partitioner->get_channel_ids().get<uint32_t>();

    size_t expected_level2_rows = 0;
    for (uint32_t i = 0; i < probe_block.rows(); ++i) {
        auto id = find_partition_for_hash(probe_hashes[i], build_partitions);
        if (id.level >= 2) {
            expected_level2_rows++;
        }
    }
    ASSERT_GT(expected_level2_rows, 0u);

    size_t actual_level2_rows = 0;
    for (const auto& [_, partition] : shared_state->probe_partitions) {
        if (partition.id.level >= 2) {
            actual_level2_rows += partition_rows(partition);
        }
    }
    ASSERT_EQ(actual_level2_rows, expected_level2_rows);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, EndToEndProcessesLevelTwoPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    shared_state->is_spilled = true;
    local_state->_need_to_setup_internal_operators = true;
    local_state->_child_eos = true;

    HashJoinSpillPartitionId partition_id {2, 0};
    auto& build_partition = shared_state->build_partitions[partition_id.key()];
    build_partition.id = partition_id;
    build_partition.build_block = vectorized::MutableBlock::create_unique(
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));

    auto& probe_partition = shared_state->probe_partitions[partition_id.key()];
    probe_partition.id = partition_id;
    probe_partition.blocks.emplace_back(
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));

    local_state->_pending_partitions.emplace_back(partition_id);

    vectorized::Block output_block;
    bool eos = false;
    auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(output_block.rows(), 3);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, EndToEndProcessesPendingSplitPartitions) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    shared_state->is_spilled = true;
    local_state->_need_to_setup_internal_operators = true;
    local_state->_child_eos = true;
    local_state->_partition_cursor = PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT;

    HashJoinSpillPartitionId first_id {2, 0};
    HashJoinSpillPartitionId second_id {2, 1};

    auto& first_build = shared_state->build_partitions[first_id.key()];
    first_build.id = first_id;
    first_build.build_block = vectorized::MutableBlock::create_unique(
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));

    auto& first_probe = shared_state->probe_partitions[first_id.key()];
    first_probe.id = first_id;
    first_probe.blocks.emplace_back(
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3}));

    auto& second_build = shared_state->build_partitions[second_id.key()];
    second_build.id = second_id;
    second_build.build_block = vectorized::MutableBlock::create_unique(
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({7, 8}));

    auto& second_probe = shared_state->probe_partitions[second_id.key()];
    second_probe.id = second_id;
    second_probe.blocks.emplace_back(
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({7, 8}));

    // Process pending split partitions in order.
    local_state->_pending_partitions.emplace_back(first_id);
    local_state->_pending_partitions.emplace_back(second_id);

    vectorized::Block output_block;
    bool eos = false;
    auto st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(output_block.rows(), 3);

    output_block.clear();
    st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(output_block.rows(), 2);

    bool seen_eos = false;
    for (uint32_t i = 0; i < PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT + 1; ++i) {
        output_block.clear();
        st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
        ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
        if (eos) {
            seen_eos = true;
            break;
        }
    }
    ASSERT_TRUE(seen_eos);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, EndToEndProcessesAutoSplitChildPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor probe_row_desc(_helper.runtime_state->desc_tbl(), {0});
    RowDescriptor build_row_desc(_helper.runtime_state->desc_tbl(), {1});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, probe_row_desc);
    local_state->_build_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].right}, build_row_desc);

    vectorized::Block build_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5});

    HashJoinSpillPartitionId build_partition_id {0, 0};
    auto& build_partition = shared_state->build_partitions[build_partition_id.key()];
    ASSERT_EQ(build_partition.id.level, build_partition_id.level);
    ASSERT_EQ(build_partition.id.path, build_partition_id.path);
    build_partition.build_block = vectorized::MutableBlock::create_unique(std::move(build_block));

    vectorized::Block probe_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5});
    HashJoinSpillPartitionId root_id {0, 0};
    auto& root_partition = shared_state->probe_partitions[root_id.key()];
    root_partition.id = root_id;
    root_partition.accumulating_block =
            vectorized::MutableBlock::create_unique(std::move(probe_block));

    auto st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state,
                                                     root_id);
    ASSERT_TRUE(st.ok()) << "split build failed: " << st.to_string();
    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split probe failed: " << st.to_string();
    ASSERT_FALSE(local_state->_pending_partitions.empty());

    auto child_id = local_state->_pending_partitions.front();
    auto child_it = shared_state->probe_partitions.find(child_id.key());
    ASSERT_TRUE(child_it != shared_state->probe_partitions.end());
    ASSERT_FALSE(child_it->second.blocks.empty());
    const size_t expected_rows = child_it->second.blocks.front().rows();

    shared_state->is_spilled = true;
    local_state->_need_to_setup_internal_operators = true;
    local_state->_child_eos = true;
    local_state->_base_partition_split.assign(PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
                                              true);

    vectorized::Block output_block;
    bool eos = false;
    st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(output_block.rows(), expected_rows);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, EndToEndProcessesAutoSplitGrandchildPartition) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor probe_row_desc(_helper.runtime_state->desc_tbl(), {0});
    RowDescriptor build_row_desc(_helper.runtime_state->desc_tbl(), {1});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, probe_row_desc);
    local_state->_build_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].right}, build_row_desc);

    vectorized::Block build_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5, 6});
    HashJoinSpillPartitionId build_partition_id {0, 0};
    auto& build_partition = shared_state->build_partitions[build_partition_id.key()];
    ASSERT_EQ(build_partition.id.level, build_partition_id.level);
    ASSERT_EQ(build_partition.id.path, build_partition_id.path);
    build_partition.build_block = vectorized::MutableBlock::create_unique(std::move(build_block));

    vectorized::Block probe_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5, 6});
    HashJoinSpillPartitionId root_id {0, 0};
    auto& root_partition = shared_state->probe_partitions[root_id.key()];
    root_partition.id = root_id;
    root_partition.accumulating_block =
            vectorized::MutableBlock::create_unique(std::move(probe_block));

    auto st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state,
                                                     root_id);
    ASSERT_TRUE(st.ok()) << "split build failed: " << st.to_string();
    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split probe failed: " << st.to_string();
    ASSERT_FALSE(local_state->_pending_partitions.empty());

    auto child_id = local_state->_pending_partitions.front();
    auto child_it = shared_state->build_partitions.find(child_id.key());
    ASSERT_TRUE(child_it != shared_state->build_partitions.end());

    st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split build failed: " << st.to_string();
    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split probe failed: " << st.to_string();
    ASSERT_FALSE(local_state->_pending_partitions.empty());

    HashJoinSpillPartitionId grandchild_id {};
    bool found_grandchild = false;
    for (const auto& candidate : local_state->_pending_partitions) {
        if (candidate.level == 2) {
            grandchild_id = candidate;
            found_grandchild = true;
            break;
        }
    }
    ASSERT_TRUE(found_grandchild);

    auto grandchild_it = shared_state->probe_partitions.find(grandchild_id.key());
    ASSERT_TRUE(grandchild_it != shared_state->probe_partitions.end());
    ASSERT_FALSE(grandchild_it->second.blocks.empty());
    const size_t expected_rows = grandchild_it->second.blocks.front().rows();

    shared_state->is_spilled = true;
    local_state->_need_to_setup_internal_operators = true;
    local_state->_child_eos = true;
    local_state->_partition_cursor = PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT;

    vectorized::Block output_block;
    bool eos = false;
    local_state->_pending_partitions.clear();
    local_state->_pending_partitions.emplace_back(grandchild_id);
    st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_FALSE(eos);
    ASSERT_EQ(output_block.rows(), expected_rows);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, EndToEndProcessesSplitGrandchildFromProbeSpillStream) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor probe_row_desc(_helper.runtime_state->desc_tbl(), {0});
    RowDescriptor build_row_desc(_helper.runtime_state->desc_tbl(), {1});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, probe_row_desc);
    local_state->_build_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].right}, build_row_desc);

    vectorized::Block build_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1, 2, 3, 4, 5, 6});
    HashJoinSpillPartitionId build_partition_id {0, 0};
    auto& build_partition = shared_state->build_partitions[build_partition_id.key()];
    ASSERT_EQ(build_partition.id.level, build_partition_id.level);
    ASSERT_EQ(build_partition.id.path, build_partition_id.path);
    build_partition.build_block = vectorized::MutableBlock::create_unique(std::move(build_block));

    std::vector<int32_t> values(4096);
    std::iota(values.begin(), values.end(), 0);
    vectorized::Block full_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(values);
    ASSERT_TRUE(local_state->_partitioner->do_partitioning(_helper.runtime_state.get(), &full_block)
                        .ok());

    const auto* hashes = local_state->_partitioner->get_channel_ids().get<uint32_t>();
    std::vector<int32_t> parent_values;
    for (uint32_t i = 0; i < full_block.rows(); ++i) {
        if (spill_partition_index(hashes[i], 0) == 0) {
            parent_values.emplace_back(values[i]);
        }
    }
    ASSERT_FALSE(parent_values.empty());

    vectorized::Block parent_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(parent_values);

    vectorized::SpillStreamSPtr parent_stream;
    auto st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), parent_stream, print_id(_helper.runtime_state->query_id()),
            "hash_probe_parent", probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
            std::numeric_limits<size_t>::max(), local_state->operator_profile());
    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();
    st = parent_stream->spill_block(_helper.runtime_state.get(), parent_block, false);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();
    st = parent_stream->spill_eof();
    ASSERT_TRUE(st) << "Spill eof failed: " << st.to_string();

    HashJoinSpillPartitionId root_id {0, 0};
    auto& root_partition = shared_state->probe_partitions[root_id.key()];
    root_partition.id = root_id;
    root_partition.spill_stream = parent_stream;

    st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split build failed: " << st.to_string();
    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split probe failed: " << st.to_string();

    HashJoinSpillPartitionId child_id {};
    bool found_child = false;
    auto& partitions = shared_state->probe_partitions;
    auto root_it = partitions.find(root_id.key());
    ASSERT_TRUE(root_it != partitions.end());
    for (const auto& candidate : root_it->second.children) {
        auto child_it = partitions.find(candidate.key());
        if (child_it != partitions.end() && child_it->second.spill_stream &&
            child_it->second.spill_stream->get_written_bytes() > 0) {
            child_id = candidate;
            found_child = true;
            break;
        }
    }
    ASSERT_TRUE(found_child);

    st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split build failed: " << st.to_string();
    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split probe failed: " << st.to_string();

    HashJoinSpillPartitionId grandchild_id {};
    bool found_grandchild = false;
    auto child_it = partitions.find(child_id.key());
    ASSERT_TRUE(child_it != partitions.end());
    for (const auto& candidate : child_it->second.children) {
        auto grandchild_it = partitions.find(candidate.key());
        if (grandchild_it != partitions.end() && grandchild_it->second.spill_stream &&
            grandchild_it->second.spill_stream->get_written_bytes() > 0) {
            grandchild_id = candidate;
            found_grandchild = true;
            break;
        }
    }
    ASSERT_TRUE(found_grandchild);

    auto& grandchild_build_partition = shared_state->build_partitions[grandchild_id.key()];
    grandchild_build_partition.id = grandchild_id;
    if (!grandchild_build_partition.build_block) {
        grandchild_build_partition.build_block = vectorized::MutableBlock::create_unique(
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1}));
    }

    shared_state->is_spilled = true;
    local_state->_need_to_setup_internal_operators = true;
    local_state->_child_eos = true;
    local_state->_partition_cursor = PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT;
    local_state->_pending_partitions.clear();
    local_state->_pending_partitions.emplace_back(grandchild_id);

    vectorized::Block output_block;
    bool eos = false;
    st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_FALSE(eos);
}

TEST_F(PartitionedHashJoinProbeOperatorTest,
       EndToEndProcessesSplitGrandchildFromBuildAndProbeSpillStreams) {
    auto [probe_operator, sink_operator] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    RowDescriptor probe_row_desc(_helper.runtime_state->desc_tbl(), {0});
    RowDescriptor build_row_desc(_helper.runtime_state->desc_tbl(), {1});
    const auto& tnode = probe_operator->_tnode;
    local_state->_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].left}, probe_row_desc);
    local_state->_build_partitioner = create_spill_partitioner(
            _helper.runtime_state.get(), PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT,
            {tnode.hash_join_node.eq_join_conjuncts[0].right}, build_row_desc);

    std::vector<int32_t> values(4096);
    std::iota(values.begin(), values.end(), 0);
    vectorized::Block build_full_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(values);
    ASSERT_TRUE(local_state->_build_partitioner
                        ->do_partitioning(_helper.runtime_state.get(), &build_full_block)
                        .ok());
    const auto* build_hashes = local_state->_build_partitioner->get_channel_ids().get<uint32_t>();

    std::vector<int32_t> build_parent_values;
    for (uint32_t i = 0; i < build_full_block.rows(); ++i) {
        if (spill_partition_index(build_hashes[i], 0) == 0) {
            build_parent_values.emplace_back(values[i]);
        }
    }
    ASSERT_FALSE(build_parent_values.empty());
    vectorized::Block build_parent_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(build_parent_values);

    vectorized::SpillStreamSPtr build_parent_stream;
    auto st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), build_parent_stream,
            print_id(_helper.runtime_state->query_id()), "hash_build_parent",
            probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
            std::numeric_limits<size_t>::max(), local_state->operator_profile());
    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();
    st = build_parent_stream->spill_block(_helper.runtime_state.get(), build_parent_block, false);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();
    st = build_parent_stream->spill_eof();
    ASSERT_TRUE(st) << "Spill eof failed: " << st.to_string();

    HashJoinSpillPartitionId build_partition_id {0, 0};
    auto& build_partition = shared_state->build_partitions[build_partition_id.key()];
    build_partition.spill_stream = build_parent_stream;

    vectorized::Block probe_full_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(values);
    ASSERT_TRUE(local_state->_partitioner
                        ->do_partitioning(_helper.runtime_state.get(), &probe_full_block)
                        .ok());
    const auto* probe_hashes = local_state->_partitioner->get_channel_ids().get<uint32_t>();

    std::vector<int32_t> probe_parent_values;
    for (uint32_t i = 0; i < probe_full_block.rows(); ++i) {
        if (spill_partition_index(probe_hashes[i], 0) == 0) {
            probe_parent_values.emplace_back(values[i]);
        }
    }
    ASSERT_FALSE(probe_parent_values.empty());
    vectorized::Block probe_parent_block =
            vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>(probe_parent_values);

    vectorized::SpillStreamSPtr probe_parent_stream;
    st = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _helper.runtime_state.get(), probe_parent_stream,
            print_id(_helper.runtime_state->query_id()), "hash_probe_parent",
            probe_operator->node_id(), std::numeric_limits<int32_t>::max(),
            std::numeric_limits<size_t>::max(), local_state->operator_profile());
    ASSERT_TRUE(st) << "Register spill stream failed: " << st.to_string();
    st = probe_parent_stream->spill_block(_helper.runtime_state.get(), probe_parent_block, false);
    ASSERT_TRUE(st) << "Spill block failed: " << st.to_string();
    st = probe_parent_stream->spill_eof();
    ASSERT_TRUE(st) << "Spill eof failed: " << st.to_string();

    HashJoinSpillPartitionId root_id {0, 0};
    auto& root_probe_partition = shared_state->probe_partitions[root_id.key()];
    root_probe_partition.id = root_id;
    root_probe_partition.spill_stream = probe_parent_stream;

    st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split build failed: " << st.to_string();
    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state, root_id);
    ASSERT_TRUE(st.ok()) << "split probe failed: " << st.to_string();

    HashJoinSpillPartitionId child_id {};
    bool found_child = false;
    auto& probe_partitions = shared_state->probe_partitions;
    auto root_it = probe_partitions.find(root_id.key());
    ASSERT_TRUE(root_it != probe_partitions.end());
    for (const auto& candidate : root_it->second.children) {
        auto child_it = probe_partitions.find(candidate.key());
        if (child_it != probe_partitions.end() && child_it->second.spill_stream &&
            child_it->second.spill_stream->get_written_bytes() > 0) {
            child_id = candidate;
            found_child = true;
            break;
        }
    }
    ASSERT_TRUE(found_child);

    st = probe_operator->_split_build_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split build failed: " << st.to_string();
    st = probe_operator->_split_probe_partition(_helper.runtime_state.get(), *local_state,
                                                child_id);
    ASSERT_TRUE(st.ok()) << "split probe failed: " << st.to_string();

    HashJoinSpillPartitionId grandchild_id {};
    bool found_grandchild = false;
    auto child_it = probe_partitions.find(child_id.key());
    ASSERT_TRUE(child_it != probe_partitions.end());
    for (const auto& candidate : child_it->second.children) {
        auto grandchild_it = probe_partitions.find(candidate.key());
        if (grandchild_it != probe_partitions.end() && grandchild_it->second.spill_stream &&
            grandchild_it->second.spill_stream->get_written_bytes() > 0) {
            grandchild_id = candidate;
            found_grandchild = true;
            break;
        }
    }
    ASSERT_TRUE(found_grandchild);

    auto build_it = shared_state->build_partitions.find(grandchild_id.key());
    ASSERT_TRUE(build_it != shared_state->build_partitions.end());
    if (!build_it->second.build_block) {
        build_it->second.build_block = vectorized::MutableBlock::create_unique(
                vectorized::ColumnHelper::create_block<vectorized::DataTypeInt32>({1}));
    }

    shared_state->is_spilled = true;
    local_state->_need_to_setup_internal_operators = true;
    local_state->_child_eos = true;
    local_state->_partition_cursor = PartitionedHashJoinTestHelper::TEST_PARTITION_COUNT;
    local_state->_pending_partitions.clear();
    local_state->_pending_partitions.emplace_back(grandchild_id);

    vectorized::Block output_block;
    bool eos = false;
    st = probe_operator->get_block(_helper.runtime_state.get(), &output_block, &eos);
    ASSERT_TRUE(st.ok()) << "get_block failed: " << st.to_string();
    ASSERT_FALSE(eos);
}

TEST_F(PartitionedHashJoinProbeOperatorTest, Other) {
    auto [probe_operator, _] = _helper.create_operators();

    std::shared_ptr<MockPartitionedHashJoinSharedState> shared_state;
    auto local_state = _helper.create_probe_local_state(_helper.runtime_state.get(),
                                                        probe_operator.get(), shared_state);

    local_state->_shared_state->is_spilled = true;
    ASSERT_FALSE(probe_operator->_should_revoke_memory(_helper.runtime_state.get()));

    auto st = probe_operator->_revoke_memory(_helper.runtime_state.get());
    ASSERT_TRUE(st.ok()) << "Revoke memory failed: " << st.to_string();
}

} // namespace doris::pipeline
