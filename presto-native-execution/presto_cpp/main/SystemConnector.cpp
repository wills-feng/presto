/*
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
#include "presto_cpp/main/SystemConnector.h"
#include "presto_cpp/main/PrestoTask.h"
#include "presto_cpp/main/TaskManager.h"

#include "velox/type/Timestamp.h"

namespace facebook::presto {

using namespace velox;

namespace {

static const std::string kRuntimeSchema = "runtime";
static const std::string kTasksTable = "tasks";

static std::vector<std::string> kTaskColumnNames = {
    "node_id",
    "task_id",
    "stage_execution_id",
    "stage_id",
    "query_id",
    "state",
    "splits",
    "queued_splits",
    "running_splits",
    "completed_splits",
    "split_scheduled_time_ms",
    "split_cpu_time_ms",
    "split_blocked_time_ms",
    "raw_input_bytes",
    "raw_input_rows",
    "processed_input_bytes",
    "processed_input_rows",
    "output_bytes",
    "output_rows",
    "physical_written_bytes",
    "created",
    "start",
    "last_heartbeat",
    "end"};

static std::vector<velox::TypePtr> kTaskColumnTypes = {
    velox::VARCHAR(),   velox::VARCHAR(),   velox::VARCHAR(),
    velox::VARCHAR(),   velox::VARCHAR(),   velox::VARCHAR(),
    velox::BIGINT(),    velox::BIGINT(),    velox::BIGINT(),
    velox::BIGINT(),    velox::BIGINT(),    velox::BIGINT(),
    velox::BIGINT(),    velox::BIGINT(),    velox::BIGINT(),
    velox::BIGINT(),    velox::BIGINT(),    velox::BIGINT(),
    velox::BIGINT(),    velox::BIGINT(),    velox::TIMESTAMP(),
    velox::TIMESTAMP(), velox::TIMESTAMP(), velox::TIMESTAMP()};
} // namespace

const velox::RowTypePtr& SystemTableHandle::taskSchema() {
  static const RowTypePtr kTaskSchema =
      ROW(std::move(kTaskColumnNames), std::move(kTaskColumnTypes));
  return kTaskSchema;
}

SystemTableHandle::SystemTableHandle(
    std::string connectorId,
    std::string schemaName,
    std::string tableName)
    : ConnectorTableHandle(std::move(connectorId)),
      schemaName_(std::move(schemaName)),
      tableName_(std::move(tableName)) {
  VELOX_USER_CHECK_EQ(
      schemaName_,
      kRuntimeSchema,
      "SystemConnector supports only runtime schema");
  VELOX_USER_CHECK_EQ(
      tableName_, kTasksTable, "SystemConnector supports only tasks table");
}

std::string SystemTableHandle::toString() const {
  return fmt::format("schema: {} table: {}", schemaName_, tableName_);
}

SystemDataSource::SystemDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    const TaskManager* taskManager,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : taskManager_(taskManager), pool_(pool) {
  auto systemTableHandle =
      std::dynamic_pointer_cast<SystemTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      systemTableHandle,
      "TableHandle must be an instance of SystemTableHandle");

  taskTableHandle_ = systemTableHandle;
  outputColumnMappings_.reserve(outputType->names().size());
  auto taskSchema = taskTableHandle_->taskSchema();
  for (const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column '{}'",
        outputName);

    auto handle = std::dynamic_pointer_cast<SystemColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of SystemColumnHandle "
        "for '{}' on table '{}'",
        handle->name());

    auto columnIndex = taskSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(columnIndex.has_value());
    outputColumnMappings_.push_back(columnIndex.value());
  }

  outputType_ = outputType;
  // taskTableResult_ = std::dynamic_pointer_cast<RowVector>(
  //     BaseVector::create(taskSchema, 0, pool_));
}

void SystemDataSource::addSplit(
    std::shared_ptr<connector::ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      currentSplit_,
      "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<SystemSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for SystemDataSource.");
}

SystemDataSource::TaskColumnEnum SystemDataSource::getTaskColumnEnum(
    int32_t taskColumn) {
  switch (taskColumn) {
    case 0:
      return TaskColumnEnum::kNodeId;
    case 1:
      return TaskColumnEnum::kTaskId;
    case 2:
      return TaskColumnEnum::kStageExecutionId;
    case 3:
      return TaskColumnEnum::kStageId;
    case 4:
      return TaskColumnEnum::kQueryId;
    case 5:
      return TaskColumnEnum::kState;
    case 6:
      return TaskColumnEnum::kSplits;
    case 7:
      return TaskColumnEnum::kQueuedSplits;
    case 8:
      return TaskColumnEnum::kRunningSplits;
    case 9:
      return TaskColumnEnum::kCompletedSplits;
    case 10:
      return TaskColumnEnum::kSplitScheduledTimeMs;
    case 11:
      return TaskColumnEnum::kSplitCpuTimeMs;
    case 12:
      return TaskColumnEnum::kSplitBlockedTimeMs;
    case 13:
      return TaskColumnEnum::kRawInputBytes;
    case 14:
      return TaskColumnEnum::kRawInputRows;
    case 15:
      return TaskColumnEnum::kProcessedInputBytes;
    case 16:
      return TaskColumnEnum::kProcessedInputRows;
    case 17:
      return TaskColumnEnum::kOutputBytes;
    case 18:
      return TaskColumnEnum::kOutputRows;
    case 19:
      return TaskColumnEnum::kPhysicalWrittenBytes;
    case 20:
      return TaskColumnEnum::kCreated;
    case 21:
      return TaskColumnEnum::kStart;
    case 22:
      return TaskColumnEnum::kLastHeartBeat;
    case 23:
      return TaskColumnEnum::kEnd;
    default:
      VELOX_UNREACHABLE("Unknown TaskColumn");
  }
}

#define SET_TASK_COLUMN(value)            \
  int j = 0;                              \
  for (const auto& taskEntry : taskMap) { \
    auto task = taskEntry.second;         \
    auto taskInfo = taskInfos[j];         \
    flat->set(j, value);                  \
    j++;                                  \
  }

#define SET_TASK_FMT_COLUMN(value)        \
  int j = 0;                              \
  std::string temp;                       \
  for (const auto& taskEntry : taskMap) { \
    auto task = taskEntry.second;         \
    auto taskInfo = taskInfos[j];         \
    temp = fmt::format("{}", value);      \
    flat->set(j, StringView(temp));       \
    j++;                                  \
  }

std::optional<RowVectorPtr> SystemDataSource::getTaskResults() {
  TaskMap taskMap = taskManager_->tasks();
  auto numRows = taskMap.size();

  std::vector<protocol::TaskInfo> taskInfos;
  taskInfos.reserve(numRows);
  for (const auto& taskEntry : taskMap) {
    taskInfos.push_back(taskEntry.second->updateInfo());
  }

  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numRows, pool_));

  auto toMillis = [](int64_t nanos) -> int64_t { return nanos / 1'000'000; };
  for (auto i = 0; i < outputColumnMappings_.size(); i++) {
    result->childAt(i)->resize(numRows);
    auto taskColumn = outputColumnMappings_.at(i);
    auto taskEnum = getTaskColumnEnum(taskColumn);
    switch (taskEnum) {
      case TaskColumnEnum::kNodeId: {
        auto flat = result->childAt(i)->as<FlatVector<StringView>>();
        SET_TASK_COLUMN(StringView(taskInfo.nodeId));
        break;
      }

      case TaskColumnEnum::kTaskId: {
        auto flat = result->childAt(i)->as<FlatVector<StringView>>();
        SET_TASK_COLUMN(StringView(taskInfo.taskId));
        break;
      }

      case TaskColumnEnum::kStageExecutionId: {
        auto flat = result->childAt(i)->as<FlatVector<StringView>>();
        SET_TASK_FMT_COLUMN(task->id.stageExecutionId());
        break;
      }
      case TaskColumnEnum::kStageId: {
        auto flat = result->childAt(i)->as<FlatVector<StringView>>();
        SET_TASK_FMT_COLUMN(task->id.stageId());
        break;
      }
      case TaskColumnEnum::kQueryId: {
        auto flat = result->childAt(i)->as<FlatVector<StringView>>();
        SET_TASK_FMT_COLUMN(task->id.queryId());
        break;
      }

      case TaskColumnEnum::kState: {
        auto flat = result->childAt(i)->as<FlatVector<StringView>>();
        SET_TASK_FMT_COLUMN(json(taskInfo.taskStatus.state).dump());
        break;
      }

      case TaskColumnEnum::kSplits: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.totalDrivers);
        break;
      }

      case TaskColumnEnum::kQueuedSplits: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.queuedDrivers);
        break;
      }

      case TaskColumnEnum::kRunningSplits: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.runningDrivers);
        break;
      }

      case TaskColumnEnum::kCompletedSplits: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.completedDrivers);
        break;
      }

      case TaskColumnEnum::kSplitScheduledTimeMs: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(toMillis(taskInfo.stats.totalScheduledTimeInNanos));
        break;
      }

      case TaskColumnEnum::kSplitCpuTimeMs: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(toMillis(taskInfo.stats.totalCpuTimeInNanos));
        break;
      }

      case TaskColumnEnum::kSplitBlockedTimeMs: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(toMillis(taskInfo.stats.totalBlockedTimeInNanos));
        break;
      }

      case TaskColumnEnum::kRawInputBytes: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.rawInputDataSizeInBytes);
        break;
      }

      case TaskColumnEnum::kRawInputRows: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.rawInputPositions);
        break;
      }

      case TaskColumnEnum::kProcessedInputBytes: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.processedInputDataSizeInBytes);
        break;
      }

      case TaskColumnEnum::kProcessedInputRows: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.processedInputPositions);
        break;
      }

      case TaskColumnEnum::kOutputBytes: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.outputDataSizeInBytes);
        break;
      }

      case TaskColumnEnum::kOutputRows: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.outputPositions);
        break;
      }

      case TaskColumnEnum::kPhysicalWrittenBytes: {
        auto flat = result->childAt(i)->as<FlatVector<int64_t>>();
        SET_TASK_COLUMN(taskInfo.stats.physicalWrittenDataSizeInBytes);
        break;
      }

      case TaskColumnEnum::kCreated: {
        auto flat = result->childAt(i)->as<FlatVector<Timestamp>>();
        SET_TASK_COLUMN(velox::Timestamp::fromMillis(task->createTime));
        break;
      }

      case TaskColumnEnum::kStart: {
        auto flat = result->childAt(i)->as<FlatVector<Timestamp>>();
        SET_TASK_COLUMN(velox::Timestamp::fromMillis(task->firstStartTime));
        break;
      }

      case TaskColumnEnum::kLastHeartBeat: {
        auto flat = result->childAt(i)->as<FlatVector<Timestamp>>();
        SET_TASK_COLUMN(velox::Timestamp::fromMillis(task->lastHeartbeatMs));
        break;
      }

      case TaskColumnEnum::kEnd: {
        auto flat = result->childAt(i)->as<FlatVector<Timestamp>>();
        SET_TASK_COLUMN(velox::Timestamp::fromMillis(task->lastEndTime));
        break;
      }
    }
  }
  return result;
}

std::optional<RowVectorPtr> SystemDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  if (!currentSplit_) {
    return nullptr;
  }

  auto result = getTaskResults();

  currentSplit_ = nullptr;

  return result;
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<SystemConnectorFactory>())

std::unique_ptr<velox::connector::ConnectorSplit>
SystemPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit) const {
  auto systemSplit = dynamic_cast<const protocol::SystemSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      systemSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<SystemSplit>(
      catalogId,
      systemSplit->tableHandle.schemaName,
      systemSplit->tableHandle.tableName);
}

std::unique_ptr<velox::connector::ColumnHandle>
SystemPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto systemColumn = dynamic_cast<const protocol::SystemColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      systemColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<SystemColumnHandle>(systemColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
SystemPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser,
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  auto systemLayout =
      std::dynamic_pointer_cast<const protocol::SystemTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      systemLayout, "Unexpected table handle type {}", tableHandle.connectorId);
  return std::make_unique<SystemTableHandle>(
      tableHandle.connectorId,
      systemLayout->table.schemaName,
      systemLayout->table.tableName);
}

std::unique_ptr<protocol::ConnectorProtocol>
SystemPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::SystemConnectorProtocol>();
}

} // namespace facebook::presto