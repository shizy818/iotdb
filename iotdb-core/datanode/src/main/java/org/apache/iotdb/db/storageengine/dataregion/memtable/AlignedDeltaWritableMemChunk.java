package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public class AlignedDeltaWritableMemChunk implements IWritableMemChunk {
  private long maxTime;
  private final Map<String, Integer> measurementIndexMap;
  private final List<IMeasurementSchema> schemaList;
  private final AlignedTVList stableList;
  private final AlignedTVList deltaList;
  private final DeltaIndexTree deltaTree;
  private final boolean ignoreAllNullRows;

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long TARGET_CHUNK_SIZE = CONFIG.getTargetChunkSize();
  private long maxNumberOfPointsInChunk = CONFIG.getTargetChunkPointNum();
  private final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  private static final Logger LOGGER = LoggerFactory.getLogger(AlignedDeltaWritableMemChunk.class);

  public AlignedDeltaWritableMemChunk(List<IMeasurementSchema> schemaList, boolean isTableModel) {
    this.measurementIndexMap = new LinkedHashMap<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
      dataTypeList.add(schemaList.get(i).getType());
    }
    this.stableList = AlignedTVList.newAlignedList(dataTypeList);
    this.deltaList = AlignedTVList.newAlignedList(new ArrayList<>(dataTypeList));
    this.deltaTree = new DeltaIndexTree(DELTA_TREE_DEGREE);
    this.maxTime = Long.MIN_VALUE;
    this.ignoreAllNullRows = !isTableModel;
  }

  private AlignedDeltaWritableMemChunk(
      List<IMeasurementSchema> schemaList,
      AlignedTVList stableList,
      AlignedTVList deltaList,
      DeltaIndexTree deltaTree,
      boolean isTableModel) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
    }
    this.stableList = stableList;
    this.deltaList = deltaList;
    this.deltaTree = deltaTree;
    this.ignoreAllNullRows = !isTableModel;
  }

  public Set<String> getAllMeasurements() {
    return measurementIndexMap.keySet();
  }

  public List<TSDataType> getTsDataTypes() {
    return schemaList.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
  }

  public boolean containsMeasurement(String measurementId) {
    return measurementIndexMap.containsKey(measurementId);
  }

  @Override
  public void putLong(long t, long v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putInt(long t, int v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putFloat(long t, float v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putDouble(long t, double v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBinary(long t, Binary v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBoolean(long t, boolean v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putAlignedRow(long t, Object[] v) {
    if (t >= maxTime) {
      stableList.putAlignedValue(t, v);
      maxTime = t;
    } else {
      int stableId = stableList.binarySearch(t);
      deltaList.putAlignedValue(t, v);
      int deltaId = deltaList.rowCount() - 1;
      deltaTree.insert(t, stableId, deltaId);
    }
  }

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  private int findSplitIndex(long[] t, int start, int end) {
    int index = start;
    for (int i = start; i < end; i++) {
      if (t[i] >= maxTime) {
        maxTime = t[i];
      } else {
        index = i;
      }
    }
    return index;
  }

  @Override
  public void putAlignedTablet(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    int splitIndex = findSplitIndex(t, start, end);
    // delta part
    int deltaId = deltaList.rowCount();
    deltaList.putAlignedValues(t, v, bitMaps, start, splitIndex, results);
    for (int i = start; i < splitIndex; i++) {
      int stableId = stableList.binarySearch(t[i]);
      deltaTree.insert(t[i], stableId, deltaId + i - start);
    }
    stableList.putAlignedValues(t, v, bitMaps, splitIndex, end, results);
  }

  @Override
  public void writeNonAlignedPoint(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void writeNonAlignedTablet(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public void writeAlignedPoints(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    Object[] reorderedValue =
        checkAndReorderColumnValuesInInsertPlan(schemaList, objectValue, null).left;
    putAlignedRow(insertTime, reorderedValue);
  }

  @Override
  public void writeAlignedTablet(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
    Pair<Object[], BitMap[]> pair =
        checkAndReorderColumnValuesInInsertPlan(schemaList, valueList, bitMaps);
    Object[] reorderedColumnValues = pair.left;
    BitMap[] reorderedBitMaps = pair.right;
    putAlignedTablet(times, reorderedColumnValues, reorderedBitMaps, start, end, results);
  }

  /**
   * Check metadata of columns and return array that mapping existed metadata to index of data
   * column.
   *
   * @param schemaListInInsertPlan Contains all existed schema in InsertPlan. If some timeseries
   *     have been deleted, there will be null in its slot.
   * @return columnIndexArray: schemaList[i] is schema of columns[columnIndexArray[i]]
   */
  private Pair<Object[], BitMap[]> checkAndReorderColumnValuesInInsertPlan(
      List<IMeasurementSchema> schemaListInInsertPlan, Object[] columnValues, BitMap[] bitMaps) {
    Object[] reorderedColumnValues = new Object[schemaList.size()];
    BitMap[] reorderedBitMaps = bitMaps == null ? null : new BitMap[schemaList.size()];
    for (int i = 0; i < schemaListInInsertPlan.size(); i++) {
      IMeasurementSchema measurementSchema = schemaListInInsertPlan.get(i);
      if (measurementSchema != null) {
        Integer index = this.measurementIndexMap.get(measurementSchema.getMeasurementName());
        // Index is null means this measurement was not in this AlignedTVList before.
        // We need to extend a new column in AlignedMemChunk and AlignedTVList.
        // And the reorderedColumnValues should extend one more column for the new measurement
        if (index == null) {
          index = measurementIndexMap.size();
          this.measurementIndexMap.put(schemaListInInsertPlan.get(i).getMeasurementName(), index);
          this.schemaList.add(schemaListInInsertPlan.get(i));
          this.stableList.extendColumn(schemaListInInsertPlan.get(i).getType());
          this.deltaList.extendColumn(schemaListInInsertPlan.get(i).getType());
          reorderedColumnValues =
              Arrays.copyOf(reorderedColumnValues, reorderedColumnValues.length + 1);
          if (reorderedBitMaps != null) {
            reorderedBitMaps = Arrays.copyOf(reorderedBitMaps, reorderedBitMaps.length + 1);
          }
        }
        reorderedColumnValues[index] = columnValues[i];
        if (bitMaps != null) {
          reorderedBitMaps[index] = bitMaps[i];
        }
      }
    }
    return new Pair<>(reorderedColumnValues, reorderedBitMaps);
  }

  @Override
  public long count() {
    if (!ignoreAllNullRows && measurementIndexMap.isEmpty()) {
      return rowCount();
    }
    return rowCount() * measurementIndexMap.size();
  }

  @Override
  public long rowCount() {
    return alignedListSize();
  }

  public int alignedListSize() {
    return stableList.rowCount() + deltaList.rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return null;
  }

  @Override
  public long getMaxTime() {
    return maxTime;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery() {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE);
  }

  @Override
  public synchronized TVList getSortedTvListForQuery(
      List<IMeasurementSchema> schemaList, boolean ignoreAllNullRows) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE);
  }

  @Override
  public void sortTvListForFlush() {
    // do nothing
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int delete1 = stableList.delete(lowerBound, upperBound);
    int delete2 = deltaList.delete(lowerBound, upperBound);
    return delete1 + delete2;
  }

  public int deleteTime(long lowerBound, long upperBound) {
    int delete1 = stableList.deleteTime(lowerBound, upperBound);
    int delete2 = deltaList.deleteTime(lowerBound, upperBound);
    return delete1 + delete2;
  }

  public Pair<Integer, Boolean> deleteDataFromAColumn(
      long lowerBound, long upperBound, String measurementId) {
    Pair<Integer, Boolean> delete1 =
        stableList.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
    Pair<Integer, Boolean> delete2 =
        deltaList.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
    return new Pair<>(delete1.left + delete2.left, delete1.right && delete2.right);
  }

  public void removeColumn(String measurementId) {
    stableList.deleteColumn(measurementIndexMap.get(measurementId));
    deltaList.deleteColumn(measurementIndexMap.get(measurementId));
    IMeasurementSchema schemaToBeRemoved = schemaList.get(measurementIndexMap.get(measurementId));
    schemaList.remove(schemaToBeRemoved);
    measurementIndexMap.clear();
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementName(), i);
    }
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schemaList);
  }

  private void writeData(
      AlignedChunkWriterImpl alignedChunkWriter, List<TSDataType> dataTypes, TimeValuePair tvPair) {
    long time = tvPair.getTimestamp();
    TsPrimitiveType[] value = tvPair.getValue().getVector();
    for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
      TSDataType tsDataType = dataTypes.get(columnIndex);
      ValueChunkWriter valueChunkWriter =
          alignedChunkWriter.getValueChunkWriterByIndex(columnIndex);
      switch (tsDataType) {
        case BOOLEAN:
          valueChunkWriter.write(
              time, value != null && value[columnIndex].getBoolean(), value == null);
          break;
        case INT32:
        case DATE:
          valueChunkWriter.write(
              time, value == null ? 0 : value[columnIndex].getInt(), value == null);
          break;
        case INT64:
        case TIMESTAMP:
          valueChunkWriter.write(
              time, value == null ? 0 : value[columnIndex].getLong(), value == null);
          break;
        case FLOAT:
          valueChunkWriter.write(
              time, value == null ? 0 : value[columnIndex].getFloat(), value == null);
          break;
        case DOUBLE:
          valueChunkWriter.write(
              time, value == null ? 0 : value[columnIndex].getDouble(), value == null);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          valueChunkWriter.write(
              time,
              value == null ? Binary.EMPTY_VALUE : value[columnIndex].getBinary(),
              value == null);
          break;
        default:
          LOGGER.error("WritableMemChunk does not support data type: {}", tsDataType);
          break;
      }
    }
  }

  @SuppressWarnings({"squid:S6541", "squid:S3776"})
  @Override
  public void encode(BlockingQueue<Object> ioTaskQueue) {
    AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(schemaList);

    int avgPointSizeOfLargestColumn =
        Math.max(
            stableList.getAvgPointSizeOfLargestColumn(),
            deltaList.getAvgPointSizeOfLargestColumn());
    maxNumberOfPointsInChunk =
        Math.min(maxNumberOfPointsInChunk, (TARGET_CHUNK_SIZE / avgPointSizeOfLargestColumn));

    // create MergeSortAlignedTVListIterator.
    AlignedDeltaMemChunkIterator iterator = iterator();

    int pointNumInPage = 0;
    int pointNumInChunk = 0;
    long[] times = new long[MAX_NUMBER_OF_POINTS_IN_PAGE];

    while (iterator.hasNext()) {
      TimeValuePair tvPair = iterator.next();
      times[pointNumInPage] = tvPair.getTimestamp();
      writeData(alignedChunkWriter, stableList.getTsDataTypes(), tvPair);
      pointNumInPage++;
      pointNumInChunk++;

      if (pointNumInPage == MAX_NUMBER_OF_POINTS_IN_PAGE
          || pointNumInChunk >= maxNumberOfPointsInChunk) {
        alignedChunkWriter.write(times, pointNumInPage, 0);
        pointNumInPage = 0;
      }

      if (pointNumInChunk >= maxNumberOfPointsInChunk) {
        alignedChunkWriter.sealCurrentPage();
        alignedChunkWriter.clearPageWriter();
        try {
          ioTaskQueue.put(alignedChunkWriter);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        alignedChunkWriter = new AlignedChunkWriterImpl(schemaList);
        pointNumInChunk = 0;
      }
    }

    // last batch of points
    if (pointNumInChunk > 0) {
      if (pointNumInPage > 0) {
        alignedChunkWriter.write(times, pointNumInPage, 0);
        alignedChunkWriter.sealCurrentPage();
        alignedChunkWriter.clearPageWriter();
      }
      try {
        ioTaskQueue.put(alignedChunkWriter);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void release() {
    stableList.clear();
    deltaList.clear();
    deltaTree.clear();
  }

  @Override
  public long getFirstPoint() {
    return Math.min(stableList.getMinTime(), deltaList.getMinTime());
  }

  @Override
  public long getLastPoint() {
    return maxTime;
  }

  @Override
  public boolean isEmpty() {
    return (stableList.rowCount() == 0 && deltaList.rowCount() == 0)
        || measurementIndexMap.isEmpty();
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (IMeasurementSchema schema : schemaList) {
      size += schema.serializedSize();
    }

    size += stableList.serializedSize();
    size += deltaList.serializedSize();
    size += deltaTree.serializedSize();
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(schemaList.size(), buffer);
    for (IMeasurementSchema schema : schemaList) {
      byte[] bytes = new byte[schema.serializedSize()];
      schema.serializeTo(ByteBuffer.wrap(bytes));
      buffer.put(bytes);
    }

    stableList.serializeToWAL(buffer);
    deltaList.serializeToWAL(buffer);
    deltaTree.serializeToWAL(buffer);
  }

  public List<Integer> buildColumnIndexList(List<IMeasurementSchema> schemaList) {
    List<Integer> columnIndexList = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : schemaList) {
      columnIndexList.add(
          measurementIndexMap.getOrDefault(measurementSchema.getMeasurementName(), -1));
    }
    return columnIndexList;
  }

  public TsBlock buildTsBlock(
      List<IMeasurementSchema> fullPathSchemaList,
      List<Integer> columnIndexList,
      int floatPrecision,
      List<TSEncoding> encodingList,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows) {
    List<TSDataType> dataTypes =
        fullPathSchemaList.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(dataTypes);
    TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();

    int[] timeColumnDeleteCursor = {0};
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    BitMap nullbitMap = new BitMap(dataTypes.size());
    AlignedDeltaMemChunkIterator iterator = iterator();
    while (iterator.hasNext()) {
      TimeValuePair tvPair = iterator.next();
      long time = tvPair.getTimestamp();
      if (!isPointDeleted(time, timeColumnDeletion, timeColumnDeleteCursor)) {
        TsPrimitiveType[] values = tvPair.getValue().getVector();
        if (valueColumnsDeletionList != null && !valueColumnsDeletionList.isEmpty()) {
          for (int i = 0; i < dataTypes.size(); i++) {
            if (isPointDeleted(
                time, valueColumnsDeletionList.get(i), valueColumnDeleteCursor.get(i))) {
              values[i] = null;
              nullbitMap.mark(i);
            }
          }
        }

        // check if all null
        if (nullbitMap.isAllMarked() && ignoreAllNullRows) {
          nullbitMap.reset();
          continue;
        }

        // time column
        timeBuilder.writeLong(time);
        // value columns
        for (int i = 0; i < dataTypes.size(); i++) {
          ColumnBuilder valueBuilder = builder.getColumnBuilder(i);

          int columnIndex = columnIndexList.get(i);
          if (columnIndex < 0 || values[columnIndex] == null) {
            valueBuilder.appendNull();
            continue;
          }

          TSDataType tsDataType = dataTypes.get(i);
          switch (tsDataType) {
            case BOOLEAN:
              valueBuilder.writeBoolean(values[columnIndex].getBoolean());
              break;
            case INT32:
            case DATE:
              valueBuilder.writeInt(values[columnIndex].getInt());
              break;
            case INT64:
            case TIMESTAMP:
              valueBuilder.writeLong(values[columnIndex].getLong());
              break;
            case FLOAT:
              float fv = values[columnIndex].getFloat();
              if (!Float.isNaN(fv)
                  && (encodingList.get(i) == TSEncoding.RLE
                      || encodingList.get(i) == TSEncoding.TS_2DIFF)) {
                fv = MathUtils.roundWithGivenPrecision(fv, floatPrecision);
              }
              valueBuilder.writeFloat(fv);
              break;
            case DOUBLE:
              double dv = values[columnIndex].getDouble();
              if (!Double.isNaN(dv)
                  && (encodingList.get(i) == TSEncoding.RLE
                      || encodingList.get(i) == TSEncoding.TS_2DIFF)) {
                dv = MathUtils.roundWithGivenPrecision(dv, floatPrecision);
              }
              valueBuilder.writeDouble(dv);
              break;
            case TEXT:
            case BLOB:
            case STRING:
              valueBuilder.writeBinary(values[columnIndex].getBinary());
              break;
            default:
              LOGGER.error("buildTsBlock does not support data type: {}", tsDataType);
              break;
          }
        }
        builder.declarePosition();
        nullbitMap.reset();
      }
    }
    return builder.build();
  }

  public static AlignedDeltaWritableMemChunk deserialize(
      DataInputStream stream, boolean isTableModel) throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }

    AlignedTVList stableList = (AlignedTVList) TVList.deserialize(stream);
    AlignedTVList deltaList = (AlignedTVList) TVList.deserialize(stream);
    DeltaIndexTree deltaTree = DeltaIndexTree.deserialize(stream);
    return new AlignedDeltaWritableMemChunk(
        schemaList, stableList, deltaList, deltaTree, isTableModel);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public boolean isAllDeleted() {
    return stableList.isAllDeleted() && deltaList.isAllDeleted();
  }

  private void filterDeletedTimeStamp(
      AlignedTVList alignedTVList,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows,
      Map<Long, BitMap> timestampWithBitmap) {
    BitMap allValueColDeletedMap = alignedTVList.getAllValueColDeletedMap();

    int rowCount = alignedTVList.rowCount();
    List<int[]> valueColumnDeleteCursor = new ArrayList<>();
    if (valueColumnsDeletionList != null) {
      valueColumnsDeletionList.forEach(x -> valueColumnDeleteCursor.add(new int[] {0}));
    }

    for (int row = 0; row < rowCount; row++) {
      // the row is deleted
      if (allValueColDeletedMap != null && allValueColDeletedMap.isMarked(row)) {
        continue;
      }
      long timestamp = alignedTVList.getTime(row);

      BitMap bitMap = new BitMap(schemaList.size());
      for (int column = 0; column < schemaList.size(); column++) {
        if (alignedTVList.isNullValue(alignedTVList.getValueIndex(row), column)) {
          bitMap.mark(column);
        }

        // skip deleted row
        if (valueColumnsDeletionList != null
            && !valueColumnsDeletionList.isEmpty()
            && isPointDeleted(
                timestamp,
                valueColumnsDeletionList.get(column),
                valueColumnDeleteCursor.get(column))) {
          bitMap.mark(column);
        }

        // skip all-null row
        if (ignoreAllNullRows && bitMap.isAllMarked()) {
          continue;
        }
        timestampWithBitmap.put(timestamp, bitMap);
      }
    }
  }

  public long[] getFilteredTimestamp(
      List<List<TimeRange>> deletionList, List<BitMap> bitMaps, boolean ignoreAllNullRows) {
    Map<Long, BitMap> timestampWithBitmap = new TreeMap<>();

    filterDeletedTimeStamp(stableList, deletionList, ignoreAllNullRows, timestampWithBitmap);
    filterDeletedTimeStamp(deltaList, deletionList, ignoreAllNullRows, timestampWithBitmap);

    List<Long> filteredTimestamps = new ArrayList<>();
    for (Map.Entry<Long, BitMap> entry : timestampWithBitmap.entrySet()) {
      filteredTimestamps.add(entry.getKey());
      bitMaps.add(entry.getValue());
    }
    return filteredTimestamps.stream().mapToLong(Long::valueOf).toArray();
  }

  public AlignedDeltaMemChunkIterator iterator() {
    return new AlignedDeltaMemChunkIterator();
  }

  public class AlignedDeltaMemChunkIterator {
    private DeltaIndexTree.DeltaIndexTreeLeafNode current;
    private int entryIndex = 0;
    private int stableIndex = 0;

    private boolean probeNext = false;
    private TimeValuePair currentTvPair = null;
    private boolean validEntry = false;
    private TimeValuePair intermediateDeltaTvPair = null;
    private TimeValuePair intermediateStableTvPair = null;

    BitMap stableDeletedRowMap = ignoreAllNullRows ? stableList.getAllValueColDeletedMap() : null;
    BitMap deltaDeletedRowMap = ignoreAllNullRows ? deltaList.getAllValueColDeletedMap() : null;

    public AlignedDeltaMemChunkIterator() {
      this.current = deltaTree.getFirstLeaf();
    }

    private void nextDeltaEntry() {
      entryIndex++;
      // next leaf page
      if (entryIndex >= current.count) {
        current = current.next;
        entryIndex = 0;
      }
    }

    private void prepareNext() {
      currentTvPair = null;

      // try to find valid entry in delta index tree
      if (!validEntry) {
        // skip deleted entry
        while (current != null && entryIndex < current.count) {
          int deltaId = current.deltaIds[entryIndex];
          if (deltaDeletedRowMap != null && deltaDeletedRowMap.isMarked(deltaId)) {
            nextDeltaEntry();
          } else {
            validEntry = true;
            intermediateDeltaTvPair = deltaList.getTimeValuePair(deltaId);
            break;
          }
        }
        // handle duplicated timestamp
        while (current != null && entryIndex < current.count) {
          if ((entryIndex + 1 < current.count
                  && current.keys[entryIndex] == current.keys[entryIndex + 1])
              || (current.next != null
                  && !current.next.isEmpty()
                  && current.keys[entryIndex] == current.next.keys[0])) {
            nextDeltaEntry();
            int deltaId = current.deltaIds[entryIndex];
            // update not-null column
            TsPrimitiveType[] values = deltaList.getTimeValuePair(deltaId).getValue().getVector();
            for (int columnIndex = 0; columnIndex < schemaList.size(); columnIndex++) {
              if (values[columnIndex] != null) {
                intermediateDeltaTvPair.getValue().getVector()[columnIndex] = values[columnIndex];
              }
            }
          } else {
            break;
          }
        }
      }

      int stableRowCount = stableList.rowCount();
      if (validEntry) {
        int stableId = current.stableIds[entryIndex];
        int deltaId = current.deltaIds[entryIndex];
        // skip deleted rows
        while (stableIndex <= stableId
            && stableDeletedRowMap != null
            && stableDeletedRowMap.isMarked(stableIndex)) {
          stableIndex++;
        }
        intermediateStableTvPair = stableList.getTimeValuePair(stableIndex);
        // handle duplicated timestamp
        while (stableIndex + 1 <= stableId
            && stableList.getTime(stableIndex) == stableList.getTime(stableIndex + 1)) {
          stableIndex++;
          // update not-null column
          TsPrimitiveType[] values =
              stableList.getTimeValuePair(stableIndex).getValue().getVector();
          for (int columnIndex = 0; columnIndex < schemaList.size(); columnIndex++) {
            if (values[columnIndex] != null) {
              intermediateStableTvPair.getValue().getVector()[columnIndex] = values[columnIndex];
            }
          }
        }

        if (stableIndex > stableId) {
          currentTvPair = intermediateDeltaTvPair;
        } else if (stableIndex == stableId
            && stableList.getTime(stableIndex) == deltaList.getTime(deltaId)) {
          currentTvPair = intermediateStableTvPair;
          TsPrimitiveType[] values = intermediateDeltaTvPair.getValue().getVector();
          for (int columnIndex = 0; columnIndex < schemaList.size(); columnIndex++) {
            if (values[columnIndex] != null) {
              currentTvPair.getValue().getVector()[columnIndex] = values[columnIndex];
            }
          }
          stableIndex++;
          intermediateStableTvPair = null;
        } else {
          currentTvPair = intermediateStableTvPair;
        }
      } else {
        // skip deleted rows
        while (stableIndex < stableRowCount
            && stableDeletedRowMap != null
            && stableDeletedRowMap.isMarked(stableIndex)) {
          stableIndex++;
        }
        if (stableIndex < stableRowCount) {
          currentTvPair = stableList.getTimeValuePair(stableIndex);
        }
        // handle duplicated timestamp
        while (stableIndex + 1 < stableRowCount
            && stableList.getTime(stableIndex) == stableList.getTime(stableIndex + 1)) {
          stableIndex++;
          TsPrimitiveType[] values =
              stableList.getTimeValuePair(stableIndex).getValue().getVector();
          for (int columnIndex = 0; columnIndex < schemaList.size(); columnIndex++) {
            if (values[columnIndex] != null) {
              currentTvPair.getValue().getVector()[columnIndex] = values[columnIndex];
            }
          }
        }
      }
      probeNext = true;
    }

    public boolean hasNext() {
      if (!probeNext) {
        prepareNext();
      }
      return currentTvPair != null;
    }

    public TimeValuePair next() {
      if (!hasNext()) {
        return null;
      }
      if (current == null || current.isEmpty()) {
        stableIndex++;
        intermediateStableTvPair = null;
      } else {
        int stableId = current.stableIds[entryIndex];
        if (stableIndex <= stableId) {
          stableIndex++;
          intermediateStableTvPair = null;
        } else {
          nextDeltaEntry();
          validEntry = false;
          intermediateDeltaTvPair = null;
        }
      }
      probeNext = false;
      return currentTvPair;
    }
  }
}
