package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.List;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Future;

import static com.linkedin.venice.hadoop.MapReduceConstants.*;
import static org.mockito.Mockito.*;

public class TestVeniceReducer extends AbstractTestVeniceMR {

  @Test
  public void testReduce() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf());
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    values.add(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = mock(Reporter.class);

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaIdCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<VeniceReducer.KafkaMessageCallback> callbackCaptor = ArgumentCaptor.forClass(VeniceReducer.KafkaMessageCallback.class);

    verify(mockWriter).put(keyCaptor.capture(), valueCaptor.capture(), schemaIdCaptor.capture(), callbackCaptor.capture());
    Assert.assertEquals(keyCaptor.getValue(), keyFieldValue.getBytes());
    Assert.assertEquals(valueCaptor.getValue(), valueFieldValue.getBytes());
    Assert.assertEquals(schemaIdCaptor.getValue(), new Integer(VALUE_SCHEMA_ID));
    Assert.assertEquals(callbackCaptor.getValue().getProgressable(), mockReporter);

    verify(mockReporter).incrCounter(COUNTER_GROUP_KAFKA, COUNTER_OUTPUT_RECORDS, 1);
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testReduceWithNoValue() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf());
    final String keyFieldValue = "test_key";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = mock(Reporter.class);

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);
  }

  @Test
  public void testReduceWithMultipleSameValues() {
    Reporter mockReporter = mock(Reporter.class);
    // Duplicate key with same values should not fail
    testDuplicateKey(true,mockReporter);
    verify(mockReporter, never()).incrCounter(eq(COUNTER_GROUP_DATA_QUALITY), eq(DUPLICATE_KEY_WITH_DISTINCT_VALUE), anyLong());
    verify(mockReporter, times(1)).incrCounter(eq(COUNTER_GROUP_DATA_QUALITY), eq(DUPLICATE_KEY_WITH_IDENTICAL_VALUE), eq(1L));
  }

  @Test
  public void testReduceWithMultipleDistinctValues() {
    Reporter mockReporter = mock(Reporter.class);
    // Duplicate key with distinct values should not fail
    testDuplicateKey(false, mockReporter);
    verify(mockReporter, times(1)).incrCounter(eq(COUNTER_GROUP_DATA_QUALITY), eq(DUPLICATE_KEY_WITH_DISTINCT_VALUE), eq(1L));
    verify(mockReporter, never()).incrCounter(eq(COUNTER_GROUP_DATA_QUALITY), eq(DUPLICATE_KEY_WITH_IDENTICAL_VALUE), anyLong());
  }

  private void testDuplicateKey(boolean sameValue, Reporter reporter) {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf());

    //key needs to be Avro-formatted bytes here cause
    //Reducer is gonna try deserialize it if it finds duplicates key
    byte[] keyBytes =
        new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key");

    BytesWritable keyWritable = new BytesWritable(keyBytes);
    ArrayList<BytesWritable> values = new ArrayList<>();
    values.add(new BytesWritable("test_value".getBytes()));
    values.add(sameValue ? new BytesWritable("test_value".getBytes()) :
        new BytesWritable("test_value1".getBytes()));
    OutputCollector mockCollector = mock(OutputCollector.class);
    reducer.reduce(keyWritable, values.iterator(), mockCollector, reporter);
  }

  @Test
  public void testReduceWithTopicAuthorizationException() throws IOException {
    AbstractVeniceWriter mockVeniceWriter = mock(AbstractVeniceWriter.class);
    when(mockVeniceWriter.put(any(), any(), anyInt(), any())).thenThrow(new TopicAuthorizationVeniceException("No ACL permission"));
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockVeniceWriter);
    reducer.configure(setupJobConf());

    // One key and one value
    byte[] keyBytes = new VeniceAvroKafkaSerializer("\"string\"").serialize("test_topic", "test_key");
    BytesWritable keyWritable = new BytesWritable(keyBytes);
    ArrayList<BytesWritable> values = new ArrayList<>();
    values.add(new BytesWritable("test_value".getBytes()));
    Reporter mockReporter = mock(Reporter.class);
    OutputCollector mockCollector = mock(OutputCollector.class);
    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);

    // Expect the counter to record this authorization error
    verify(mockReporter, times(1)).incrCounter(eq(COUNTER_GROUP_KAFKA), eq(AUTHORIZATION_FAILURES), eq(1L));
    verify(mockCollector, never()).collect(any(), any());
  }

  @Test
  public void testReduceWithDifferentReporters() {
    AbstractVeniceWriter mockWriter = mock(AbstractVeniceWriter.class);
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(mockWriter);
    reducer.configure(setupJobConf());
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    values.add(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = mock(Reporter.class);

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);

    ArgumentCaptor<VeniceReducer.KafkaMessageCallback> callbackCaptor = ArgumentCaptor.forClass(VeniceReducer.KafkaMessageCallback.class);

    verify(mockWriter).put(any(), any(), anyInt(), callbackCaptor.capture());
    Assert.assertEquals(callbackCaptor.getValue().getProgressable(), mockReporter);

    // test with different reporter
    Reporter newMockReporter = mock(Reporter.class);
    reducer.reduce(keyWritable, values.iterator(), mockCollector, newMockReporter);
    verify(mockWriter, times(2)).put(any(), any(), anyInt(), callbackCaptor.capture());
    Assert.assertEquals(callbackCaptor.getValue().getProgressable(), newMockReporter);
  }

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "KafkaPushJob failed with exception.*")
  public void testReduceWithWriterException() {
    AbstractVeniceWriter exceptionWriter = new AbstractVeniceWriter(TOPIC_NAME) {
      @Override
      public void close(boolean shouldEndAllSegments) {
        // no-op
      }

      @Override
      public Future<RecordMetadata> put(Object key, Object value, int valueSchemaId, Callback callback) {
        callback.onCompletion(null, new VeniceException("Fake exception"));
        return null;
      }

      @Override
      public Future<RecordMetadata> update(Object key, Object update, int valueSchemaId,
          int derivedSchemaId, Callback callback) {
        // no-op
        return null;
      }

      @Override
      public void flush() {
        // no-op
      }

      @Override
      public void close() {
        // no-op
      }
    };

    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(exceptionWriter);
    reducer.configure(setupJobConf());
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    values.add(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = mock(Reporter.class);

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);
    // The following 'reduce' operation will throw exception
    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);
  }

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "KafkaPushJob failed with exception.*")
  public void testClosingReducerWithWriterException() throws IOException {
    AbstractVeniceWriter exceptionWriter = new AbstractVeniceWriter(TOPIC_NAME) {
      @Override
      public Future<RecordMetadata> put(Object key, Object value, int valueSchemaId, Callback callback) {
        callback.onCompletion(null, new VeniceException("Some writer exception"));
        return null;
      }

      @Override
      public Future<RecordMetadata> update(Object key, Object update, int valueSchemaId,
          int derivedSchemaId, Callback callback) {
        // no-op
        return null;
      }

      @Override
      public void flush() {
        // no-op
      }

      @Override
      public void close(boolean shouldCloseAllSegments) {
        Assert.assertFalse(shouldCloseAllSegments, "A writer exception is thrown, should not close all segments");
      }

      @Override
      public void close() throws IOException {
        // no-op
      }
    };
    VeniceReducer reducer = new VeniceReducer();
    reducer.setVeniceWriter(exceptionWriter);
    reducer.configure(setupJobConf());
    final String keyFieldValue = "test_key";
    final String valueFieldValue = "test_value";
    BytesWritable keyWritable = new BytesWritable(keyFieldValue.getBytes());
    BytesWritable valueWritable = new BytesWritable(valueFieldValue.getBytes());
    List<BytesWritable> values = new ArrayList<>();
    values.add(valueWritable);
    OutputCollector mockCollector = mock(OutputCollector.class);
    Reporter mockReporter = mock(Reporter.class);

    reducer.reduce(keyWritable, values.iterator(), mockCollector, mockReporter);
    reducer.close();
  }
}
