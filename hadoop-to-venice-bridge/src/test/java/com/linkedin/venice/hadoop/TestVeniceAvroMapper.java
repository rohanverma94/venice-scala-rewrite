package com.linkedin.venice.hadoop;

import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;

import static com.linkedin.venice.hadoop.MapReduceConstants.*;
import static org.mockito.Mockito.*;

public class TestVeniceAvroMapper extends AbstractTestVeniceMR {

  @Test
  public void testConfigure() {
    JobConf job = setupJobConf();
    VeniceAvroMapper mapper = new VeniceAvroMapper();
    try {
      mapper.configure(job);
    } catch (Exception e) {
      Assert.fail("VeniceAvroMapper#configure should not throw any exception when all the required props are there");
    }
  }

  @Test (expectedExceptions = UndefinedPropertyException.class)
  public void testConfigureWithMissingProps() {
    JobConf job = setupJobConf();
    job.unset(KafkaPushJob.TOPIC_PROP);
    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(job);
  }

  @Test
  public void testMap() throws IOException {
    final String keyFieldValue = "key_field_value";
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, null);

    ArgumentCaptor<BytesWritable> keyCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    ArgumentCaptor<BytesWritable> valueCaptor = ArgumentCaptor.forClass(BytesWritable.class);
    verify(output).collect(keyCaptor.capture(), valueCaptor.capture());
    Assert.assertTrue(getHexString(keyCaptor.getValue().copyBytes())
        .endsWith(getHexString(keyFieldValue.getBytes())));
    Assert.assertTrue(getHexString(valueCaptor.getValue().copyBytes())
        .endsWith(getHexString(valueFieldValue.getBytes())));
  }

  @Test (expectedExceptions = VeniceException.class)
  public void testMapWithNullKey() throws IOException {
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(null, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, null);
  }

  @Test
  public void testMapWithNullValue() throws IOException {
    final String keyFieldValue = "key_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, null);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.map(wrapper, NullWritable.get(), output, mock(Reporter.class));

    verify(output, never()).collect(Mockito.any(), Mockito.any());
  }

  @Test
  public void testMapWithTopicAuthorizationException() throws IOException {
    final String keyFieldValue = "key_field_value";
    final String valueFieldValue = "value_field_value";
    AvroWrapper<IndexedRecord> wrapper = getAvroWrapper(keyFieldValue, valueFieldValue);
    OutputCollector<BytesWritable, BytesWritable> output = mock(OutputCollector.class);
    Reporter mockReporter = mock(Reporter.class);
    VeniceReducer mockReducer = mock(VeniceReducer.class);
    doThrow(new TopicAuthorizationVeniceException("No ACL permission")).when(mockReducer).sendMessageToKafka(any(), any(), any());

    VeniceAvroMapper mapper = new VeniceAvroMapper();
    mapper.configure(setupJobConf());
    mapper.setVeniceReducer(mockReducer);
    mapper.setIsMapperOnly(true);
    mapper.map(wrapper, NullWritable.get(), output, mockReporter);
    verify(mockReporter, times(1)).incrCounter(eq(COUNTER_GROUP_KAFKA), eq(AUTHORIZATION_FAILURES), eq(1L));
  }

  private AvroWrapper<IndexedRecord> getAvroWrapper(String keyFieldValue, String valueFieldValue) {
    GenericData.Record record = new GenericData.Record(Schema.parse(SCHEMA_STR));
    record.put(KEY_FIELD, keyFieldValue);
    record.put(VALUE_FIELD, valueFieldValue);
    return new AvroWrapper<>(record);
  }
}
