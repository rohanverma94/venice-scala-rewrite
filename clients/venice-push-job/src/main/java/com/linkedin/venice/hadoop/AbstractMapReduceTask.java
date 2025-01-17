package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.SSL_CONFIGURATOR_CLASS_CONFIG;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;


/**
 * Class for commonalities between {@link AbstractVeniceMapper} and {@link VeniceReducer}.
 */
public abstract class AbstractMapReduceTask {
  public static final String MAPRED_TASK_ID_PROP_NAME = "mapred.task.id";
  protected static final int TASK_ID_NOT_SET = -1;

  private int partitionCount;
  private int taskId = TASK_ID_NOT_SET;
  private boolean isChunkingEnabled;

  abstract protected void configureTask(VeniceProperties props, JobConf job);

  protected int getPartitionCount() {
    return this.partitionCount;
  }

  protected int getTaskId() {
    return this.taskId;
  }

  protected void setChunkingEnabled(boolean isChunkingEnabled) {
    this.isChunkingEnabled = isChunkingEnabled;
  }

  protected boolean isChunkingEnabled() {
    return isChunkingEnabled;
  }

  public final void configure(JobConf job) {
    VeniceProperties props;

    SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(job.get(SSL_CONFIGURATOR_CLASS_CONFIG));
    try {
      Properties javaProps =
          configurator.setupSSLConfig(HadoopUtils.getProps(job), UserCredentialsFactory.getHadoopUserCredentials());
      props = new VeniceProperties(javaProps);
    } catch (IOException e) {
      throw new VeniceException("Could not get user credential for job:" + job.getJobName(), e);
    }
    setChunkingEnabled(props.getBoolean(VeniceWriter.ENABLE_CHUNKING));
    this.partitionCount = job.getNumReduceTasks();
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(job.get(MAPRED_TASK_ID_PROP_NAME));
    if (taskAttemptID == null) {
      throw new UndefinedPropertyException(
          MAPRED_TASK_ID_PROP_NAME + " not found in the " + JobConf.class.getSimpleName());
    }
    TaskID taskID = taskAttemptID.getTaskID();
    if (taskID == null) {
      throw new NullPointerException("taskAttemptID.getTaskID() is null");
    }
    this.taskId = taskID.getId();

    configureTask(props, job);
  }
}
