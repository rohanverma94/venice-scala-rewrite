package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;


public interface InputDataInfoProvider {

  class InputDataInfo {
    private final KafkaPushJob.SchemaInfo schemaInfo;
    private final Long inputFileDataSizeInBytes;

    InputDataInfo(KafkaPushJob.SchemaInfo schemaInfo, Long inputFileDataSizeInBytes) {
      this.schemaInfo = schemaInfo;
      this.inputFileDataSizeInBytes = inputFileDataSizeInBytes;
    }

    public KafkaPushJob.SchemaInfo getSchemaInfo() {
      return schemaInfo;
    }

    public Long getInputFileDataSizeInBytes() {
      return inputFileDataSizeInBytes;
    }
  }

  InputDataInfo validateInputAndGetSchema(String inputUri, VeniceProperties props) throws Exception;
}
