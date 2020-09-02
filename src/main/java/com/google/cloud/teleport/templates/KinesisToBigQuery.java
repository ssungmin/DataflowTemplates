/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import com.amazonaws.regions.Regions;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import com.amazonaws.regions.Regions;
import org.joda.time.Instant;

/**
 * The {@link KinesisToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
 * from Kafka, executes a UDF, and outputs the resulting records to BigQuery. Any errors which occur
 * in the transformation of the data or execution of the UDF will be output to a separate errors
 * table in BigQuery. The errors table will be created if it does not exist prior to execution. Both
 * output and error tables are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Kafka topic exists and the message is encoded in a valid JSON format.
 *   <li>The BigQuery output table exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/kafka-to-bigquery
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.KafkaToBigQuery \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}"
 *
 * # Execute the template
 * JOB_NAME=kafka-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "bootstrapServers=my_host:9092,inputTopic=kafka-test,\
 * outputTableSpec=kafka-test:kafka.kafka_to_bigquery,\
 * outputDeadletterTable=kafka-test:kafka.kafka_to_bigquery_deadletter"
 * </pre>
 */
public class KinesisToBigQuery {

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(KinesisToBigQuery.class);

  /** The tag for the main output for the UDF. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_OUT =
          new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

  /** The tag for the dead-letter output of the udf. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_DEADLETTER_OUT =
          new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>>
          TRANSFORM_DEADLETTER_OUT = new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The default suffix for error tables if dead letter table is not specified. */
  public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions ,AwsOptions, JavascriptTextTransformerOptions {
    @Description("AWS Access Key")
    String getAwsAccessKey();

    void setAwsAccessKey(String value);

    @Description("AWS Secret Key")
    String getAwsSecretKey();

    void setAwsSecretKey(String value);


    @Description("Name of the Kinesis Data Stream to read from")
    String getInputStreamName();

    void setInputStreamName(String value);

    @Description("Initial Position In Stream")
    String getInitialPositionInStream();

    void setInitialPositionInStream(String value);


    @Description("Initial Position In Stream")
    String getInitialTimestampInStream();

    void setInitialTimestampInStream(String value);

    @Description("gzip exist")
    ValueProvider<String> getGzipYN();

    void setGzipYN( ValueProvider<String> value);

    @Description("Table spec to write the output to")
    String getOutputTableSpec();

    void setOutputTableSpec(String value);


    @Description(
            "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                    + "format. If it doesn't exist, it will be created during pipeline execution.")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String value);

  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * KinesisToBigQuery#run(Options)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);

    // Register the coder for pipeline
    FailsafeElementCoder<KV<String, String>, String> coder =
            FailsafeElementCoder.of(
                    KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), StringUtf8Coder.of());


    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    /*
     * Steps:
     *  1) Read messages in from Kafka
     *  2) Transform the Kafka Messages into TableRows
     *     - Transform message payload via UDF
     *     - Convert UDF result to TableRow objects
     *  3) Write successful records out to BigQuery
     *  4) Write failed records out to BigQuery
     */
    InitialPositionInStream initialPosition = InitialPositionInStream.LATEST;
    Instant inst = Instant.parse("2020-01-01T00:00:00.00Z");

    if (options.getInitialPositionInStream().equals("TRIM_HORIZON")) {
      initialPosition = InitialPositionInStream.TRIM_HORIZON;
    } else if(options.getInitialPositionInStream().equals("AT_TIMESTAMP")) {
      inst = Instant.parse(options.getInitialTimestampInStream());

    }


    PCollectionTuple transformOut =
     //PCollection transformOut =
              pipeline
                .apply(
                    "kinesis stream source",
                        KinesisIO.read()
                        .withStreamName(options.getInputStreamName())
                        .withInitialPositionInStream(initialPosition)
                        .withInitialTimestampInStream(inst)
                        .withAWSClientsProvider(options.getAwsAccessKey(),options.getAwsSecretKey() , Regions.fromName(options.getAwsRegion())))

                .apply(
                              "parse kinesis events",
                        ParDo.of(
                            new DoFn<KinesisRecord, KV<String, String>>() {
                                @ProcessElement
                                public void processElement(@Element KinesisRecord record, PipelineOptions pipe, OutputReceiver<KV<String, String>> out) {
                                     //KinesisRecord record = out.element();
                                      try {

                                        Options opts = pipe.as(Options.class);

                                        if (opts.getGzipYN().equals("N") ) {
                                          out.output( KV.of(record.getPartitionKey(), getStringFromByteArrayWithGzip(record.getDataAsBytes())));


                                        } else {

                                          out.output( KV.of(record.getPartitionKey(), new String(record.getDataAsBytes(), "UTF-8")));
                                        }


                                      } catch (Exception e) {
                                            LOG.warn("failed to parse event: {}", e.getLocalizedMessage());
                                      }
                                }

                              private String getStringFromByteArrayWithGzip(byte[] dataAsBytes) {
                                try {
                                  ByteArrayInputStream bais = new ByteArrayInputStream(dataAsBytes);
                                  GZIPInputStream gzis = new GZIPInputStream(bais);
                                  InputStreamReader reader = new InputStreamReader(gzis,"UTF-8");
                                  BufferedReader in = new BufferedReader(reader);

                                  StringBuilder sb = new StringBuilder();
                                  String readed;
                                  while ((readed = in.readLine()) != null) {
                                    sb.append(readed);
                                  }
                                  return sb.toString();
                                } catch (IOException ioe) {
                                  LOG.warn("failed to unzip event: {}", ioe.getLocalizedMessage());
                                }
                                return null;
                              }
                              }))

                        .apply("ConvertMessageToTableRow", new MessageToTableRow(options));
     // LOG.info(transformOut.toString());

    /*
     * Step #3: Write the successful records out to BigQuery
     */

    transformOut
            .get(TRANSFORM_OUT)
            .apply(
                    "WriteSuccessfulRecords",
                    BigQueryIO.writeTableRows()
                            .withoutValidation()
                            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                            .to(options.getOutputTableSpec()));

    /*
     * Step #4: Write failed records out to BigQuery
     */
    /**
    PCollectionList.of(transformOut.get(UDF_DEADLETTER_OUT))
            .and(transformOut.get(TRANSFORM_DEADLETTER_OUT))
            .apply("Flatten", Flatten.pCollections())
            .apply(
                    "WriteFailedRecords",
                    WriteKafkaMessageErrors.newBuilder()
                            .setErrorRecordsTable(
                                    ValueProviderUtils.maybeUseDefaultDeadletterTable(
                                            options.getOutputDeadletterTable(),
                                            options.getOutputTableSpec(),
                                            DEFAULT_DEADLETTER_TABLE_SUFFIX))
                            .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                            .build()); **/
    return pipeline.run();
  }

  static class KafkaRecordToFailsafeElementFn
          extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> message = context.element();
      context.output(FailsafeElement.of(message, message.getValue()));
    }
  }

  /**
   * If deadletterTable is available, it is returned as is, otherwise outputTableSpec +
   * defaultDeadLetterTableSuffix is returned instead.
   */
  private static ValueProvider<String> maybeUseDefaultDeadletterTable(
          ValueProvider<String> deadletterTable,
          ValueProvider<String> outputTableSpec,
          String defaultDeadLetterTableSuffix) {
    return DualInputNestedValueProvider.of(
            deadletterTable,
            outputTableSpec,
            new SerializableFunction<TranslatorInput<String, String>, String>() {
              @Override
              public String apply(TranslatorInput<String, String> input) {
                String userProvidedTable = input.getX();
                String outputTableSpec = input.getY();
                if (userProvidedTable == null) {
                  return outputTableSpec + defaultDeadLetterTableSuffix;
                }
                return userProvidedTable;
              }
            });
  }

  /**
   * The {@link MessageToTableRow} class is a {@link PTransform} which transforms incoming Kafka
   * Message objects into {@link TableRow} objects for insertion into BigQuery while applying an
   * optional UDF to the input. The executions of the UDF and transformation to {@link TableRow}
   * objects is done in a fail-safe way by wrapping the element with it's original payload inside
   * the {@link FailsafeElement} class. The {@link MessageToTableRow} transform will output a {@link
   * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link KinesisToBigQuery#UDF_OUT} - Contains all {@link FailsafeElement} records
   *       successfully processed by the optional UDF.
   *   <li>{@link KinesisToBigQuery#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement} records
   *       which failed processing during the UDF execution.
   *   <li>{@link KinesisToBigQuery#TRANSFORM_OUT} - Contains all records successfully converted from
   *       JSON to {@link TableRow} objects.
   *   <li>{@link KinesisToBigQuery#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
   *       records which couldn't be converted to table rows.
   * </ul>
   */
  static class MessageToTableRow
          extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {

    private final Options options;

    MessageToTableRow(Options options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<String, String>> input) {

      PCollectionTuple udfOut =
              input
                      // Map the incoming messages into FailsafeElements so we can recover from failures
                      // across multiple transforms.
                      .apply("MapToRecord", ParDo.of(new MessageToFailsafeElementFn()))
                      .apply(
                              "InvokeUDF",
                              FailsafeJavascriptUdf.<KV<String, String>>newBuilder()
                                      .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                                      .setFunctionName(options.getJavascriptTextTransformFunctionName())
                                      .setSuccessTag(UDF_OUT)
                                      .setFailureTag(UDF_DEADLETTER_OUT)
                                      .build());


      // Convert the records which were successfully processed by the UDF into TableRow objects.
      PCollectionTuple jsonToTableRowOut =
              udfOut
                      .get(UDF_OUT)
                      .apply(
                              "JsonToTableRow",
                              FailsafeJsonToTableRow.<KV<String, String>>newBuilder()
                                      .setSuccessTag(TRANSFORM_OUT)
                                      .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                                      .build());

      // Re-wrap the PCollections so we can return a single PCollectionTuple
      return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
              .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
              .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
              .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
    }
  }


  /**
   * The {@link MessageToFailsafeElementFn} wraps an Kafka Message with the {@link FailsafeElement}
   * class so errors can be recovered from and the original message can be output to a error records
   * table.
   */
  static class MessageToFailsafeElementFn
          extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> message = context.element();

      context.output(FailsafeElement.of(message, message.getValue()));
    }
  }



}
