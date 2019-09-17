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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.kafka.connector.KafkaIO;
import com.google.cloud.teleport.kafka.connector.KafkaRecord;
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
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * The {@link KafkaToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
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
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/multikafka-singlequeue
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.MultipleKafkaTopicsToSingleQueue \
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
public class MultipleKafkaTopicsToSingleQueue {

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigQuery.class);

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions, JavascriptTextTransformerOptions {
    @Description("Kafka Bootstrap Servers to write the input from")
    ValueProvider<String> getOutputBootstrapServers();

    void setOutputBootstrapServers(ValueProvider<String> value);

    @Description("Topic to write the output to")
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> value);

    @Description("Kafka Bootstrap Servers to read the input from")
    ValueProvider<String> getInputBootstrapServers();

    void setInputBootstrapServers(ValueProvider<String> value);

    @Description("Kafka topics to read the input from")
    ValueProvider<String> getInputTopics();

    void setInputTopics(ValueProvider<String> value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * MultipleKafkaTopicsToSingleQueue#run(Options)} method to start the pipeline and invoke {@code
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

    pipeline
                    /*
                     * Step #1: Read messages in from Kafka
                     */
                    .apply(
                            "ReadFromMultipleKafka",
                            KafkaIO.<String, String>read()
                                    .withBootstrapServers(options.getInputBootstrapServers())
                                    .withTopics(ValueProvider.NestedValueProvider.of(
                                            options.getInputTopics(),
                                            new SerializableFunction<String, List<String>>() {
                                                @Override
                                              public List<String> apply(String str) {
                                                return Arrays.asList(str.toString().split(","));
                                              }
                                            }))
                                    .withKeyDeserializer(StringDeserializer.class)
                                    .withValueDeserializer(StringDeserializer.class)
                                    // NumSplits is hard-coded to 1 for single-partition use cases (e.g., Debezium
                                    // Change Data Capture). Once Dataflow dynamic templates are available, this can
                                    // be deprecated.
                                    .withNumSplits(1))
                      .apply("SetRecordToProperTopic", ParDo.of(new DoFn<KafkaRecord<String, String>, KV<String, String>>(){
                        @ProcessElement
                        public void processElement(ProcessContext c){
                          KafkaRecord<String, String> element = c.element();
                          c.output(KV.of("testqueue", element.getKV().getValue()));
                        }
                      }))
                    .apply("WriteToQueue",
                            KafkaIO.<String, String>write()
                                    .withBootstrapServers("kafka-dev.bhinnekalocal.com:9092,kafka-dev-2.bhinnekalocal.com:9092")
                                    .withTopic("testqueue")
                                    .withKeySerializer(StringSerializer.class)
                                    .withValueSerializer(StringSerializer.class)
                    );



    return pipeline.run();
  }

}
