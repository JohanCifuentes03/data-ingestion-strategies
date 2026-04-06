/**
 * Batch processing implementation using Apache Spark.
 * <p>
 * Reads accumulated events from Kafka and processes them in a single batch execution.
 * Implements the classic MapReduce paradigm with high throughput but higher latency.
 * 
 * <h2>Strategy Characteristics</h2>
 * <ul>
 *   <li><b>Processing Model:</b> Batch (MapReduce)</li>
 *   <li><b>Latency:</b> High (minutes to hours)</li>
 *   <li><b>Throughput:</b> Very High</li>
 *   <li><b>Resource Efficiency:</b> High (optimized for large batches)</li>
 * </ul>
 *
 * @see org.tesis.batch.SparkBatchJob
 * @since 1.0
 */
package org.tesis.batch;
