/**
 * Micro-batch processing implementation using Spark Structured Streaming.
 * <p>
 * Processes events in small batches with configurable trigger intervals.
 * Balances latency and throughput using mini-batch streaming.
 * 
 * <h2>Strategy Characteristics</h2>
 * <ul>
 *   <li><b>Processing Model:</b> Micro-batch (mini-batch streaming)</li>
 *   <li><b>Latency:</b> Medium (seconds to minutes)</li>
 *   <li><b>Throughput:</b> High</li>
 *   <li><b>Resource Efficiency:</b> Medium</li>
 * </ul>
 *
 * @see org.tesis.microbatch.SparkStructuredJob
 * @since 1.0
 */
package org.tesis.microbatch;
