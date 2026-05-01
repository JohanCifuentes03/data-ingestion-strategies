/**
 * Common utilities and domain models for data ingestion benchmark jobs.
 * <p>
 * This package provides:
 * <ul>
 *   <li>{@link org.tesis.common.Event} - Domain model for ingested events</li>
 *   <li>{@link org.tesis.common.ConfigLoader} - Configuration parsing utilities</li>
 *   <li>{@link org.tesis.common.JdbcEventWriter} - JDBC batch writer with idempotency</li>
 * </ul>
 * <p>
 * All ingestion strategies (batch, micro-batch, streaming) depend on this common module.
 *
 * @since 1.0
 */
package org.tesis.common;
