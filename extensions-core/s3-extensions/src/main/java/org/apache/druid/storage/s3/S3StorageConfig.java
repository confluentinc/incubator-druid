/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * General configurations for Amazon S3 storage.
 */
public class S3StorageConfig
{
  /**
   * Server-side encryption type. We use a short name to match the configuration prefix with {@link S3SSEKmsConfig} and
   * {@link S3SSECustomConfig}.
   *
   * @see S3StorageDruidModule#configure
   */
  @JsonProperty("sse")
  private final ServerSideEncryption serverSideEncryption;

  /**
   * S3 client config. We use a short name to match the configuration prefix with {@link S3ClientConfig}
   *
   * @see S3StorageDruidModule#configure
   */
  @JsonProperty("client")
  private final S3ClientConfig s3ClientConfig;
    
  @JsonCreator
  public S3StorageConfig(
      @JsonProperty("sse") ServerSideEncryption serverSideEncryption,
      @JsonProperty("client") S3ClientConfig s3ClientConfig
  )
  {
    this.serverSideEncryption = serverSideEncryption == null ? new NoopServerSideEncryption() : serverSideEncryption;
    this.s3ClientConfig = s3ClientConfig;
  }

  @JsonProperty("sse")
  public ServerSideEncryption getServerSideEncryption()
  {
    return serverSideEncryption;
  }

  @JsonProperty("client")
  public S3ClientConfig getS3ClientConfig()
  {
    return s3ClientConfig;
  }
    
}
