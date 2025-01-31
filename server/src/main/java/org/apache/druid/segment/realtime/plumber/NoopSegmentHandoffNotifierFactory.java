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

package org.apache.druid.segment.realtime.plumber;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.handoff.SegmentHandoffNotifier;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;

import java.util.concurrent.Executor;

public class NoopSegmentHandoffNotifierFactory implements SegmentHandoffNotifierFactory
{
  private static final Logger log = new Logger(NoopSegmentHandoffNotifierFactory.class);

  private static final SegmentHandoffNotifier NOTIFIER = new SegmentHandoffNotifier()
  {
    @Override
    public boolean registerSegmentHandoffCallback(SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable)
    {
      log.info("Not waiting for segment to be handed off, executing handOffRunnable");
      exec.execute(handOffRunnable);
      return true;
    }

    @Override
    public void start()
    {
    }

    @Override
    public void close()
    {
    }
  };

  @Override
  public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource, String taskId)
  {
    return NOTIFIER;
  }
}
