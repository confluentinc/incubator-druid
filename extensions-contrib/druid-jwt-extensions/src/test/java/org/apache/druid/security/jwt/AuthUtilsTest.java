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

package org.apache.druid.security.jwt;

import org.easymock.EasyMock;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AuthUtilsTest
{
  private static final String AUTHORIZATION_HEADER = "Authorization";

  @Test
  public void testGetBearerTokenFromHttpReq()
  {
    String bearerToken = "TestToken1234";
    HttpServletRequest httpReq = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(httpReq.getHeader(AUTHORIZATION_HEADER))
            .andReturn("Bearer " + bearerToken)
            .andReturn(bearerToken);
    EasyMock.replay(httpReq);
    String bearerResult = AuthUtils.getBearerTokenFromHttpReq(httpReq);
    assertEquals(bearerResult, bearerToken);
    String nullResult = AuthUtils.getBearerTokenFromHttpReq(httpReq);
    assertNull(nullResult);
  }
}
