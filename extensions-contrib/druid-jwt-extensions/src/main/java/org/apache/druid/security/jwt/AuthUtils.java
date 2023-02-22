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

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

public class AuthUtils
{
  private static final String AUTHORIZATION_HEADER = "Authorization";

  @Nullable
  public static String getBearerTokenFromHttpReq(HttpServletRequest httpReq)
  {
    String authHeader = httpReq.getHeader(AUTHORIZATION_HEADER);

    if (authHeader != null && authHeader.startsWith("Bearer")) {
      // the RFC for bearer tokens specifies that it could be one or more leading spaces
      // https://datatracker.ietf.org/doc/rfc6750/
      // credentials = "Bearer" 1*SP b64token
      return authHeader.substring(6).trim();
    }
    return null;
  }

}
