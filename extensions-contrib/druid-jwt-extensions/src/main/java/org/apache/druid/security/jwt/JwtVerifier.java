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

import org.apache.druid.java.util.common.logger.Logger;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;

import java.util.Set;

public class JwtVerifier
{
  private static final Logger LOG = new Logger(JwtVerifier.class);

  private final JwtConsumer JwtComsumer;

  public JwtVerifier(
      String issuer,
      String jwksUri,
      Set<String> audience

  )
  {
    this.JwtComsumer = createConsumer(issuer, jwksUri, audience);
  }

  private static JwtConsumer createConsumer(
      String issuer,
      String jwksUri,
      Set<String> audience
  )
  {
    JwtConsumerBuilder builder = new JwtConsumerBuilder().setExpectedIssuer(issuer)
                                                         .setVerificationKeyResolver(
                                                             new HttpsJwksVerificationKeyResolver(
                                                                 new HttpsJwks(jwksUri)
                                                             )
                                                         )
                                                         .setRequireExpirationTime()
                                                         .setRequireSubject();
    if (audience != null) {
      builder = builder.setExpectedAudience(audience.stream().toArray(String[]::new));
    }
    LOG.debug("JWT Consumer Setup");
    return builder.build();

  }

  public JwtClaims authenticate(String encodedJwt) throws InvalidJwtException
  {
    LOG.debug("verify JWT");
    return JwtComsumer.processToClaims(encodedJwt);
  }

}
