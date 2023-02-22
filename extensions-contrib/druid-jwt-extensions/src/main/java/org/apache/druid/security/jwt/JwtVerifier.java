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

import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;

import java.util.List;

public class JwtVerifier
{
  private static final String CLAIM_GROUPS = "groups";

  private final JwtConsumer jwtConsumer;

  public JwtVerifier(
      String issuer,
      String jwksUri,
      List<String> audience

  )
  {
    this.jwtConsumer = createConsumer(issuer, jwksUri, audience);
  }

  private static JwtConsumer createConsumer(
      String issuer,
      String jwksUri,
      List<String> audience
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
      builder = builder.setExpectedAudience(audience.toArray(new String[0]));
    }
    return builder.build();

  }

  /**
   * Return JWT custom claim: groups
   */
  public static List<String> getClaimGroups(JwtClaims claims) throws InvalidGroupsClaimException
  {
    try {
      return claims.getStringListClaimValue(CLAIM_GROUPS);
    }
    catch (MalformedClaimException e) {
      throw new InvalidGroupsClaimException("Missing JWT Custom Claim: groups", e);
    }
  }

  /**
   * Caller/Client's groups claim is configured to uniquely identify caller itself.
   * Ideally every Caller/Client service would have a unique group that would be used for authorization.
   */
  public static String getCallerGroup(JwtClaims claims) throws InvalidGroupsClaimException
  {
    return String.join(",", getClaimGroups(claims));
  }

  public JwtClaims authenticate(String encodedJwt) throws InvalidJwtException
  {
    return jwtConsumer.processToClaims(encodedJwt);
  }

}
