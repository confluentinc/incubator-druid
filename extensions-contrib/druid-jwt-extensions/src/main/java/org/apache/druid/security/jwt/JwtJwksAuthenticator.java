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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("jwt_jwks")
public class JwtJwksAuthenticator implements Authenticator
{
  private static final Logger LOG = new Logger(JwtJwksAuthenticator.class);

  private final String name;
  private final String authorizerName;
  private final String issuer;
  private final List<String> audience;
  private final String jwksUri;

  private final JwtVerifier jwtVerifier;
  private final IssuerExtractor issuerExtractor;

  private final Map<String, String> jwtGroupIdentityMap;

  @JsonCreator
  public JwtJwksAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("issuer") String issuer,
      @JsonProperty("audience") List<String> audience,
      @JsonProperty("jwksUri") String jwksUri,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("jwtGroupIdentityMap") Map jwtGroupIdentityMap
  )
  {
    this.name = Preconditions.checkNotNull(name, "null Name");
    this.issuer = Preconditions.checkNotNull(issuer, "null issuer");
    this.jwksUri = Preconditions.checkNotNull(jwksUri, "null jwksUri");
    this.authorizerName = authorizerName;
    this.audience = audience;
    this.jwtGroupIdentityMap = jwtGroupIdentityMap;
    this.jwtVerifier = new JwtVerifier(issuer, jwksUri, audience);
    this.issuerExtractor = new IssuerExtractor();
  }

  /**
   * Map JWT groups claim to the identity of the requester which is used in authorizer.
   * Ideally, Caller/Client's groups claim is configured to uniquely identify caller itself.
   * If Groups claim isn't configured, we return the sub claim (Subject of the JWT) as the identity of the requester.
   */
  private static String claimToIdentity(
      Map<String, String> jwtGroupIdentityMap, JwtClaims jwtClaims
  ) throws MalformedClaimException
  {
    if (jwtGroupIdentityMap == null) {
      return jwtClaims.getSubject();
    } else {
      try {
        String groupIdentity = JwtVerifier.getCallerGroup(jwtClaims);
        String identity = jwtGroupIdentityMap.get(groupIdentity);
        if (identity == null) {
          return jwtClaims.getSubject();
        }
        return identity;
      }
      catch (InvalidGroupsClaimException e) {
        return jwtClaims.getSubject();
      }
    }
  }

  @Override
  public Filter getFilter()
  {
    return new HTTPAuthenticationFilter();
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return HTTPAuthenticationFilter.class;
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return "Bearer";
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return null;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

  @Override
  @Nullable
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    String bearerToken = (String) context.get("bearer-token");

    if (bearerToken != null) {
      try {
        JwtClaims jwtClaims = jwtVerifier.authenticate(bearerToken);
        if (jwtClaims != null) {
          LOG.info("JWT validation succeeded!");
          return new AuthenticationResult(claimToIdentity(jwtGroupIdentityMap, jwtClaims), authorizerName, name, null);
        }
      }
      catch (InvalidJwtException e) {
        LOG.error(e, "Invalid Credentails");
      }
      catch (MalformedClaimException e) {
        LOG.error(e, "Missing JWT Claim: sub");
      }
    }
    return null;
  }

  public class HTTPAuthenticationFilter implements Filter
  {
    @Override
    public void init(FilterConfig filterConfig)
    {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
        throws IOException, ServletException
    {
      HttpServletResponse httpResp = (HttpServletResponse) servletResponse;
      String bearerToken = AuthUtils.getBearerTokenFromHttpReq((HttpServletRequest) servletRequest);
      if (bearerToken == null) {
        // Request didn't have HTTP bearer token credentials, move on to the next filter
        filterChain.doFilter(servletRequest, servletResponse);
        return;
      }

      // filter JWT verifier based on issuer
      // if issuer specified doesn't match to issuer from JWT then move on to the next filter
      try {
        String extractedIssuer = issuerExtractor.getIssuer(bearerToken);
        if (!Objects.equals(extractedIssuer, issuer)) {
          filterChain.doFilter(servletRequest, servletResponse);
          return;
        }
      }
      catch (InvalidJwtException | MalformedClaimException e) {
        LOG.error(e, "Invalid JWT Issuer");
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid JWT Issuer");
        return;
      }

      // If any jwt authentication error occurs, we send a 401 response immediately
      // and do not proceed further down the filter chain.
      try {
        JwtClaims jwtClaims = jwtVerifier.authenticate(bearerToken);
        LOG.info("JWT validation succeeded!");
        // TODO add useful JWT claims into the context filed of AuthenticationResult
        AuthenticationResult authenticationResult = new AuthenticationResult(claimToIdentity(
            jwtGroupIdentityMap,
            jwtClaims
        ), authorizerName, name, null);
        servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
        filterChain.doFilter(servletRequest, servletResponse);
      }
      catch (InvalidJwtException e) {
        LOG.error(e, "Invalid Credentails");
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Bearer authentication failed, Invalid JWT");
      }
      catch (MalformedClaimException e) {
        httpResp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing JWT Claim: sub");
        LOG.error(e, "Missing JWT Claim: sub");
      }
    }

    @Override
    public void destroy()
    {

    }
  }

}
