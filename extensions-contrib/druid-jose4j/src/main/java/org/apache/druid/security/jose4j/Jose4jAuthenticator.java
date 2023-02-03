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

package org.apache.druid.security.jose4j;

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
import java.util.Map;
import java.util.Set;

@JsonTypeName("jose4j")
public class Jose4jAuthenticator implements Authenticator
{
  private static final Logger LOG = new Logger(Jose4jAuthenticator.class);

  private final String name;
  private final String authorizerName;
  private final String issuer;
  private final Set<String> audience;
  private final String jwksUri;
  private final boolean skipOnFailure;

  private final JwtAuthenticator jwtAuthenticator;

  @JsonCreator
  public Jose4jAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("issuer") String issuer,
      @JsonProperty("audience") Set<String> audience,
      @JsonProperty("jwksUri") String jwksUri,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("skipOnFailure") Boolean skipOnFailure
  )
  {
    this.name = Preconditions.checkNotNull(name, "null Name");
    this.issuer = Preconditions.checkNotNull(issuer, "null issuer");
    this.jwksUri = Preconditions.checkNotNull(jwksUri, "null jwksUri");
    this.authorizerName = authorizerName;
    this.audience = audience;
    this.skipOnFailure = skipOnFailure == null ? false : skipOnFailure;
    this.jwtAuthenticator = new JwtAuthenticator(issuer, jwksUri, audience);
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
    String bearerToken = (String) context.get("token");

    if (bearerToken != null) {
      try {
        JwtClaims jwtClaims = jwtAuthenticator.authenticate(bearerToken);
        if (jwtClaims != null) {
          LOG.info("JWT validation succeeded!");
          return new AuthenticationResult(
              jwtClaims.getSubject(),
              authorizerName,
              name,
              null
          );
        }
      }
      catch (InvalidJwtException | MalformedClaimException ex) {
        LOG.error(ex, "Invalid Credentails");
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
      try {
        JwtClaims jwtClaims = jwtAuthenticator.authenticate(bearerToken);
        if (jwtClaims != null) {
          LOG.info("JWT validation succeeded!");
          AuthenticationResult authenticationResult = new AuthenticationResult(
              jwtClaims.getSubject(),
              authorizerName,
              name,
              null
          );
          servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
          filterChain.doFilter(servletRequest, servletResponse);
        } else {
          if (skipOnFailure) {
            LOG.info("Skipping failed authenticator %s ", name);
            filterChain.doFilter(servletRequest, servletResponse);
          } else {
            httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
          }
        }
      }
      catch (InvalidJwtException | MalformedClaimException ex) {
        LOG.error(ex, "Invalid Credentails");
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Bearer authentication failed, Invalid JWT");
      }
    }

    @Override
    public void destroy()
    {

    }
  }

}
