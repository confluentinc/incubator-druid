---
id: druid-jwt-extensions
title: "Druid JWT Extension"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

Apache Druid Extension to enable Authentication for Druid Processes using JWT. This extension adds an Authenticator
which decodes and verifies JWT.
To load the extension, [include](../../development/extensions.md#loading-extensions) `druid-jwt-extensions` in the
extensions load list.

## Configuration

### Creating an Authenticator

```
druid.auth.authenticatorChain=["MyJwtAuthenticator"]

druid.auth.authenticator.MyJwtAuthenticator.type=jwt_jwks
```

To use the JWT authenticator, add an authenticator with type `jwt_jwks` to the authenticatorChain. The example above
uses the name "MyJwtAuthenticator" for the Authenticator.

Configuration of the named authenticator is assigned through properties with the form:

```
druid.auth.authenticator.<authenticatorName>.<authenticatorProperty>
```

The configuration examples in the rest of this document will use "jwt" as the name of the authenticator being
configured.

### Properties

| Property                                           | Description                                                     | Default | required                                        |
|----------------------------------------------------|-----------------------------------------------------------------|---------|-------------------------------------------------|
| `druid.auth.authenticator.jwt.audience`            | Recipients for which the JWT is intended.                       | none    | If `aud` claim is present in JWT, it's required |
| `druid.auth.authenticator.jwt.issuer`              | Issuer of the JWT                                               | none    | yes                                             |
| `druid.auth.authenticator.jwt.jwksUri`             | JWKS endpoint provided by identity provider                     | none    | yes                                             |
| `druid.auth.authenticator.jwt.jwtGroupIdentityMap` | Map JWT custom `groups` Claim to identity used in authorization | none    | No                                              |
| `druid.auth.authenticator.jwt.authorizerName`      | Authorizer that requests should be directed to                  | none    | Yes                                             |

Example configuration of an authenticator
```
druid.auth.authenticator.jwt.audience=["audA", "audB"]
druid.auth.authenticator.jwt.issuer=https://--YOUR DOMAIN----/
druid.auth.authenticator.jwt.jwksUri=https://--YOUR DOMAIN----/.well-known/jwks.json
druid.auth.authenticator.jwt.jwtGroupIdentityMap={"groupA": "authZuserA"}
druid.auth.authenticator.jwt.authorizerName=basic
```

#### druid.auth.authenticator.jwt.jwtGroupIdentityMap
This property maps JWT custom `groups` claim to the identity used in authorization. Ideally every Caller/Client service would have a unique `group`
so the authenticator would uniquely identify the caller. For a set of users within same group, this property would map
those JWTs to same identity in the authorizer. If property is not set, JWT `sub` claim is used as the identity in the authorizer. 


