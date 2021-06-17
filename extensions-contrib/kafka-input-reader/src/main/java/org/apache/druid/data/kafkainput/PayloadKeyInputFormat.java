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

package org.apache.druid.data.kafkainput;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;

import java.io.File;
import java.util.Objects;

public class PayloadKeyInputFormat implements InputFormat
{
  private static final String DEFAULT_RESOURCE_PREFIX = "resource.";

  private final String resourceLabelPrefix;

  public PayloadKeyInputFormat(
      @JsonProperty("resourceLabelPrefix") String resourceLabelPrefix
  )
  {
    this.resourceLabelPrefix = resourceLabelPrefix != null ? resourceLabelPrefix : DEFAULT_RESOURCE_PREFIX;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return null;
    /*try {
      //Object myInstance = Class.forName("MyClass").newInstance();
      //https://stackoverflow.com/questions/1268817/create-new-object-from-a-string-in-java
      String deseralizer = this.valueType.equals("String") ? "StringDeserializer" : this.valueType;
      InputFormat myInstance = (InputFormat) Class.forName(this.valueType).newInstance();
      return myInstance.createReader(inputRowSchema, source, temporaryDirectory);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
    }*/
  }

  @JsonProperty
  public String getResourceLabelPrefix()
  {
    return resourceLabelPrefix;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PayloadKeyInputFormat)) {
      return false;
    }
    PayloadKeyInputFormat that = (PayloadKeyInputFormat) o;
    return Objects.equals(resourceLabelPrefix, that.resourceLabelPrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(resourceLabelPrefix);
  }
}
