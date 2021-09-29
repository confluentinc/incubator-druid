# OpenTracing Emitter

The OpenTelemetry emitter generates OpenTelemetry Spans for queries.


## Configuration

### Enabling

Load the plugin and enable the emitter in `common.runtime.properties`:

Load the plugin:

```
druid.extensions.loadList=[..., "opentelemetry-emitter"]
```

Enable the emitter:

```
druid.emitter=opentelemetry
```

*_Don't forget to remove the line_ `druid.emitter=noop` _to avoid rewriting_.

## Testing
###Zipkin
Run zipkin docker container
```
docker run -d -p 9411:9411 openzipkin/zipkin
```
Zipkin will be available at `http://localhost:9411`.

Add zipkin dependency to `extensions-contrib/opentelemetry-emitter/pom.xml`:
```
<dependency>
  <groupId>io.opentelemetry</groupId>
  <artifactId>opentelemetry-exporter-zipkin</artifactId>
  <version>${your.version}</version>
</dependency>
```

Run:

```
mvn package
tar -C /tmp -xf distribution/target/apache-druid-0.21.0-bin.tar.gz
cd /tmp/apache-druid-0.21.0
```


Edit `conf/druid/single-server/micro-quickstart/_common/common.runtime.properties` to enable
the emitter (see `Configuration` section above). Set the following properties:
```
otel.traces.exporter=zipkin
otel.service.name=druid
```
[More about OpenTelemetry SDK Autoconfigure](https://github.com/open-telemetry/opentelemetry-java/tree/main/sdk-extensions/autoconfigure)

Start the quickstart:

```
bin/start-micro-quickstart
```

Load sampel data - [example](https://druid.apache.org/docs/latest/tutorials/index.html#step-4-load-data)

Then you can send queries using `curl` and a query file `query.json`:
```
curl -XPOST -H'Content-Type: application/json' http://localhost:8888/druid/v2/sql/ -d @query.json
```

To test propagation run simple server which read query from stdin:
```
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import io.opentelemetry.sdk.autoconfigure.OpenTelemetrySdkAutoConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Main
{

  private static String process(String in) throws IOException
  {
    Map<String, Object> map = new HashMap<>();
    map.put("query", in);
    Map<String, Object> contextMap = new HashMap<>();

    Tracer tracer = GlobalOpenTelemetry.getTracer("Simple server");
    Span outGoing = tracer.spanBuilder("Simple server").setSpanKind(SpanKind.CLIENT).startSpan();
    TextMapSetter<Map<String, Object>> setter =
        (carrier, key, value) -> {
          // Insert the context as Header
          if (carrier != null) {
            carrier.put(key, value);
          }
        };


    CloseableHttpClient httpClient = HttpClientBuilder.create().build();

    String result = "";
    try (Scope scope = outGoing.makeCurrent()) {
      // store context
      GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), contextMap, setter);
      map.put("context", contextMap);
      ObjectMapper objectMapper = new ObjectMapper();
      ByteArrayOutputStream baot = new ByteArrayOutputStream();
      objectMapper.writeValue(baot, map);

      System.out.println("Request " + baot);

      // make request
      HttpPost request = new HttpPost("http://localhost:8888/druid/v2/sql/");
      StringEntity params = new StringEntity(baot.toString());
      request.addHeader("Content-Type", "application/json");
      request.setEntity(params);
      HttpResponse res = httpClient.execute(request);
      HttpEntity entity = res.getEntity();
      result = EntityUtils.toString(entity, "UTF-8").trim();
    }
    catch (Exception ex) {
      System.out.println(ex.getMessage());
    }
    finally {
      httpClient.close();
    }
    outGoing.end();
    return result;
  }

  public static void main(String[] args) throws IOException
  {
    System.setProperty("otel.service.name", "Simple server");
    System.setProperty("otel.traces.exporter", "zipkin");

    OpenTelemetrySdkAutoConfiguration.initialize();

    while (true) {
      Scanner scanner = new Scanner(System.in);
      String in = scanner.nextLine();
      if (in.equals("exit")) {
        break;
      }
      System.out.println(process(in));
    }
  }
}
```

###Otel Collector
Will be added.
