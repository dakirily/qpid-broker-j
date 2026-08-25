<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Embedded Broker-J test support

This module starts a memory-backed Broker-J instance inside the application's JVM. It is intended for tests that
need a real AMQP broker without managing a separate process or container.

Add `qpid-broker-embedded` as a test dependency. The module includes the memory store and AMQP 1.0 protocol at
runtime. JUnit remains optional, so the broker can also be used by another test framework.

## Manual lifecycle

```java
try (EmbeddedQpidBroker broker = EmbeddedQpidBroker.builder()
        .virtualHost("application-test")
        .credentials("test-user", "test-password")
        .exchange("events", ExchangeType.TOPIC)
        .queue("orders")
        .binding("events", "orders", "order.*")
        .build())
{
    broker.start();
    final URI amqpUri = broker.getAmqpUri();
    // Run the application test against amqpUri.
}
```

The default port is `0`, which asks the operating system to select an available loopback port. Queue and exchange
overloads accepting a `Map<String, Object>` allow additional Broker-J configured-object attributes.

## JUnit Jupiter

```java
@RegisterExtension
public static final EmbeddedQpidBrokerExtension BROKER = EmbeddedQpidBrokerExtension.builder()
        .exchange("events", ExchangeType.FANOUT)
        .queue("orders")
        .binding("events", "orders", "")
        .build();

@Test
void testApplication(final EmbeddedQpidBroker broker)
{
    final URI amqpUri = broker.getAmqpUri();
    // The broker parameter is supplied by the extension.
}
```

The extension is class-scoped: it starts the broker before the test class and stops it afterwards. Register it in a
`static final` field when using `@RegisterExtension`.

AMQP 0-x values can be selected with `protocols(...)`, but the matching Broker-J protocol plug-in must also be on
the test runtime class path.
