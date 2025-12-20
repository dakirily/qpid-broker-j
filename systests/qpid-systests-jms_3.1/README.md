## Qpid Broker-J JMS 3.1 System Tests

This readme is intended for developers and contributors of Qpid Broker-J project to provide information about module `qpid-systests-jms_3.1`.

### Running the tests

Tests in this module can be started using command

```
mvn verify -pl systests/qpid-systests-jms_3.1 -DskipTests=false -DskipITs=false
```

### Test Profiles Support

Tests in this module are based on [Jakarta Messaging 3.1 Specification](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1). 
Therefore they require JMS entities (connection, session, consumer, producer etc) to be imported from `jakarta.jms` namespace.

Legacy QPID JMS Client supporting AMQP protocols 0.x does not support `jakarta.jms` namespace.

As a consequence tests in module `qpid-systests-jms_3.1` can be started only with the AMQP-1.0 associated maven 
test profiles: java-mms.1-0 (default), java-bdb.1-0 and java-dby.1-0.

### Test Coverage

Tests in module `qpid-systests-jms_3.1` are mostly based on appropriate tests in modules tests in module 
`qpid-systests-jms_1.1` and `qpid-systests-jms_2.0` with some additions and extensions.