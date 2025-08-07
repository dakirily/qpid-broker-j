# Apache Qpid Broker-J

[![License](https://img.shields.io/github/license/apache/qpid-broker-j)](https://github.com/apache/qpid-broker-j/blob/main/LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.qpid/qpid-broker.svg)](https://central.sonatype.com/artifact/org.apache.qpid/qpid-broker)
[![Build Status](https://github.com/apache/qpid-broker-j/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/apache/qpid-broker-j/actions/workflows/build.yml/?branch=main)
[![Build Status](https://ci-builds.apache.org/buildStatus/icon?job=Qpid%2FQpid-Broker-J-TestMatrix&subject=Apache%20Jenkins)](https://ci-builds.apache.org/job/Qpid/job/Qpid-Broker-J-TestMatrix/)
[![Docker Image Size](https://img.shields.io/docker/image-size/apache/qpid-broker-j?logo=docker)](https://hub.docker.com/r/apache/qpid-broker-j)
___
[Website](https://qpid.apache.org/) |
[Wiki](https://cwiki.apache.org/confluence/display/qpid) |
[Documentation](https://qpid.apache.org/documentation.html) |
[Developer Mailing List](mailto:dev@qpid.apache.org) ([Archive](https://lists.apache.org/list.html?dev@qpid.apache.org)) |
[User Mailing List](mailto:users@qpid.apache.org) ([Archive](https://lists.apache.org/list.html?users@qpid.apache.org)) |
[Open Issues](https://issues.apache.org/jira/issues/?jql=project%20%3D%20QPID%20AND%20resolution%20%3D%20Unresolved%20AND%20component%20%3D%20Broker-J%20ORDER%20BY%20key%20DESC)
___

# Qpid Broker-J

The Apache Qpid Broker-J is a powerful open-source message broker.

* Supports Advanced Message Queuing Protocol (AMQP) versions 0-8, 0-9, 0-91, 0-10 and 1.0
* 100% Java implementation
* Authentication options include for LDAP, Kerberos, O-AUTH2, TLS client-authentication and more
* Message storage options include Apache Derby, Oracle BDB JE, and Generic JDBC
* REST and AMQP 1.0 management API
* Web-management console
* Plug-able architecture

Below are some quick pointers you might find useful.

## Building the code

The [Quick Start Guide](doc/developer-guide/src/main/markdown/quick-start.md) walks you through the steps required
to build, test and run Qpid Broker-J.

The [Build Instructions](doc/developer-guide/src/main/markdown/build-instructions.md) cover all details behind building
and testing.

## Running the Broker

For full details, see the `Getting Started` in User documentation mentioned below.

For convenience, the brief instructions are repeated in the
[Quick Start Guide](doc/developer-guide/src/main/markdown/quick-start.md).

### IDE Integration

Tips on setting up IDE to work with Qpid Broker-J project are provided in
[IDE Integration](doc/developer-guide/src/main/markdown/ide-integration.md).

## Documentation

Documentation (in docbook format) is found beneath the *doc* module.

Links to latest published User documentation can be found in overview of
[Broker-J Component](http://qpid.apache.org/components/broker-j/index.html).

Please check [Developer Guide](doc/developer-guide/src/main/markdown/index.md) for developer documentation.
