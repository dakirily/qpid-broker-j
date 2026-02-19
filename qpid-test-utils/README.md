# Module qpid-test-utils

This module provides utility classes and JUnit extensions used in Broker-J unit tests.

## QpidUnitTestExtension

`QpidUnitTestExtension` is a JUnit extension that logs test method execution start and end, and
routes log output into per-test files using Logback's `SiftingAppender`.

### How it works

The extension sets a `classQualifiedTestName` property on the Logback `LoggerContext` at each stage
of the test lifecycle. `LogbackPropertyValueDiscriminator` reads this property and the
`SiftingAppender` uses it to select the target log file.

| Lifecycle phase | Property value               | Log file                                |
|-----------------|------------------------------|-----------------------------------------|
| `@BeforeAll`    | `com.example.MyTest`         | `TEST-com.example.MyTest.txt`           |
| `@BeforeEach`   | `com.example.MyTest.myTest`  | `TEST-com.example.MyTest.myTest.txt`    |
| test execution  | `com.example.MyTest.myTest`  | `TEST-com.example.MyTest.myTest.txt`    |
| `@AfterEach`    | `com.example.MyTest`         | `TEST-com.example.MyTest.txt`           |
| `@AfterAll`     | cleared                      | `TEST-testrun.txt` (default)            |

Log files are written to `target/surefire-reports/` (configurable via the `test.output.dir` system
property).

### Design note: LogbackPropertyValueDiscriminator vs MDC

The extension sets a property on the Logback `LoggerContext` (a process-wide property map) rather
than using MDC. MDC values are thread-local and would be lost in background threads, producing
incorrect log routing. Since production code frequently performs logging in background threads, the
context-property approach ensures all log output from a test — regardless of which thread produces
it — is routed to the correct file.

## UnitTestBase

`UnitTestBase` is a convenience base class for unit tests. It applies `QpidUnitTestExtension`
automatically and provides common utilities.

### Features

- Automatic test lifecycle logging via `@ExtendWith(QpidUnitTestExtension.class)`
- Port allocation: `findFreePort()`, `getNextAvailable(int)`
- System property management: `setTestSystemProperty(String, String)` with automatic restore
- Teardown registry: `registerAfterAllTearDown(Runnable)`
- Test name accessors: `getTestClassName()`, `getTestName()`

### Lifecycle

`UnitTestBase` uses `@TestInstance(Lifecycle.PER_CLASS)`, which means a single test instance is
created per test class. This allows non-static `@BeforeAll` and `@AfterAll` methods and enables
state sharing across test methods when needed.

## Usage

### Example: extending UnitTestBase

Most unit tests extend `UnitTestBase` to get logging and utilities automatically:

```java
class MyTest extends UnitTestBase
{
    @Test
    void testSomething()
    {
        final int port = findFreePort();
        // test using port ...
    }

    @Test
    void testWithSystemProperty()
    {
        setTestSystemProperty("my.property", "value");
        // property is restored automatically after the test
    }
}
```

### Example: using QpidUnitTestExtension directly

For tests that cannot extend `UnitTestBase`, apply the extension directly:

```java
@ExtendWith(QpidUnitTestExtension.class)
class MyStandaloneTest
{
    @Test
    void testSomething()
    {
        // test logic ...
    }
}
```

### Expected log files

Running the test class `org.example.MyTest` with methods `testA` and `testB` produces:

```
target/surefire-reports/
    TEST-org.example.MyTest.txt           # class-level log (beforeAll/afterAll, cleanup)
    TEST-org.example.MyTest.testA.txt     # method-level log for testA
    TEST-org.example.MyTest.testB.txt     # method-level log for testB
```

Each method-level log file contains:

```
... INFO  ... ========================= start executing test : MyTest#testA
... (test output) ...
... INFO  ... ========================= stop executing test : MyTest#testA
```
