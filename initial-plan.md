we're going to create a java library called "postka", a portmanteau of kafka and postgres.  the library will provide an
interface that matches the kafka clients (producer, consumer, admin), but rather than being backed by kafka brokers, it
will connect to a postgres database.  it should be a drop in replacement for the latest version of the existing java
kafka libraries, with parallels for important interfaces like KafkaProducer, KafkaConsumer, etc.  all names, including
packages and classes should be identical except the word "Kafka" should be replaced with the word "Postka", such that
a simple find and replace can be used to swap one for the other.  eventually 

the primary goal is functional parity, NOT performance. you should reference kafka documentation extensively as need to
maintain feature parity.  to confidently achieve feature parity, the project will have extensive junit test coverage
against real running instances of kafka brokers and postgres using the testcontainers library.

flyway migrations should be used to manage any SQL objects in postgres required to set up.  these migrations will run
at the start of tests obviously, but need to be production-ready so an operator of postka can run a setup script against
postgres.

this project should use java 25 and gradle for building.  

to start, build the "hello world" test suite for postka:
 - create a simple set of tables in postgres that represent the metadata and record storage necessary for topics/partitions/etc.
 - write the PostkaProducer and PostkaConsumer interfaces and implementing classes that write to and read from postgres
 - create testcontainers unit tests that show the simplest possible feature parity with the real kafka classes

try to keep everything as simple as possible initially, and just get the running hello world tests.