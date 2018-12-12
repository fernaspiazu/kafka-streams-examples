## Setup & Run

In order to run this example, is sufficient to have a single kafka broker with the following topics:

- favourite-colour-input
- user-colour-intermediary
- favourite-colour-output

The following commands are needed to create the topics above:

```bash
$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colour-input
$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic user-colour-intermediary --config cleanup.policy=compact
$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colour-output --config cleanup.policy=compact
```

Once topics are created, clone, clean & package the `favourite-colour` module:

```bash
$ git clone git@github.com:fernaspiazu/kafka-streams-examples.git
$ cd kafka-streams-examples/favourite-colour
$ mvn clean package
$ java -jar target/favourite-colour-1.0-jar-with-dependencies.jar
```
