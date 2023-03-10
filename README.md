# kafka-streams-experiment

Some simple utils and code examples for Kafka Stream.


Stream scenarios :
- Simple String stream & testing
- Simple Avro object stream & testing (managing all kind of avro types)
- Stream error management example

Stream utils :
- PropertiesLoader : simple yml to Properties loader
- StreamContext : simple properties holder for application configuration
- AvroSerdes : equivalent class of Serdes to easily get a specific Avro model Serdes
- StreamException & MayBeException : models for code logic exception management
- StreamExceptionCatcher : topology helper to manage exception inside stream logic


More to come :

- Simple join
- Stream/Stream join with windowing
- Advanced joins conditions (Statestore)
- Punctuation example
- Optimised joins with GlobalKTable
