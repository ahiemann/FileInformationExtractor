# FileInformationExtractor
[![Build Status](https://travis-ci.org/ahiemann/FileInformationExtractor.svg?branch=main)](https://travis-ci.org/ahiemann/FileInformationExtractor)
[![Coverage Status](https://coveralls.io/repos/github/ahiemann/FileInformationExtractor/badge.svg?branch=main)](https://coveralls.io/github/ahiemann/FileInformationExtractor?branch=main)

A Scala program that uses Apache Tika in conjunction with Alpakka and Akka streams to extract fulltext and metadata 
from files in a (currently hardcoded) directory. Apache Kafka is used as a sink for these data.
Then Apache Spark is used to create statistics about the extracted information. 

This data could be used in the context of a document archive system that allows to find a document
by searching for keywords or metadata of the document, e.g. using Elasticsearch in the backend.  

The Apache Kafka instance has to be started manually using "docker-compose -f zk-single-kafka-single.yml up" 
in the "kafka" directory of the project. 