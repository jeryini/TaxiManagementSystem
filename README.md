# Traffic management system
A server side system for **reading simulation traffic data** and distributing its processing. The system reads traffic data, checks for correct data and saves it into the database. It also updates statistics. The system is *horizontally and vertically scalable* and *resilient to partial outages*.

## Purpose
This project was done for a Master's degree at [Faculty of Computer and Information Science](http://www.fri.uni-lj.si/en/), [University of Ljubljana](http://www.uni-lj.si/eng/). We did a comparison between three different architectures:
* EDA
* SEDA
* hypothetical ASEDA

The system was designed and build as a Reactive Streams System providing asynchronous stream processing with non-blocking back pressure. We have extensively followed [Reactive Manifesto](http://www.reactivemanifesto.org/) as well as [Reactive Streams](http://www.reactive-streams.org/) initiative. The project builds on top of [Reactor framework](http://projectreactor.io/). The reason for following the reactive programming paradigm was, because we wanted to extend [SEDA architecture](http://www.eecs.harvard.edu/~mdw/proj/seda/) with Actor based model. SEDA already defines the importance of back pressure and scalability so the step toward Actor based model and Reactive Streams was a logical one.

## General Requirements
This solution **requires** the following systems:
* [Java SE Runtime Environment 8](http://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html)
 The logic of the system is written in JAVA programming language.
* [MySQL](http://www.mysql.com/)
 The MySQL database for storing traffic tickets. **Version 5.7**

### Required libraries
Project uses Gradle-based build system and Maven to define dependencies on third party libraries. Nonetheless here is the list of required libraries:

* [MySQL Java Connector](http://dev.mysql.com/downloads/connector/j/)
Java connector for MySQL.
* [Reactor library](https://github.com/reactor/reactor)
All three different architectures are build on top of the Reactor library.
* [Commons CLI](http://commons.apache.org/proper/commons-cli/)
API for parsing command line options.

## General solution
TODO

### Running
TODO

## TODO
* Update README.md

