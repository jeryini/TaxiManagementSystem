# Taxi management system
A server side system for **reading taxi trips data** and distributing its processing. The following consecutive operations are executed by the system:

1. receives trip as event
2. checks for errors in received trip
3. saves the trip into the database
4. computes query 1 and query 2 and outputs changes into file

The project is basically a solution of the [DEBS 2015 Grand Challenge](http://www.debs2015.org/call-grand-challenge.html). So for a complete understanding
of the solution for query 1 and query 2 read the description on the provided link. The New York city taxi
data was [made public by the Chris Wong](http://chriswhong.com/open-data/foil_nyc_taxi/). The system is *horizontally and vertically scalable* and *resilient to partial outages*.

## Purpose
This project was done for a Master's degree at [Faculty of Computer and Information Science](http://www.fri.uni-lj.si/en/), [University of Ljubljana](http://www.uni-lj.si/eng/). We did a comparison between the following architectures:
* EDA - event driven architecture
* SEDA - staged event driven architecture
* AEDA - actor based event driven architecture
* hypothetical ASEDA - actor based staged event driven architecture
* DASEDA - distributed actor based staged event driven architecture

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
The main class from which all different types of Architecture extend is Architecture.java. The class is abstract and has 
a single abstract method run() which all implementations that extend Architecture must implement. The solution contains a 
short but inefficient EDAPrimer.java. It is an example of correct implementation for query 1 and query 2 and is used for 
comparing the output with other implementations.

### Testing
The test cases define a check to see if the output from the different implementation for query 1 and query 2 are consistent.
There is also a test case to check that multiple consecutive runs on the same object result in equal results.

### Running


## TODO
* Update README.md

