# Vaccination-Rate-Hadoop

This project implements a batch processing job using Apache Hadoop MapReduce to calculate average MMR vaccination rates by state from a CSV file. It leverages Cassandra as a NoSQL database to store the results.

## Project Setup
Prerequisites:
Apache Hadoop <br/>
Apache Cassandra <br/>
Maven <br/>


## Testing
The provided build.sh script automates building the JAR and copying it to the Hadoop master node. Simply run:

```
$ bash build.sh
```
