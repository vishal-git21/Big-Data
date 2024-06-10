# Big Data Course Projects

This repository contains code developed during the Big Data course. It includes assignments and projects using Apache Spark and Apache Kafka.

## Contents

1. **Spark Assignment**
   - `spark-solution`: This file contains code for performing basic transformations, aggregations, and other common computations using Apache Spark.

2. **Kafka Assignment**
   - `producer`: This file produces three types of messages (likes, comments, and shares) related to a post.
   - `consumer1,consumer2,consumer3` : These files simulate the consumption of messages about likes, comments, and shares depending on the task at hand

## Spark Assignment

The `spark-solution` file includes the following operations using Apache Spark:
- Basic transformations: map, filter, flatMap, etc.
- Aggregations: groupBy, reduceByKey, aggregate, etc.
- Other common computations: join, union, etc.

### Running the Spark Assignment

1. Ensure you have Apache Spark installed.
2. Submit the Spark job using:
   ```bash
   spark-submit spark-solution.py
   ```

## Kafka Assignment

The Kafka assignment involves producing and consuming messages related to social media interactions (likes, comments, shares).

### Producer

- **File**: `producer`
- **Description**: This file simulates the production of messages about likes, comments, and shares.

### Consumers

- **Description**: These files simulate the consumption of messages about likes, comments, and shares depending on the task at hand

