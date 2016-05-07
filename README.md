# A simple PySpark example using TDD

This is a very basic example of how to use Test Driven Development (TDD) in the context of PySpark, Spark's Python API.

Author: Dat Tran

License: See LICENSE.txt

### Getting Started

1. Use brew to install Apache Spark: `brew install apache-spark`
2. Change logging settings:
  - `cd /usr/local/Cellar/apache-spark/1.6.1/libexec/conf`
  - `cp log4j.properties.template log4j.properties`
  - Set info to error: `log4j.rootCategory=ERROR, console`
3. Add this to your bash profile: `export SPARK_HOME="/usr/local/Cellar/apache-spark/1.6.1/libexec/"`
4. Use nosetests to run the test: `nosetests -vs test_clustering.py`
