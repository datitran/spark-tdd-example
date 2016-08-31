# A simple PySpark example using TDD

This is a very basic example of how to use Test Driven Development (TDD) in the context of PySpark, Spark's Python API.

### Getting Started

1. Use brew to install Apache Spark: `brew install apache-spark`
2. Change logging settings:
  - `cd /usr/local/Cellar/apache-spark/2.0.0/libexec/conf`
  - `cp log4j.properties.template log4j.properties`
  - Set info to error: `log4j.rootCategory=ERROR, console`
3. Add this to your bash profile: `export SPARK_HOME="/usr/local/Cellar/apache-spark/2.0.0/libexec/"`
4. Use nosetests to run the test: `nosetests -vs test_clustering.py`

## Dependencies
- [Apache Spark](http://spark.apache.org/) Spark 2.0.0
- [Python](https://www.python.org/) Python 3.5
- [nosetests](http://nose.readthedocs.io/en/latest/) nose 1.3.7

## Copyright

See [LICENSE](LICENSE) for details.
Copyright (c) 2016 [Dat Tran](http://www.dat-tran.com/).
