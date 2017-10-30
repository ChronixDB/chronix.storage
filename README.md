[![Build Status](https://travis-ci.org/ChronixDB/chronix.storage.svg)](https://travis-ci.org/ChronixDB/chronix.storage)
[![Coverage Status](https://coveralls.io/repos/github/ChronixDB/chronix.storage/badge.svg?branch=master)](https://coveralls.io/github/ChronixDB/chronix.storage?branch=master)
[![Apache License 2](http://img.shields.io/badge/license-ASF2-blue.svg)](https://github.com/ChronixDB/chronix.storage/blob/master/LICENSE)
[ ![Download](https://api.bintray.com/packages/chronix/maven/chronix-storage/images/download.svg) ](https://bintray.com/chronix/maven/chronix-storage/_latestVersion)

# Chronix Storage
The Chronix Storage is an implementation of the Chronix API that uses Apache Lucene.
Hence the Chronix Storage can be used with Chronix-Timeseries to store and query time series.

## Usage
The following code snipped shows how to create a chronix client that writes to a given lucene index.
```groovy
def chronix = new ChronixClient(new MetricTimeSeriesConverter<>(), new ChronixLuceneStorage(200, ChronixTimeSeriesDefaults.GROUP_BY, ChronixTimeSeriesDefaults.REDUCE))
def luceneIndex = new LuceneIndex(FSDirectory.open(Paths.get("build/lucene")), new StandardAnalyzer())


//Define a time series of type metric
def ts = new MetricTimeSeries.Builder("github stars","metric")
         .attribute("host", "groovy")
         .attribute("source", "readme.md")
         .build()

//Add values
ts.add(Instant.now().toEpochMilli(), 4711)

//Add the time series to the the index
chronix.add([ts] as List, luceneIndex)

//we do a hart commit - only for testing purposes
luceneIndex.getOpenWriter().commit()

//Stream time series that match the lucene query *:*
def timeSeriesStream = chronix.stream(luceneIndex, createQuery("*:*"))

//Create a proper lucene query
Query createQuery(String searchString) {
  QueryParser queryParser = new QueryParser("name", luceneIndex.getOpenWriter().getAnalyzer())
  return queryParser.parse(searchString)
}
```

## Contributing
Is there anything missing? Do you have ideas for new features or improvements? You are highly welcome to contribute
your improvements, to the Chronix projects. All you have to do is to fork this repository,
improve the code and issue a pull request.

## Building Chronix Storage from Scratch
Everything should run out of the box. The only three things that must be available:
- Git
- JDK 1.8
- Gradle

Just do the following steps:

```bash
cd <checkout-dir>
git clone https://github.com/ChronixDB/chronix.storage.git
cd chronix.storage
gradlew clean build
```

## Maintainer

Florian Lautenschlager @flolaut

## License

This software is provided under the Apache License, Version 2.0 license.

See the `LICENSE` file for details.
