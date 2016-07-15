/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.solr

import de.qaware.chronix.ChronixClient
import de.qaware.chronix.converter.KassiopeiaSimpleConverter
import de.qaware.chronix.converter.common.DoubleList
import de.qaware.chronix.converter.common.LongList
import de.qaware.chronix.lucene.client.ChronixLuceneStorage
import de.qaware.chronix.lucene.client.LuceneIndex
import de.qaware.chronix.timeseries.MetricTimeSeries
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query
import org.apache.lucene.store.FSDirectory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths
import java.text.DecimalFormat
import java.util.function.BinaryOperator
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Tests the integration of Chronix with Lucene
 *
 * @author f.lautenschlager
 */
class ChronixClientTestIT extends Specification {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronixClientTestIT.class);

    //Test subjects
    @Shared
    ChronixClient<MetricTimeSeries, LuceneIndex, Query> chronix

    @Shared
    LuceneIndex luceneIndex;

    @Shared
    def listStringField = ["List first part", "List second part"]
    @Shared
    def listIntField = [1I, 2I]
    @Shared
    def listLongField = [11L, 25L]
    @Shared
    def listDoubleField = [1.5D, 2.6D]


    @Shared
    Function<MetricTimeSeries, String> groupBy = new Function<MetricTimeSeries, String>() {
        @Override
        String apply(MetricTimeSeries ts) {
            StringBuilder metricKey = new StringBuilder();

            metricKey.append(ts.attribute("host")).append("-")
                    .append(ts.attribute("source")).append("-")
                    .append(ts.attribute("group")).append("-")
                    .append(ts.getMetric());

            return metricKey.toString();
        }
    }

    @Shared
    BinaryOperator<MetricTimeSeries> reduce = new BinaryOperator<MetricTimeSeries>() {
        @Override
        MetricTimeSeries apply(MetricTimeSeries t1, MetricTimeSeries t2) {
            t1.addAll(t2.getTimestampsAsArray(), t2.getValuesAsArray())
            t1.getAttributesReference().putAll(t2.getAttributesReference())
            return t1;
        }
    }

    def DoubleList concat(DoubleList first, DoubleList second) {
        first.addAll(second)
        first
    }

    def LongList concat(LongList first, LongList second) {
        first.addAll(second)
        first
    }

    def setupSpec() {
        given:
        LOGGER.info("Setting up the integration test.")
        chronix = new ChronixClient(new KassiopeiaSimpleConverter<>(), new ChronixLuceneStorage(200, groupBy, reduce))
        Path path = Paths.get("build/lucene");
        def directory = FSDirectory.open(path);
        def analyzer = new StandardAnalyzer();
        luceneIndex = new LuceneIndex(directory, analyzer)


        when: "We first clean the index to ensure that no old data is loaded."
        luceneIndex.getOpenWriter().deleteAll()
        luceneIndex.getOpenWriter().commit()

        LOGGER.info("Adding data to Chronix.")
        importTimeSeriesData();
        //we do a hart commit - only for testing purposes
        luceneIndex.getOpenWriter().commit()

        then:
        true


    }

    def importTimeSeriesData() {
        def url = ChronixClientTestIT.getResource("/timeSeries");
        def tsDir = new File(url.toURI())

        tsDir.listFiles().each { File file ->
            LOGGER.info("Processing file {}", file)
            def documents = new HashMap<Integer, MetricTimeSeries>()

            def attributes = file.name.split("_")
            def onlyOnce = true
            def nf = DecimalFormat.getInstance(Locale.ENGLISH);

            def filePoints = 0

            file.splitEachLine(";") { fields ->
                //Its the first line of a csv file
                if ("Date" == fields[0]) {
                    if (onlyOnce) {
                        fields.subList(1, fields.size()).eachWithIndex { String field, int i ->
                            def ts = new MetricTimeSeries.Builder(field)
                                    .attribute("host", attributes[0])
                                    .attribute("source", attributes[1])
                                    .attribute("group", attributes[2])

                            //Add some generic fields an values
                                    .attribute("myIntField", 5I)
                                    .attribute("myLongField", 8L)
                                    .attribute("myDoubleField", 5.5D)
                                    .attribute("myByteField", "String as byte".getBytes("UTF-8"))
                                    .attribute("myStringList", listStringField)
                                    .attribute("myIntList", listIntField)
                                    .attribute("myLongList", listLongField)
                                    .attribute("myDoubleList", listDoubleField)
                                    .build()
                            documents.put(i, ts)

                        }
                    }
                } else {
                    //First field is the timestamp: 26.08.2013 00:00:17.361
                    def date = Date.parse("dd.MM.yyyy HH:mm:ss.SSS", fields[0])
                    fields.subList(1, fields.size()).eachWithIndex { String value, int i ->
                        documents.get(i).add(date.getTime(), nf.parse(value).doubleValue())
                        filePoints = i

                    }
                }
                onlyOnce = false
            }
            chronix.add(documents.values(), luceneIndex)
            def updateResponse = luceneIndex.getOpenWriter().commit()
            LOGGER.info("Update Response of Commit is {}", updateResponse)
        }
    }

    def "Test add and query time series to Chronix with Solr"() {
        when:
        //query all documents
        List<MetricTimeSeries> timeSeries = chronix.stream(luceneIndex, createQuery("*:*")).collect(Collectors.toList());

        then:
        timeSeries.size() == 26i
        def selectedTimeSeries = timeSeries.get(0)

        selectedTimeSeries.size() >= 7000
        selectedTimeSeries.attribute("myIntField") == 5
        selectedTimeSeries.attribute("myLongField") == 8L
        selectedTimeSeries.attribute("myDoubleField") == 5.5D
        selectedTimeSeries.attribute("myByteField") == "String as byte".getBytes("UTF-8")
        selectedTimeSeries.attribute("myStringList") == listStringField
        selectedTimeSeries.attribute("myIntList") == listIntField
        selectedTimeSeries.attribute("myLongList") == listLongField
        selectedTimeSeries.attribute("myDoubleList") == listDoubleField
    }


    Query createQuery(String searchString) {
        QueryParser queryParser = new QueryParser("metric", luceneIndex.getOpenWriter().getAnalyzer());
        return queryParser.parse(searchString);
    }

}
