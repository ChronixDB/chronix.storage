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
package de.qaware.chronix.lucene.client

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.store.RAMDirectory
import spock.lang.Specification

/**
 * Unit test for the lucene index wrapper class
 * @author f.lautenschlager
 */
class LuceneIndexTest extends Specification {


    def "test getSearcher after openWriter"() {
        given:
        def luceneIndex = new LuceneIndex(new RAMDirectory(), new StandardAnalyzer())

        when:
        luceneIndex.getOpenWriter().commit()
        def searcher = luceneIndex.getSearcher()

        then:
        searcher != null
        luceneIndex.readerOpen()
        luceneIndex.writerClosed()
    }

    def "test getOpenWriter after openReader"() {
        given:
        def luceneIndex = new LuceneIndex(new RAMDirectory(), new StandardAnalyzer())

        when:
        luceneIndex.getOpenWriter().commit()
        luceneIndex.getOpenReader()
        def indexWriter = luceneIndex.getOpenWriter()


        then:
        indexWriter != null
        luceneIndex.readerClosed()
        luceneIndex.writerOpen()
    }

    def "test getOpenReader after openWriter"() {
        given:
        def luceneIndex = new LuceneIndex(new RAMDirectory(), new StandardAnalyzer())

        when:
        luceneIndex.openWriter.commit()
        def indexWriter = luceneIndex.getOpenReader()

        then:
        indexWriter != null
        luceneIndex.readerOpen()
        luceneIndex.writerClosed()
    }

    def "test getDirectory"() {
        given:
        def luceneIndex = new LuceneIndex(new RAMDirectory(), new StandardAnalyzer())

        when:
        def dir = luceneIndex.getDirectory()

        then:
        dir != null
        dir instanceof RAMDirectory
    }
}
