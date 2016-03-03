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
package de.qaware.chronix.lucene.client;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class that holds the lucene index writer and searcher
 *
 * @author f.lautenschlager
 */
public final class LuceneIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndex.class);

    private IndexSearcher searcher;
    private IndexReader reader;
    boolean reopenReader = true;
    private IndexWriter writer;

    private final Directory directory;
    private final Analyzer analyzer;

    /**
     * Constructs and lucene index
     */
    public LuceneIndex(Directory directory, Analyzer analyzer) {
        this.directory = directory;
        this.analyzer = analyzer;
    }

    /**
     * Initializes the searcher, if not initialized.
     * Closes the writer and opens a searcher.
     *
     * @return the lucene index searcher
     */
    public IndexSearcher getSearcher() throws IOException {
        if (searcher == null || reopenReader) {
            reader = getOpenReader();
            searcher = new IndexSearcher(reader);
        }
        return searcher;

    }

    /**
     * The returned index writer can be initialized.
     *
     * @return the index writer.
     */
    public IndexWriter getWriter() {
        return writer;
    }

    /**
     * @return the lucene index writer
     */
    public IndexWriter getOpenWriter() throws IOException {
        if (writer == null || !writer.isOpen()) {
            LOGGER.debug("Closing reader and opening writer.");
            if (reader != null) {
                LOGGER.debug("Closing reader.");
                reader.close();
                reopenReader = true;
            }
            writer = openWriter();
        }
        return writer;
    }

    /**
     * The returned index reader can be initialized.
     *
     * @return the index reader.
     */
    public IndexReader getReader() {
        return reader;
    }

    /**
     * @return the lucene index reader
     */
    public IndexReader getOpenReader() throws IOException {
        if (writer != null && writer.isOpen()) {
            LOGGER.debug("Closing writer");
            writer.close();
        }
        if (reader == null || reopenReader) {
            LOGGER.debug("Opening reader");
            reader = openReader();
            reopenReader = false;
        }
        return reader;
    }

    private IndexReader openReader() throws IOException {
        return DirectoryReader.open(directory);
    }

    private IndexWriter openWriter() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        return new IndexWriter(directory, config);
    }

    /**
     * @return the directory holding the index
     */
    public Directory getDirectory() {
        return directory;
    }
}
