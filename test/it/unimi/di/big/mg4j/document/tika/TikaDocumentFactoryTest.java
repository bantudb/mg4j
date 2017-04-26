package it.unimi.di.big.mg4j.document.tika;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2011-2016 Paolo Boldi and Sebastiano Vigna  
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import static org.junit.Assert.assertTrue;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.lang.MutableString;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.junit.Test;

public class TikaDocumentFactoryTest {
    private static final String TEST_STRING = "The quick brown fox jumps over a lazy dog.";

    protected Reference2ObjectMap<Enum<?>,Object> metadata(String file) {
		final Reference2ObjectArrayMap<Enum<?>, Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 2 );
		metadata.put( PropertyBasedDocumentFactory.MetadataKeys.TITLE, file);
		return metadata;
	}

    protected InputStream stream(String fileName) {
        return getClass().getResourceAsStream(fileName);
    }

    private String reader2String(Reader inputReader) throws IOException {
        MutableString content = new MutableString();
        BufferedReader reader = new BufferedReader(inputReader);
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line);
        }
        return content.toString();
    }

    protected void performTest(DocumentFactory df, String fileName) throws IOException {
        Document doc = df.getDocument(stream(fileName), metadata(fileName));
        String content = reader2String((Reader)doc.content(0));
        assertTrue(content.contains(TEST_STRING));
        doc.close();        
    }

    @Test
    public void testRTFDocumentFactory() throws IOException {
        performTest(new RTFDocumentFactory(), "testrtf.data");
    }

    @Test
    public void testMSOfficeDocDocumentFactory() throws IOException {
        performTest(new MSOfficeDocumentFactory(), "testdoc.data");
    }

    @Test
    public void testOpenDocumentDocumentFactory() throws IOException {
        performTest(new OpenDocumentDocumentFactory(), "testodt.data");
    }

    @Test
    public void testHtmlDocumentFactory() throws IOException {
        performTest(new HtmlDocumentFactory(), "testhtml.data");
    }

    @Test
    public void testOOXMLDocxDocumentFactory() throws IOException {
        performTest(new OOXMLDocumentFactory(), "testdocx.data");
    }

    @Test
    public void testPDFDocumentFactory() throws IOException {
        performTest(new PdfDocumentFactory(), "testpdf.data");
    }

    @Test
    public void testXMLDocumentFactory() throws IOException {
        performTest(new XMLDocumentFactory(), "testxml.data");
    }

    @Test
    public void testEPUBDocumentFactory() throws IOException {
        performTest(new EPUBDocumentFactory(), "testepub.data");
    }

    @Test
    public void testTextDocumentFactory() throws IOException {
        performTest(new TextDocumentFactory(), "testtextutf8.data");
        performTest(new TextDocumentFactory(), "testtextutf16.data");
    }

    @Test
    public void testAutoDetectDocumentFactory() throws IOException {
        String[] fileList = new String[] {
            "testdoc.data",
            "testdocx.data",
            "testepub.data",
            "testhtml.data",
            "testodt.data",
            "testpdf.data",
            "testrtf.data",
            //"testtextutf16.data",
            "testtextutf8.data",
            "testxml.data"
        };

        for (String file : fileList)
            performTest(new AutoDetectDocumentFactory(), file);
    }
}
