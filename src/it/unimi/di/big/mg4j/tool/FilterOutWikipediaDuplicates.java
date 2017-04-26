package it.unimi.di.big.mg4j.tool;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2013-2016 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/**
 * Reads a Wikipedia XML dump and outputs the same dump after eliminating
 * duplicate pages. A duplicate page is a page whose title appeared earlier in
 * the XML stream.
 */

public class FilterOutWikipediaDuplicates  {
	private static final Logger LOGGER = LoggerFactory.getLogger( FilterOutWikipediaDuplicates.class );

	public static void main( final String arg[] ) throws IOException, JSAPException, XMLStreamException {
		SimpleJSAP jsap = new SimpleJSAP( FilterOutWikipediaDuplicates.class.getName(), "Reads a Wikipedia XML dump and outputs the same dump after eliminating duplicate pages. A duplicate page is a page whose title appeared earlier in the XML stream. ",
			new Parameter[] {
				new Switch( "bzip2", 'b', "bzip2", "The (input and output) files are compressed with bzip2" ),
				new UnflaggedOption( "infile", JSAP.STRING_PARSER, JSAP.REQUIRED, "The input file containing the Wikipedia dump (- for stdin)." ),
				new UnflaggedOption( "outfile", JSAP.STRING_PARSER, JSAP.REQUIRED, "The output file containing the Wikipedia dump (- for stdout)." ),
		});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		// Prepare input/output streams
		String inFilename = jsapResult.getString("infile");
		InputStream in = "-".equals(inFilename) ? System.in : new FileInputStream(inFilename);
		if (jsapResult.userSpecified("bzip2")) in = new BZip2CompressorInputStream(in);

		String outFilename = jsapResult.getString("outfile");
		OutputStream out = "-".equals(outFilename) ? System.out : new FileOutputStream(outFilename);
		if (jsapResult.userSpecified("bzip2")) {
			if (out==System.out) LOGGER.warn("Going to produce bzip'd output onto stdout");
			out = new BZip2CompressorOutputStream(out);		
		}
		
		// Prepare reader/writer
		XMLInputFactory xmlif = XMLInputFactory.newInstance();
		xmlif.setProperty(XMLInputFactory.IS_VALIDATING, Boolean.FALSE);
		xmlif.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, Boolean.TRUE);
		XMLEventReader er = xmlif.createXMLEventReader(in);
		XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
		
		XMLEventWriter ew = xmlof.createXMLEventWriter(out);
		
		// Keeps track of whether we are in a page (in which case events are just stored in a list) or not
		ArrayList<XMLEvent> events = new ArrayList<XMLEvent>();
		boolean inPage = false, inTitle = true;
		StringBuilder pageTitleBuilder = new StringBuilder();
		ObjectOpenHashSet<String> titles = new ObjectOpenHashSet<String>();

		ProgressLogger pl = new ProgressLogger(LOGGER);
		pl.itemsName = "pages";
		
		while (er.hasNext()) {
			XMLEvent event = (XMLEvent) er.next();
			if (event.isStartElement()) {
				String startingElement = event.asStartElement().getName().getLocalPart();
				if (startingElement.equalsIgnoreCase("page")) {					
					inPage = true;
					events.clear();
				}
				if (inPage) {
					events.add(event);
					if (event.asStartElement().getName().getLocalPart().equalsIgnoreCase("title")) {
						inTitle = true;
						pageTitleBuilder.setLength(0);
					}
				}
				else ew.add(event);
			} else if (event.isEndElement()) {
				if (inPage) {
					events.add(event);
					if (event.asEndElement().getName().getLocalPart().equalsIgnoreCase("title")) inTitle = false;
				}
				else ew.add(event);
				String endingElement = event.asEndElement().getName().getLocalPart();
				if (endingElement.equalsIgnoreCase("page")) {
					pl.update();
					String pageTitle = pageTitleBuilder.toString();
					if (titles.contains(pageTitle)) {
						LOGGER.info("Skipping duplicate page: " + pageTitle);
						continue;
					}
					titles.add(pageTitle);
					for (XMLEvent e: events)
						ew.add(e);
				} 
			} else {
				if (inPage) {
					events.add(event);
					if (inTitle && event.isCharacters()) pageTitleBuilder.append(event.asCharacters().getData());
				}
				else ew.add(event);
			}

		}

		ew.close();
		if (out != System.out ) out.close();
		pl.done();
	}
}
