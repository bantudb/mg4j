package  it.unimi.di.big.mg4j.document.tika;

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

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.xml.XMLParser;

/**
 * A document factory for XML.
 * 
 * <p>This factory has no metadata.
 * 
 * @author Salvatore Insalaco
 */

public class XMLDocumentFactory extends AbstractSimpleTikaDocumentFactory {
	private static final XMLParser XML_PARSER = new XMLParser();

	private static final long serialVersionUID = 1L;
	
	public XMLDocumentFactory() {}

	public XMLDocumentFactory( Reference2ObjectMap<Enum<?>, Object> defaultMetadata ) {
		super( defaultMetadata );
	}

	public XMLDocumentFactory( Properties properties ) throws ConfigurationException {
		super( properties );
	}

	public XMLDocumentFactory( String[] property ) throws ConfigurationException {
		super( property );
	}

	@Override
	protected Parser getParser() {
		return XML_PARSER;
	}
}
