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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.epub.EpubParser;

/**
 * A document factory for the epub format.
 * 
 * <p>The metadata that will be tentatively parsed are {@link Metadata#TITLE},
 * {@link Metadata#SUBJECT}, {@link Metadata#CREATOR}, {@link Metadata#DESCRIPTION},
 * {@link Metadata#PUBLISHER}, {@link Metadata#CONTRIBUTOR}, {@link Metadata#DATE},
 * {@link Metadata#TYPE}, {@link Metadata#FORMAT}, {@link Metadata#IDENTIFIER},
 * {@link Metadata#LANGUAGE}, and {@link Metadata#RIGHTS}.
 * 
 * @author Salvatore Insalaco
 */

public class EPUBDocumentFactory extends AbstractSimpleTikaDocumentFactory {
    private static final List<TikaField> FIELDS = Arrays.asList(
	            new TikaField(Metadata.TITLE),
	            new TikaField(Metadata.SUBJECT),
	            new TikaField(Metadata.CREATOR),
	            new TikaField(Metadata.DESCRIPTION),
	            new TikaField(Metadata.PUBLISHER),
	            new TikaField(Metadata.CONTRIBUTOR),
	            new TikaField(Metadata.DATE.getName()),
	            new TikaField(Metadata.TYPE),
	            new TikaField(Metadata.FORMAT),
	            new TikaField(Metadata.IDENTIFIER),
	            new TikaField(Metadata.LANGUAGE),
	            new TikaField(Metadata.RIGHTS)
	        );
	private static final long serialVersionUID = 1L;
	private static final Parser E_PUB_PARSER = new EpubParser();

	public EPUBDocumentFactory() {}

	public EPUBDocumentFactory(Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
        super(defaultMetadata);
    }

    public EPUBDocumentFactory(Properties properties) throws ConfigurationException {
        super(properties);
    }

    public EPUBDocumentFactory(String[] property) throws ConfigurationException {
        super(property);
    }

    @Override
    protected Parser getParser() {
        return E_PUB_PARSER;
    }

    @Override
    protected List<TikaField> metadataFields() {
        return FIELDS;
    }
}
