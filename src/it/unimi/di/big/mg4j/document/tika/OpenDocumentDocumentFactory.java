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

import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.odf.OpenDocumentParser;


/**
 * A document factory for the Open Document format.
 * 
 * <p>The only metadata that will be parsed is {@link GreedyTikaField#NAME}.
 * 
 * @author Salvatore Insalaco
 */

public class OpenDocumentDocumentFactory extends AbstractSimpleTikaDocumentFactory {
	private static final List<GreedyTikaField> FIELDS = Collections.singletonList(new GreedyTikaField(GreedyTikaField.NAME));
	private static final OpenDocumentParser OPEN_DOCUMENT_PARSER = new OpenDocumentParser();
	private static final long serialVersionUID = 1L;

    public OpenDocumentDocumentFactory() {}

	public OpenDocumentDocumentFactory(Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
        super(defaultMetadata);
    }

    public OpenDocumentDocumentFactory(Properties properties) throws ConfigurationException {
        super(properties);
    }

    public OpenDocumentDocumentFactory(String[] property) throws ConfigurationException {
        super(property);
    }

    @Override
    protected List<? extends TikaField> metadataFields() {
        return FIELDS;
    }
    
    @Override
    protected Parser getParser() {
        return OPEN_DOCUMENT_PARSER;
    }
}
