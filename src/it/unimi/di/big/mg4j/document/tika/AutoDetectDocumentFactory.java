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
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;

/** A document factory that automatically detect the type of the document content. 
 * 
 * <p>The metadata that will be tentatively parsed are {@link Metadata#TITLE} and
 * {@link GreedyTikaField#NAME}: the latter will contain <strong>all</strong>
 * Tika fields {@link Object#toString()}'d and concatenated.
 *   
 * @author Salvatore Insalaco
 */

public class AutoDetectDocumentFactory extends AbstractSimpleTikaDocumentFactory {
    private static final List<TikaField> FIELDS = Arrays.asList(
	            new TikaField(Metadata.TITLE),
	            new GreedyTikaField(GreedyTikaField.NAME)
	        );
	private static final long serialVersionUID = 1L;
	private static final AutoDetectParser AUTO_DETECT_PARSER = new AutoDetectParser();

    public AutoDetectDocumentFactory() {}

    public AutoDetectDocumentFactory(Reference2ObjectMap<Enum<?>, Object> defaultMetadata) {
        super(defaultMetadata);
    }

    public AutoDetectDocumentFactory(Properties properties) throws ConfigurationException {
        super(properties);
    }

    public AutoDetectDocumentFactory(String[] property) throws ConfigurationException {
        super(property);
    }

    @Override
    protected Parser getParser() {
        return AUTO_DETECT_PARSER;
    }

    @Override
    protected List<? extends TikaField> metadataFields() {
        // TODO: better treatment for the rest of metadata?
        return FIELDS;

    }
}
