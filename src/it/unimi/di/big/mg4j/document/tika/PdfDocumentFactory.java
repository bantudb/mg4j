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
import org.apache.tika.parser.pdf.PDFParser;


/**
 * A document factory for the PDF format.
 * 
 * <p>The metadata that will be tentatively parsed are
 * {@link Metadata#TITLE}, {@link Metadata#AUTHOR}, {@link Metadata#CREATOR},
 * {@link Metadata#KEYWORDS}, {@link Metadata#SUBJECT}, <samp>producer</samp>, <samp>created</samp>,
 * <samp>trapped</samp>, and {@link Metadata#LAST_MODIFIED}.
 * 
 * @author Salvatore Insalaco
 */

public class PdfDocumentFactory extends AbstractSimpleTikaDocumentFactory {
    private static final List<TikaField> FIELDS = Arrays.asList(
	            new TikaField(Metadata.TITLE),
	            new TikaField(Metadata.AUTHOR),
	            new TikaField(Metadata.CREATOR),
	            new TikaField(Metadata.KEYWORDS),
	            new TikaField(Metadata.SUBJECT),
	            new TikaField("producer"),
	            new TikaField("created"),
	            new TikaField("trapped"),
	            new TikaField(Metadata.LAST_MODIFIED.getName())
	        );
	private static final PDFParser PDF_PARSER = new PDFParser();
	private static final long serialVersionUID = 1L;

    public PdfDocumentFactory() {}

	public PdfDocumentFactory( final Properties properties ) throws ConfigurationException {
		super( properties );
	}

	public PdfDocumentFactory( final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) {
		super( defaultMetadata );
	}

	public PdfDocumentFactory( final String[] property ) throws ConfigurationException {
		super( property );
	}

    @Override
    protected Parser getParser() {
        return PDF_PARSER;
    }

    @Override
    protected List<TikaField> metadataFields() {
        return FIELDS;
    }
}
