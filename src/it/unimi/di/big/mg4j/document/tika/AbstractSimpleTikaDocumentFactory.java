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

import it.unimi.di.big.mg4j.document.AbstractDocument;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParsingReader;

/** An abstract document factory that provides an implementation for {@link #getDocument(InputStream, Reference2ObjectMap)}
 * and {@link #fields()}. Moreover, it gets a {@link WordReader} object using the {@link it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys#WORDREADER} property.
 * 
 * <p>Concrete subclasses must provide a {@link #getParser()} method and may optionally override {@link #metadataFields()} (which
 * currently returns the empty list) to return the list of Tika fields provided by this factory. Note that {@link #getParser()} should return always the same instance, as
 * Tika parsers are immutable and thread-safe.
 * 
 * @author Salvatore Insalaco
 */

public abstract class AbstractSimpleTikaDocumentFactory extends AbstractTikaDocumentFactory {
	private static final long serialVersionUID = 1L;
	/** The list of tika fields. */
	private List<TikaField> fields;
	/** The word reader used by this class. */
	private WordReader wordReader;

	public AbstractSimpleTikaDocumentFactory() {
		init();
	}

	public AbstractSimpleTikaDocumentFactory( Reference2ObjectMap<Enum<?>, Object> defaultMetadata ) {
		super( defaultMetadata );
		init();
	}

	public AbstractSimpleTikaDocumentFactory( Properties properties ) throws ConfigurationException {
		super( properties );
		init();
	}

	public AbstractSimpleTikaDocumentFactory( String[] property ) throws ConfigurationException {
		super( property );
		init();
	}

	private void init() {
		try {
			final Object o = defaultMetadata.get( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER );
			wordReader = o == null ? new FastBufferedReader() : ObjectParser.fromSpec( o.toString(), WordReader.class, MG4JClassParser.PACKAGE );
		}
		catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}


	protected boolean parseProperty( final String key, final String[] values, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws ConfigurationException {
		if ( sameKey( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, key ) ) {
			try {
				final String spec = ( ensureJustOne( key, values ) ).toString();
				metadata.put( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, spec );
				// Just to check
				ObjectParser.fromSpec( spec, WordReader.class, MG4JClassParser.PACKAGE );
			}
			catch ( ClassNotFoundException e ) {
				throw new ConfigurationException( e );
			}
			// TODO: this must turn into a more appropriate exception
			catch ( Exception e ) {
				throw new ConfigurationException( e );
			}
			return true;
		}
		return super.parseProperty( key, values, metadata );
	}


	@Override
	protected List<TikaField> fields() {
		if ( fields == null ) {
			fields = new ArrayList<TikaField>();
			fields.add( new TikaField() );
			fields.addAll( metadataFields() );
		}
		return fields;
	}

	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>, Object> metadata ) throws IOException {
		return new AbstractDocument() {
			private ParsingReader parsingReader;

			private Metadata tikaMetadata;

			public CharSequence title() {
				return (CharSequence)resolve( MetadataKeys.TITLE, metadata );
			}

			public CharSequence uri() {
				return (CharSequence)resolve( MetadataKeys.URI, metadata );
			}

			public Object content( int field ) throws IOException {
				ensureFieldIndex( field );
				if ( parsingReader == null && tikaMetadata == null ) {
					tikaMetadata = new Metadata();
					if ( uri() != null ) tikaMetadata.set( Metadata.RESOURCE_NAME_KEY, uri().toString() );

					parsingReader = new ParsingReader( getParser(), rawContent, tikaMetadata, new ParseContext() );
				}
				if ( fields().get( field ).isBody() ) {
					return parsingReader;
				}
				else {
					String text = fields().get( field ).contentFromMetadata( tikaMetadata );
					return text == null ? new FastBufferedReader( new StringReader( "" ) ) : new FastBufferedReader( new StringReader( text ) );
				}
			}

			@Override
			public void close() throws IOException {
				super.close();
				if ( parsingReader != null ) {
					parsingReader.close();
					parsingReader = null;
				}
			}

			public WordReader wordReader( final int field ) {
				ensureFieldIndex( field );
				return wordReader;
			}
		};
	}

	/** The list of Tika fields (apart for content) that this factory provides; it returns the empty list, so most subclasses may want to override this method.
	 * 
	 * @return the list of Tika fields that this factory provides.
	 */
	protected List<? extends TikaField> metadataFields() {
		return Collections.emptyList();
	}

	/** The parser to be used to parse this kind of documents; subclasses should return always the same instance, as Tika parsers are immutable and thread-safe.
	 * 
	 * @return the parser to be used to parse this kind of documents.
	 */
	protected abstract Parser getParser();

	public DocumentFactory copy() {
		return this;
	}
}
