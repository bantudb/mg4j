package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineWordReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.commons.configuration.ConfigurationException;

/** A factory that provides a single field containing just the raw input stream; the encoding
 * is set using the property {@link it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys#ENCODING}. 
 * The field is named <samp>text</samp>, but you can change the name using the property
 * <samp>fieldname</samp>.
 * 
 * <p>By default, the {@link WordReader} provided by this factory
 * is just a {@link FastBufferedReader}, but you can specify
 * an alternative word reader using the property 
 * {@link it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys#WORDREADER}.
 * For instance, if you need to index a list of identifiers to retrieve documents from 
 * the collection more easily, you can use a {@link LineWordReader}
 * to index each line of a file as a whole.
 *  
 */

public class IdentityDocumentFactory extends PropertyBasedDocumentFactory {
	private static final long serialVersionUID = 2L;

	/** Case-insensitive keys for metadata.
	 * 
	 *  @see PropertyBasedDocumentFactory.MetadataKeys
	 */ 
	public static enum MetadataKeys {
		/** The tag for the optional name of the only field provided by this factory. */
		FIELDNAME
	};

	/** The name of the only field. */
	private String fieldName;
	/** The word reader used for all documents. */
	private transient WordReader wordReader;

	protected boolean parseProperty( final String key, final String[] values, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws ConfigurationException {
		if ( sameKey( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, key ) ) {
			metadata.put( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, Charset.forName( ensureJustOne( key, values ) ).toString() );
			return true;
		}
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
		if ( sameKey( MetadataKeys.FIELDNAME, key ) ) {
			metadata.put( MetadataKeys.FIELDNAME, ensureJustOne( key, values ) );
			return true;
		}

		return super.parseProperty( key, values, metadata );
	}

	public IdentityDocumentFactory() {
		init();
	}
		
	private void init() {
		Object o = defaultMetadata.get( MetadataKeys.FIELDNAME );
		fieldName = o == null ? "text" : o.toString();
		try {
			o = defaultMetadata.get( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER );
			wordReader = o == null ? new FastBufferedReader() : ObjectParser.fromSpec( o.toString(), WordReader.class, MG4JClassParser.PACKAGE );
		}
		catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	public IdentityDocumentFactory( final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) {
		super( defaultMetadata );
		init();
	}
		
	public IdentityDocumentFactory( final Properties properties ) throws ConfigurationException {
		super( properties );
		init();
	}
		
	public IdentityDocumentFactory( final String[] property ) throws ConfigurationException {
		super( property );
		init();
	}
		
	public IdentityDocumentFactory copy() {
		return new IdentityDocumentFactory( defaultMetadata );
	}
	
	public int numberOfFields() {
		return 1;
	}
	
	public String fieldName( final int field ) {
		ensureFieldIndex( field );
		return fieldName;
	}
	
	public int fieldIndex( final String fieldName ) {
		return fieldName.equals( this.fieldName ) ? 0: -1;
	}

	public FieldType fieldType( final int field ) {
		ensureFieldIndex( field );
		return FieldType.TEXT;
	}
	
	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) {
		return new Document() {
			
			public CharSequence title() {
				return (CharSequence)resolve( PropertyBasedDocumentFactory.MetadataKeys.TITLE, metadata );
			}
			
			public String toString() {
				return title().toString();
			}

			public CharSequence uri() {
				return (CharSequence)resolve( PropertyBasedDocumentFactory.MetadataKeys.URI, metadata );
			}

			public Object content( final int field ) {
				ensureFieldIndex( field );
				try {
					return new InputStreamReader( rawContent, (String)resolveNotNull( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, metadata ) );
				}
				catch( UnsupportedEncodingException e ) {
					throw new RuntimeException( e );
				}
			}

			public WordReader wordReader( final int field ) {
				ensureFieldIndex( field );
				// TODO: should depend on locale (or something)
				return wordReader; 
			}
			
			public void close() {}
		};
	}
	
	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		init();
	}
}
