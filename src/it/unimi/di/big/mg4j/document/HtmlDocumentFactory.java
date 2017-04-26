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
import it.unimi.di.big.mg4j.util.parser.callback.AnchorExtractor;
import it.unimi.dsi.fastutil.chars.CharArrays;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.callback.ComposedCallbackBuilder;
import it.unimi.dsi.parser.callback.TextExtractor;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.nio.charset.Charset;

import org.apache.commons.configuration.ConfigurationException;

import com.google.common.base.Charsets;

/** A factory that provides fields for body and title of HTML documents. 
 * It uses internally a {@link BulletParser}. 
 * A default encoding can be provided
 * using the property {@link it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys#ENCODING}.
 * 
 * <p>By default, the {@link WordReader} provided by this factory
 * is just a {@link FastBufferedReader}, but you can specify
 * an alternative word reader using the property 
 * {@link it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys#WORDREADER}.
 * 
 * <p>Additional {@linkplain MetadataKeys keys} make it possible to tune the underlying {@link AnchorExtractor}.
 */

public class HtmlDocumentFactory extends PropertyBasedDocumentFactory {
	private static final long serialVersionUID = 1L;

	/** Default maximum number of character before an anchor (property {@link HtmlDocumentFactory.MetadataKeys#MAXPREANCHOR}). */
	public static final int DEFAULT_MAXPREANCHOR = 32;
	/** Default maximum number of character in an anchor (property {@link HtmlDocumentFactory.MetadataKeys#MAXANCHOR}).. */
	public static final int DEFAULT_MAXANCHOR = 512;
	/** Default maximum number of characters after an anchor (property {@link HtmlDocumentFactory.MetadataKeys#MAXPOSTANCHOR}).. */
	public static final int DEFAULT_MAXPOSTANCHOR = 16;

	public static enum MetadataKeys {
		/** The maximum number of characters before an anchor. */
		MAXPREANCHOR,
		/** The maximum number of characters in an anchor. */
		MAXANCHOR,
		/** The maximum number of characters after an anchor. */
		MAXPOSTANCHOR,
		/** The anchor delimiter (see {@link AnchorExtractor#AnchorExtractor(int, int, int, String)}). */
		DELIMITER,
	};

	protected static final int DEFAULT_BUFFER_SIZE = 16 * 1024;
	/** A parser that will be used to extract text from HTML documents. */
	protected transient BulletParser parser;
	/** The callback recording text. */
	protected transient TextExtractor textExtractor;
	/** The callback for anchors. */
	protected transient AnchorExtractor anchorExtractor;
	/** The word reader used for all documents. */
	protected transient WordReader wordReader;
	/** The maximum number of characters before an anchor. */
	protected int maxPreAnchor;
	/** The maximum number of characters in an anchor. */
	protected int maxAnchor;
	/** The maximum number of characters after an anchor. */
	protected int maxPostAnchor;
	/** A token that will be inserted to delimit the anchor text, or {@code null} for no delimiter. */
	protected String delimiter;
	/** The buffer holding text. */
	protected transient char[] text;

	protected boolean parseProperty( final String key, final String[] values, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws ConfigurationException {
		if ( sameKey( PropertyBasedDocumentFactory.MetadataKeys.MIMETYPE, key ) ) {
			metadata.put( PropertyBasedDocumentFactory.MetadataKeys.MIMETYPE, ensureJustOne( key, values ) );
			return true;
		}
		else if ( sameKey( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, key ) ) {
			metadata.put( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, Charset.forName( ensureJustOne( key, values ) ).toString() );
			return true;
		}
		else if ( sameKey( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, key ) ) {
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
		else if ( sameKey( MetadataKeys.MAXPREANCHOR, key ) ) {
			metadata.put( MetadataKeys.MAXPREANCHOR, Integer.valueOf( ensureJustOne( key, values ) ) );
			return true;
		}
		else if ( sameKey( MetadataKeys.MAXANCHOR, key ) ) {
			metadata.put( MetadataKeys.MAXANCHOR, Integer.valueOf( ensureJustOne( key, values ) ) );
			return true;
		}
		else if ( sameKey( MetadataKeys.MAXPOSTANCHOR, key ) ) {
			metadata.put( MetadataKeys.MAXPOSTANCHOR, Integer.valueOf( ensureJustOne( key, values ) ) );
			return true;
		}
		else if ( sameKey( MetadataKeys.DELIMITER, key ) ) {
			metadata.put( MetadataKeys.DELIMITER, ensureJustOne( key, values ) );
			return true;
		}
		
		return super.parseProperty( key, values, metadata );
	}

	protected void init() {
		this.parser = new BulletParser();
		
		ComposedCallbackBuilder composedBuilder = new ComposedCallbackBuilder();
		composedBuilder.add( this.textExtractor = new TextExtractor() );
		composedBuilder.add( this.anchorExtractor = new AnchorExtractor( maxPreAnchor, maxAnchor, maxPostAnchor, delimiter ) ); 
		parser.setCallback( composedBuilder.compose() );

		Object o;
		try {
			o = defaultMetadata.get( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER );
			wordReader = o == null ? new FastBufferedReader() : ObjectParser.fromSpec( o.toString(), WordReader.class, MG4JClassParser.PACKAGE );
		}
		catch ( Exception e ) {
			throw new RuntimeException( e );
		}
		text = new char[ DEFAULT_BUFFER_SIZE ];
	}

	@SuppressWarnings("boxing")
	protected void initVars() {
		maxPreAnchor = (Integer)resolve( MetadataKeys.MAXPREANCHOR, defaultMetadata, DEFAULT_MAXPREANCHOR );
		maxAnchor = (Integer)resolve( MetadataKeys.MAXANCHOR, defaultMetadata, DEFAULT_MAXANCHOR );
		maxPostAnchor = (Integer)resolve( MetadataKeys.MAXPOSTANCHOR, defaultMetadata, DEFAULT_MAXPOSTANCHOR );
		delimiter = (String)resolve( MetadataKeys.DELIMITER, defaultMetadata, null );
	}
	
	/** Returns a copy of this document factory. A new parser is allocated for the copy. */
	public HtmlDocumentFactory copy() {
		return new HtmlDocumentFactory( defaultMetadata );
	}
	
	public HtmlDocumentFactory( final Properties properties ) throws ConfigurationException {
		super( properties );
		initVars();
		init();
	}

	public HtmlDocumentFactory( final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) {
		super( defaultMetadata );
		initVars();
		init();
	}

	public HtmlDocumentFactory( final String[] property ) throws ConfigurationException {
		super( property );
		initVars();
		init();
	}

	public HtmlDocumentFactory() {
		super();
		initVars();
		init();
	}
	
	public int numberOfFields() {
		return 3;
	}

	public String fieldName( final int field ) {
		ensureFieldIndex( field );
		switch( field ) {
			case 0: return "text";
			case 1: return "title";
			case 2: return "anchor";
			default: throw new IllegalArgumentException();
		}
	}
	
	public int fieldIndex( final String fieldName ) {
		for ( int i = 0; i < numberOfFields(); i++ )
			if ( fieldName( i ).equals( fieldName ) ) return i;
		return -1;
	}
	
	public FieldType fieldType( final int field ) {
		ensureFieldIndex( field );
		switch( field ) {
			case 0: return FieldType.TEXT;
			case 1: return FieldType.TEXT;
			case 2: return FieldType.VIRTUAL;
			default: throw new IllegalArgumentException();
		}
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		init();
	}

	/** An HTML document. If a <samp>TITLE</samp> element is available, it will be used for {@link #title()}
	 * 	instead of the default value. 
	 * 
	 * <p>We delay the actual parsing until it is actually necessary, so operations like
	 * getting the document URI will not require parsing. */
	
	protected class HtmlDocument extends AbstractDocument {
		protected final Reference2ObjectMap<Enum<?>,Object> metadata;
		/** Whether we already parsed the document. */
		protected boolean parsed;
		/** The cached raw content. */
		protected final InputStream rawContent;

		protected void ensureParsed() throws IOException {
			if ( parsed ) return;

			int offset = 0, l;
			Charset charset = Charsets.ISO_8859_1;
			try {
				charset = Charset.forName( (String)resolveNotNull( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, metadata ) );
			}
			catch( RuntimeException keepDefaut ) {}
			Reader r = new InputStreamReader( rawContent, charset );
			while( ( l = r.read( text, offset, text.length - offset ) ) > 0 ) {
				offset += l;
				text = CharArrays.grow( text, offset + 1 );
			}
			parser.parse( text, 0, offset );
			textExtractor.title.trim();

			parsed = true;
		}
		
		protected HtmlDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) {
			this.metadata = metadata;
			this.rawContent = rawContent;
		}

		public CharSequence title() {
			try {
				ensureParsed();
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			}
			final Object metadataTitle = metadata.get( PropertyBasedDocumentFactory.MetadataKeys.TITLE );
			return (CharSequence)( metadataTitle != null ? metadataTitle : textExtractor.title );
		}

		public String toString() {
			if ( uri() != null ) return uri().toString();
			// Never parse just because of toString(). See comments in AbstractDocument.
			if ( parsed ) return title().toString();
			return "[unparsed]";
		}

		public CharSequence uri() {
			return (CharSequence)resolve( PropertyBasedDocumentFactory.MetadataKeys.URI, metadata );
		}

		public Object content( final int field ) throws IOException {
			ensureFieldIndex( field );
			ensureParsed();
			switch( field ) {
				case 0: return new FastBufferedReader( textExtractor.text );
				case 1: return new FastBufferedReader( textExtractor.title );
				case 2: return anchorExtractor.anchors;
				default: throw new IllegalArgumentException();
			}
		}

		public WordReader wordReader( final int field ) {
			ensureFieldIndex( field );
			return wordReader; 
		}
	}

	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
		return new HtmlDocument( rawContent, metadata );
	}
}
