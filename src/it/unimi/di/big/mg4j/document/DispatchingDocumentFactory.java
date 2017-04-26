package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi 
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
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.NullReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;

/** A document factory that actually dispatches the task of building documents to various factories
 *  according to some strategy.
 * 
 * <p>The strategy is specified as (an object embedding) a method that determines which factory
 * should be used on the basis of the metadata that are provided to the {@link #getDocument(InputStream, Reference2ObjectMap)}
 * method. Since usually the strategy will have to resolve the name of metadata, it is also passed
 * this factory, so that the correct 
 * {@link it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory#resolve(Enum,Reference2ObjectMap)} method can be invoked. 
 * 
 * <p>Moreover, at construction one must specify, for each subfactory and for each field of this
 * factory, which field of the subfactory should be used. Note that to guarantee sequential access,
 * fields specified for each subfactory should appear in increasing order.
 */
public class DispatchingDocumentFactory extends PropertyBasedDocumentFactory {
	private static final long serialVersionUID = 1L;

	private static final boolean DEBUG = false;

	/** Case-insensitive keys for metadata. 
	 * 
	 *  @see PropertyBasedDocumentFactory.MetadataKeys
	 */ 
	public static enum MetadataKeys {
	/** The property containing the (comma-separated) sequence of field names. */
		FIELDNAME, 
	/** The property containing the key that should be checked (e.g., mimetype). */
		KEY, 
	/** The property containing comma-separated sequence of colon-separated pairs value/document factory names. */
		RULE,
	/** The property containing a comma-separated list with as many items as there are factories; each item will be
	 *  a colon-separated list of as many integers as there are fields. The <var>k</var>-th integer in the <var>f</var>-th
	 *  list is the number of the field of the <var>f</var>-th factory that should be used to extract field number <var>k</var>,
	 *  or -1 if the field should be empty. */
		MAP
	}

	/** The value to be used in <code>RULE</code> to introduce the default factory. Otherwise, no default factory is
	 *  provided for documents that do not match. */
	public final static String OTHERWISE_IN_RULE = "?";
	
	
	/** A strategy that decides which factory is appropriate using the document metadata. */
	
	public static interface DispatchingStrategy extends Serializable {
		/** Decides the index of the factory to be used for the given metadata, possibly using
		 *  a factory to resolve property names.
		 * 
		 * @param metadata the metadata of the document to be produced.
		 * @param factory the factory used to resolve metadata names.
		 * @return the factory index.
		 */
		public int factoryNumber( Reference2ObjectMap<Enum<?>,Object> metadata, PropertyBasedDocumentFactory factory );
	};
	
	/** A strategy that is based on trying to match the value of the metadata with a given key with respect to a
	 *  certain set of values.
	 */
	public static class StringBasedDispatchingStrategy implements DispatchingStrategy {
		private static final long serialVersionUID = 1L;
		/** The key to be resolved. */
		private final Enum<?> key;
		/** The values that should be used for comparisons. */
		private final Object2IntMap<String> value;

		/** The strategy works as follows: the property named <code>key</code> is resolved; if this property
		 *  is not set, the default return value of <var>value</var> is returned. 
		 *  Otherwise, its value is compared, using the <code>equals</code>,
		 *  method with the elements of the <code>value</code> set, and the corresponding integer is returned.
		 * 
		 * @param key the key to be resolved.
		 * @param value the map of values.
		 */
		public StringBasedDispatchingStrategy( final Enum<?> key, final Object2IntMap<String> value ) {
			this.key = key;
			this.value = value;
		}
		
		public int factoryNumber( final Reference2ObjectMap<Enum<?>,Object> metadata, final PropertyBasedDocumentFactory factory ) {
			final Object val = factory.resolve( key, metadata );
			if ( DEBUG ) System.out.println( "key " + key + " resolved using " + metadata + " into " + val );
			return value.getInt( val );
		}
		
	};
	
	/** The number of subfactories used. */
	private int n;
	/** The subfactories used. */
	private DocumentFactory[] documentFactory;
	/** The number of fields of this factory. */
	private int numberOfFields;
	/** The names of the fields. */
	private String[] fieldName;
	/** The types of the fields. */
	private FieldType[] fieldType;
	/** The array specifying how subfactory fields should be mapped into fields of this factory. More precisely,
	 *  <code>rename[f][k]</code> specifies which field of factory <code>documentFactory[f]</code> should be used
	 *  to return the field named <code>fieldName[k]</code>: it is assumed that the type of the field in the subfactory
	 *  is correct (i.e., that <code>documentFactory[f].fieldType(k)==fieldType[k]</code>). The value -1 is used to
	 *  return an empty textual field (i.e., a word reader on an empty string).
	 */
	private int[][] rename;
	/** The strategy to be used. */
	private DispatchingStrategy strategy;
	/** If a {@link StringBasedDispatchingStrategy} should be used, this field represents the property key to be checked. 
	 *  Otherwise, this is <code>null</code>. */
	private Enum<?> dispatchingKey;
	/** If a {@link StringBasedDispatchingStrategy} should be used, this field represents the map from values to factories. */
	private Object2ObjectLinkedOpenHashMap<String,Class<? extends DocumentFactory>> value2factoryClass;
	
		
	private void init( final DocumentFactory[] documentFactory, final String[] fieldName, 
			final FieldType[] fieldType, final int[][] rename, final DispatchingStrategy strategy ) {
			n = documentFactory.length;
			this.documentFactory = documentFactory;
			numberOfFields = fieldName.length;
			this.fieldName = fieldName;
			this.fieldType = fieldType;
			this.rename = rename;
			this.strategy = strategy;
	}
	
	// TODO: All IllegalArgumentException where ConfigurationException; check that now it's OK
	private void checkAttributes() {
		if ( fieldName.length != fieldType.length || rename.length != documentFactory.length || documentFactory.length != n || fieldName.length != numberOfFields ) throw new IllegalArgumentException( "Length mismatch in defining the dispatching factory");
		for ( int f = 0; f < n; f++ ) {
			if ( rename[ f ].length != numberOfFields ) throw new IllegalArgumentException( "The number of fields (" + numberOfFields + ") does not match the mapping rule for factory " + documentFactory[ f ].getClass().getName() );
			for ( int k = 0; k < numberOfFields; k++ ) {
				if ( rename[ f ][ k ] < -1 || rename[ f ][ k ] >= documentFactory[ f ].numberOfFields() )
					throw new IllegalArgumentException( rename[ f ][ k ] + " is not a field of factory " + documentFactory[ f ] );
				if ( rename[ f ][ k ] >= 0 && fieldType[ k ] != documentFactory[ f ].fieldType( rename[ f ][ k ] ) )
					throw new IllegalArgumentException( "Field " + rename[ f ][ k ] + " of factory " + documentFactory[ f ] + " has a type different from the type of the field it is mapped to" );
			}			
		}
		if ( n == 0 || numberOfFields == 0 ) throw new IllegalArgumentException( "Zero factories or fields specified" );
		if ( strategy == null ) throw new IllegalArgumentException( "No strategy was specified" );
	}

	private void setExtraArguments( final Object xtraPars ) throws IllegalArgumentException {
		if ( value2factoryClass == null ) throw new IllegalArgumentException( "No " + MetadataKeys.RULE + " property was specified for the dispatching factory" );
		n = value2factoryClass.values().size();
		documentFactory = new DocumentFactory[ n ];
		Iterator<Class<? extends DocumentFactory>> it = value2factoryClass.values().iterator();
		for ( int f = 0; f < n; f++ ) {
			Class<? extends DocumentFactory> documentFactoryClass = it.next();
			try {
				if ( xtraPars == null )
					documentFactory[ f ] = documentFactoryClass.newInstance();
				else
					documentFactory[ f ] = documentFactoryClass.getConstructor( xtraPars.getClass() ).newInstance( xtraPars );
			} catch ( Exception e ) {
				throw new IllegalArgumentException( e );
			}
		}

		fieldType = new FieldType[ numberOfFields ];
		if ( rename == null ) throw new IllegalArgumentException( "No " + MetadataKeys.MAP + " property was specified for the dispatching factory" );
		for ( int f = 0; f < n; f++ ) {
			for ( int k = 0; k < numberOfFields; k++ ) {
				int kk = rename[ f ][ k ];
				if ( kk >= 0 && fieldType[ k ] != null && fieldType[ k ] != documentFactory[ f ].fieldType( kk ) ) 
					throw new IllegalArgumentException( "Mismatch between field types for field " + f + ", relative to the remapping of factory " + documentFactory[ f ].getClass().getName() + " (the type used to be " + fieldType[ k ] + ", but now we want it to be " + documentFactory[ f ].fieldType( kk ) + ")" );
				if ( kk >= 0 ) fieldType[ k ] = documentFactory[ f ].fieldType( kk );
			} 
		}
		for ( int f = 0; f < numberOfFields; f++ ) 
			if ( fieldType[ f ] == null ) throw new IllegalArgumentException( "The type of field " + fieldName[ f ] + " could not be deduced, because it is never mapped to" );
		if ( dispatchingKey == null ) throw new IllegalArgumentException( "No " + MetadataKeys.KEY + " property was specified for the dispatching factory" );
		Object2IntMap<String> value2int = new Object2IntOpenHashMap<String>();
		value2int.defaultReturnValue( -1 );
		for( Map.Entry<String,Class<? extends DocumentFactory>> e : value2factoryClass.entrySet() ) {
			int k;
			for ( k = 0; k < n; k++ ) 
				if ( e.getValue() == documentFactory[ k ].getClass() ) {
					if ( e.getKey().equals( OTHERWISE_IN_RULE ) ) value2int.defaultReturnValue( k );
					else value2int.put( e.getKey(), k );
					break;
				}
			if ( k == n ) throw new IllegalArgumentException( "Mismatch in the rule mapping " + e.getKey() + " to " + e.getValue() );
		}
		System.out.println( "Building a strategy mapping " + dispatchingKey + " to " + value2int );
		strategy = new StringBasedDispatchingStrategy( dispatchingKey, value2int );

	}

	/** Creates a new dispatching factory. 
	 * 
	 * @param documentFactory the array of subfactories.
	 * @param fieldName the names of this factory's fields.
	 * @param fieldType the types of this factory's fields. 
	 * @param rename the way fields of this class are mapped to fields of the subfactories.
	 * @param strategy the strategy to decide which factory should be used.
	 */
	public DispatchingDocumentFactory( final DocumentFactory[] documentFactory, final String[] fieldName, 
			final FieldType[] fieldType, final int[][] rename, final DispatchingStrategy strategy ){
		init( documentFactory, fieldName, fieldType, rename, strategy );
		checkAttributes();
	} 

	public DispatchingDocumentFactory copy() {
		final DocumentFactory[] documentFactory = new DocumentFactory[ this.documentFactory.length ];
		for( int i = documentFactory.length; i-- != 0; ) documentFactory[ i ] = this.documentFactory[ i ].copy();
		return new DispatchingDocumentFactory( documentFactory, fieldName, fieldType, rename, strategy );
	}
	
	public DispatchingDocumentFactory( final Properties properties ) throws ConfigurationException {
		super( properties );
		setExtraArguments( properties );
		checkAttributes();
	}

	public DispatchingDocumentFactory( final String[] property ) throws ConfigurationException {
		super( property );
		setExtraArguments( property );
		checkAttributes();
	}

	public DispatchingDocumentFactory( final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) {
		super( defaultMetadata );
		checkAttributes(); // Will certainly fail because the configuration is actually missing
	}

	public DispatchingDocumentFactory() {
		super();
		checkAttributes(); // Will certainly fail because the configuration is actually missing
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected boolean parseProperty( final String key, final String[] values, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws ConfigurationException {
		if ( sameKey( MetadataKeys.FIELDNAME, key ) ) {
			fieldName = values;
			numberOfFields = fieldName.length;
			return true;
		} 
		else if ( sameKey( MetadataKeys.KEY, key ) ) {
			final String dispatchingKeyName = ensureJustOne( key, values );
			final int lastDot = dispatchingKeyName.lastIndexOf( '.' );
			try {
				dispatchingKey = Enum.valueOf( (Class<Enum>)Class.forName( dispatchingKeyName.substring( 0, lastDot ) ),
						dispatchingKeyName.substring( lastDot + 1) );
			}
			catch ( ClassNotFoundException e ) {
				throw new IllegalArgumentException( "The class specified in the key " + dispatchingKeyName + " cannot be found" );
			} 
			return true;
		}
		else if ( sameKey( MetadataKeys.RULE, key ) ) {
			String[] rules = values;
			value2factoryClass = new Object2ObjectLinkedOpenHashMap<String,Class<? extends DocumentFactory>>();
			int i, m = rules.length;
			for ( i = 0; i < m; i++ ) {
				int pos = rules[ i ].indexOf( ':' );
				if ( pos <= 0 || pos == rules[ i ].length() - 1 ) throw new ConfigurationException( "Rule " + rules[ i ] + " does not contain a colon or it is malformed" );
				if ( rules[ i ].indexOf( ':', pos + 1 ) >= 0 ) throw new ConfigurationException( "Rule " + rules[ i ] + " contains too many colons" );
				String factoryName = rules[ i ].substring( pos + 1 );
				Class<? extends DocumentFactory> factoryClass = null;
				try {
					factoryClass = (Class<? extends DocumentFactory>)Class.forName( factoryName );
					if ( ! ( DocumentFactory.class.isAssignableFrom( factoryClass ) ) ) throw new ClassNotFoundException();
				} catch ( ClassNotFoundException e ) {
					throw new ConfigurationException( "ParsingFactory " + factoryName + " is invalid; maybe the package name is missing" );
				}
				value2factoryClass.put( rules[ i ].substring( 0, pos ), factoryClass );
			}
			m = value2factoryClass.values().size();
			return true;
			
		}
		else if ( sameKey( MetadataKeys.MAP, key ) ) {
			String[] pieces = values;
			int i, m = pieces.length;
			rename = new int[ m ][];
			for ( i = 0; i < m; i++ ) {
				String[] subpieces = pieces[ i ].split( ":" );
				if ( i > 0 && subpieces.length != rename[ 0 ].length ) throw new ConfigurationException( "Length mismatch in the map " + values );
				rename[ i ] = new int[ subpieces.length ];
				for ( int k = 0; k < subpieces.length; k++ ) {
					try {
						rename[ i ][ k ] = Integer.parseInt( subpieces[ k ] );
					} catch ( NumberFormatException e ) {
						throw new ConfigurationException( "Number format exception in the map " + values );
					}
				}
			}
		}
		return super.parseProperty( key, values, metadata );
	}
	
	
	public int numberOfFields() {
		return numberOfFields;
	}

	public String fieldName( final int field ) {
		ensureFieldIndex( field );
		return fieldName[ field ];
	}

	public int fieldIndex( final String fieldName ) {
		for ( int k = 0; k < numberOfFields; k++ ) 
			if ( this.fieldName[ k ].equals( fieldName ) ) return k;
		return -1;
	}

	public FieldType fieldType( final int field ) {
		ensureFieldIndex( field );
		return fieldType[ field ];
	}

	/** A word reader that is returned when a null field should be returned. */
	final private WordReader nullReader = new FastBufferedReader();

	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
		
		final int factoryIndex = strategy.factoryNumber( metadata, this );
		System.out.println( "The strategy returned " + factoryIndex );
		if ( factoryIndex < 0 || factoryIndex >= n ) throw new IllegalArgumentException();
		
		System.out.println( "Going to parse a document with " + metadata + ", using " + documentFactory[ factoryIndex ].getClass().getName() );
		
		final DocumentFactory factory = documentFactory[ factoryIndex ];
		final Document document = factory.getDocument( rawContent, metadata );
		
		return new AbstractDocument() {
			public CharSequence title() {
				return document.title();
			}
			
			public String toString() {
				return document.toString();
			}

			public CharSequence uri() {
				return document.uri();
			}

			public Object content( final int field ) throws IOException {
				ensureFieldIndex( field );
				if ( rename[ factoryIndex ][ field ] < 0 ) return NullReader.getInstance();
				return document.content( rename[ factoryIndex ][ field ] );
			}
			
			public WordReader wordReader( final int field ) {
				ensureFieldIndex( field );
				if ( rename[ factoryIndex ][ field ] < 0 ) return nullReader;
				return document.wordReader( rename[ factoryIndex ][ field ] ); 
			}

			public void close() throws IOException {
				super.close();
				document.close();
			}
		};

	}
	
	
	public static void main( final String[] arg ) throws IOException, ConfigurationException {
		//PdfDocumentFactory pdfFactory = new PdfDocumentFactory();
		//HtmlDocumentFactory htmlFactory = new HtmlDocumentFactory();
		//IdentityDocumentFactory idFactory = new IdentityDocumentFactory();
		//Object2IntMap map = new Object2IntOpenHashMap(
		//		new String[] { "application/pdf", "text/html" },
		//		new int[] { 0, 1 }
		//	);
		//map.defaultReturnValue( 2 );
		//DispatchingStrategy strategy = new StringBasedDispatchingStrategy( MetadataKeys.MIMETYPE, map	);
		
		Properties p = new Properties();
		p.addProperty( MetadataKeys.FIELDNAME.name().toLowerCase(), "text,title" );
		p.addProperty( MetadataKeys.KEY.name().toLowerCase(), PropertyBasedDocumentFactory.MetadataKeys.MIMETYPE.name() );
		p.addProperty( MetadataKeys.RULE.name().toLowerCase(), "application/pdf:it.unimi.di.big.mg4j.document.PdfDocumentFactory,text/html:it.unimi.di.big.mg4j.document.HtmlDocumentFactory,?:it.unimi.di.big.mg4j.document.IdentityDocumentFactory" );
		p.addProperty( MetadataKeys.MAP.name().toLowerCase(), "0:-1,0:1,0:-1" );
		p.addProperty( MetadataKeys.MAP.name().toLowerCase(), "0:-1,0:1,0:-1" );
		p.addProperty( MetadataKeys.MAP.name().toLowerCase(), "0:-1,0:1,0:-1" );
		p.addProperty( PropertyBasedDocumentFactory.MetadataKeys.ENCODING.name().toLowerCase(), "iso-8859-1" );
		
		DispatchingDocumentFactory factory = new DispatchingDocumentFactory( p ); 
		DocumentCollection dc = new FileSetDocumentCollection( arg, factory );
		BinIO.storeObject( dc, "test.collection" );
	}
}
