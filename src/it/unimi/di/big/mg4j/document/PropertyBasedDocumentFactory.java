package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.util.Properties;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A document factory initialised by default properties.
 * 
 * <p>Many document factories need a number of default values that are used when
 * the metadata passed to 
 * {@link it.unimi.di.big.mg4j.document.DocumentFactory#getDocument(java.io.InputStream,Reference2ObjectMap)} is
 * not sufficient or lacks some key. This abstract class provides a common base for all such factories.
 * 
 * <p>All concrete implementations of this class should have:
 * <ol>
 * <li>an empty constructor;
 * <li>a constructor taking a {@link it.unimi.dsi.fastutil.objects.Reference2ObjectMap}
 * having {@link java.lang.Enum} keys;
 * <li>a constructor taking a {@link Properties} object;
 * <li>a constructor taking a string array.
 * </ol>
 * 
 * <p>In the third case, the properties will be parsed by the {@link #parseProperties(Properties)}
 * method. In the fourth case, by the {@link #parseProperties(String[])} method.
 * 
 * <p>Since all implementations are expected to provide such constructors, corresponding
 * {@linkplain #getInstance(Class, String[]) static factory methods} have been provided to
 * simplify factory instantiation.
 * 
 * <p>If the implementation needs to read and parse some key, it must override the 
 * {@link #parseProperty(String, String[], Reference2ObjectMap)} method.
 * 
 * <p>Keys are specified with a dotted notation. The last dot-separated token is the actual key. The prefix is used
 * to select properties: only properties with a prefix that is a prefix of the current class name are considered.
 * Moreover, if a property with a completely specified prefix (i.e., a prefix that is a class name) is not parsed
 * an exception will be thrown.
 * 
 * <p>This class provide helpers methods {@link #resolve(Enum, Reference2ObjectMap)} and {@link #resolveNotNull(Enum, Reference2ObjectMap)}
 * to help in writing implementations of {@link it.unimi.di.big.mg4j.document.DocumentFactory#getDocument(java.io.InputStream,Reference2ObjectMap)} that
 * handle default metadata correctly.
 */

public abstract class PropertyBasedDocumentFactory extends AbstractDocumentFactory {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger( PropertyBasedDocumentFactory.class );

	/** Case-insensitive keys for metadata passed to 
	 * {@link it.unimi.di.big.mg4j.document.DocumentFactory#getDocument(java.io.InputStream,it.unimi.dsi.fastutil.objects.Reference2ObjectMap)}. 
	 * 
	 * <p>The keys in this class are general-purpose keys that are meaningful for most factories.
	 * Specific factory implementations might choose to interpret more keys, but then it is
	 * up to the {@link it.unimi.di.big.mg4j.document.DocumentSequence} that uses the factory to 
	 * provide data for those keys.
	 * 
	 * <p>Note that the metadata map is a <em>reference</em> map. We cannot use
	 * an {@link java.util.EnumMap} because we do not know in advance the enum(s) whose items will be put
	 * in the map.
	 */
	public static enum MetadataKeys {
		/** The tag for a document title (a character sequence). */
		TITLE,
		/** The tag for a document uri (a character sequence). */
		URI,
		/** The tag for MIME type metadata (a string). */
		MIMETYPE,
		/** The tag for charset encoding metadata (a string normalised through {@link java.nio.charset.Charset#forName(java.lang.String)}). */
		ENCODING,
		/** The tag for the optional name of a {@linkplain WordReader word reader} class. */
		WORDREADER,
		/** The tag for locale metadata (a {@link java.util.Locale}). */
		LOCALE
	};


	/** The set of default metadata for this factory. It is initalised by {@link #parseProperties(Properties)}.
	 * 
	 */
	protected Reference2ObjectMap<Enum<?>,Object> defaultMetadata;

	public static PropertyBasedDocumentFactory getInstance( final Class<?> klass, final String[] property ) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException { 
		return (PropertyBasedDocumentFactory)( klass.getConstructor( new Class[] { String[].class } ).newInstance( new Object[] { property } ) ); 
	}
	
	public static PropertyBasedDocumentFactory getInstance( final Class<?> klass, final Properties properties ) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException { 
		return (PropertyBasedDocumentFactory)( klass.getConstructor( Properties.class ).newInstance( properties ) ); 
	}

	public static PropertyBasedDocumentFactory getInstance( final Class<?> klass ) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException { 
		return (PropertyBasedDocumentFactory)( klass.getConstructor().newInstance() ); 
	}

	public static PropertyBasedDocumentFactory getInstance( final Class<?> klass, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException { 
		return (PropertyBasedDocumentFactory)( klass.getConstructor( new Class[] { Reference2ObjectMap.class } ).newInstance( new Object[] { metadata } ) ); 
	}
	
	private void logMetadata() {
		LOGGER.debug( this.getClass().getName() + " initialised with default metadata " + defaultMetadata );
	}
	
	protected PropertyBasedDocumentFactory( final Reference2ObjectMap<Enum<?>,Object>defaultMetadata ) {
		this.defaultMetadata = defaultMetadata;
		logMetadata();
	}

	protected PropertyBasedDocumentFactory( final Properties properties ) throws ConfigurationException {
		this.defaultMetadata = parseProperties( properties );
		logMetadata();
	}

	protected PropertyBasedDocumentFactory( final String[] property ) throws ConfigurationException {
		this.defaultMetadata = parseProperties( property );
		logMetadata();
	}

	@SuppressWarnings("unchecked")
	protected PropertyBasedDocumentFactory() {
		this.defaultMetadata = Reference2ObjectMaps.EMPTY_MAP;
		logMetadata();
	}

	/** A utility method checking whether the downcased name of an {@link Enum} is equal to a given string.
	 * 
	 *  <p>This class uses an {@link Enum} ({@link MetadataKeys}) to store valid property keys. We follow
	 *  both the uppercase naming convention for enums and the lowercase naming convention for properties,
	 *  and this method encapsulates the method calls that necessary to correctly handle key parsing. 
	 * 
	 * @param enumKey a key expressed as an {@link Enum}.
	 * @param key a key expressed as a string.
	 * @return true if <code>key</code> is equal to the downcased {@linkplain Enum#name() name} of <code>enumKey</code>.
	 */
	public static boolean sameKey( final Enum<?> enumKey, final String key ) {
		return key.equals( enumKey.name().toLowerCase() );
	}
	
	/** Parses a property with given key and value, adding it to the given map. 
	 *
	 * <p>Currently this implementation just parses the {@link MetadataKeys#LOCALE} property.
	 * <P>Subclasses should do their own parsing, returing true in case of success and
	 * returning <code>super.parseProperty()</code> otherwise. 
	 *  
	 * @param key the property key.
	 * @param valuesUnused the property value; this is an array, because properties may have a list of comma-separated values.
	 * @param metadataUnused the metadata map.
	 * @return true if the property was parsed correctly, false if it was ignored.
	 *
	 */
	protected boolean parseProperty( final String key, final String[] valuesUnused, final Reference2ObjectMap<Enum<?>,Object> metadataUnused ) throws ConfigurationException {
		if ( sameKey( MetadataKeys.LOCALE, key ) ) throw new ConfigurationException( "Locales are currently unsupported" );
		return false;
	}
	
	/** This method checks that the array of values contains just one element, and returns the element.
	 * 
	 * @param key the property name (used to build the exception message).
	 * @param values the array of values.
	 * @return the only value (if the array contains exactly one element).
	 * @throws ConfigurationException iff <var>values</var> does not contain a single element.
	 */
	protected static String ensureJustOne( final String key, final String[] values ) throws ConfigurationException {
		if ( values.length != 1 ) throw new ConfigurationException( "Property " + key + " should have just one value" );
		return values[ 0 ];
	}
	
	/** Scans the property set, parsing the properties that concern this class.
	 * 
	 * @param properties a set of properties.
	 * @return a metadata map.
	 */
	@SuppressWarnings("unchecked")
	public Reference2ObjectMap<Enum<?>,Object> parseProperties( final Properties properties ) throws ConfigurationException {
		String key, qualifier, className = this.getClass().getName();
		int lastDot;
		Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>();
		
		for( Iterator<?> i = properties.getKeys(); i.hasNext(); ) {
			key = i.next().toString();
			lastDot = key.lastIndexOf( '.' );
			qualifier = lastDot == -1 ? "" : key.substring( 0, lastDot );

			if ( className.startsWith( qualifier )
				&& ! parseProperty( key.substring( lastDot + 1 ), properties.getStringArray( key ), metadata ) 
				&& className.equals( qualifier ) )
					throw new ConfigurationException( "Unknown property " + key );
		}
		
		return metadata.isEmpty() ? Reference2ObjectMaps.EMPTY_MAP : metadata; 
	}

	/** Parses the given list of properties either as <samp><var>key</var>=<var>value</var></samp> specs (<var>value</var> may
	 * be a list of comma-separated values), or as filenames.
	 * 
	 * @param property an array of strings specifying properties.
	 * @return a metadata map.
	 */
	public Reference2ObjectMap<Enum<?>,Object> parseProperties( final String[] property ) throws ConfigurationException {
		final Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>();
		Properties properties;
		int pos;
		for( int i = 0; i < property.length; i++ ) {
			if ( ( pos = property[ i ].indexOf( '=' ) ) != - 1 ) {
				properties = new Properties();
				properties.addProperty( property[ i ].substring( 0, pos ), property[ i ].substring( pos + 1 ) );
			}
			else properties =  new Properties( property[ i ] );
				
			metadata.putAll( parseProperties( properties ) );;
		}
		
		return metadata;
	}
	
	/** Resolves the given key against the given metadata, falling back to the default metadata.
	 * 
	 * @param key a key.
	 * @param metadata a metadata map.
	 * @return the value returned by <code>metadata</code> for <code>key</code>, or the value
	 * returned by {@link #defaultMetadata} for <code>key</code> if the former is <code>null</code> (the latter,
	 * of course, might be <code>null</code>).
	 */
	
	protected Object resolve( final Enum<?> key, final Reference2ObjectMap<Enum<?>,Object> metadata ) {
		Object value = metadata.get( key );
		return value != null ? value : defaultMetadata.get( key );
	}
	
	/** Resolves the given key against the given metadata, falling back to the provided object.
	 * 
	 * @param key a key.
	 * @param metadata a metadata map.
	 * @param o a default object. 
	 * @return the value returned by <code>metadata</code> for <code>key</code>, or <code>o</code> if the
	 * former is <code>null</code>.
	 */
	
	protected Object resolve( final Enum<?> key, final Reference2ObjectMap<Enum<?>,Object> metadata, final Object o ) {
		Object value = metadata.get( key );
		return value != null ? value : o;
	}
	

	/** Resolves the given key against the given metadata, falling back to the default metadata
	 * and guaranteeing a non-<code>null</code> result.
	 * 
	 * @param key a key.
	 * @param metadata a metadata map.
	 * @return the value returned by <code>metadata</code> for <code>key</code>, or the value
	 * returned by {@link #defaultMetadata} for <code>key</code> if the former is <code>null</code>; if the
	 * latter is <code>null</code>, too, a {@link NoSuchElementException} will be thrown.
	 */
	protected Object resolveNotNull( final Enum<?> key, final Reference2ObjectMap<Enum<?>,Object> metadata ) {
		final Object value = resolve( key, metadata );
		if ( value == null ) throw new NoSuchElementException( "The key " + key + " cannot be resolved" );
		return value;
	}
	
	public String toString() {
		return super.toString() + defaultMetadata;
	}
}
