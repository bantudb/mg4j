package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2009-2016 Sebastiano Vigna 
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
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** A document collection exhibiting a list of underlying document collections, called <em>segments</em>,
 * as a single collection. The underlying collections are (virtually) <em>concatenated</em>&mdash;that is,
 * the first document of the second collection is renumbered to the size of the first collection, and so on.
 * All underlying collections must use the same {@linkplain DocumentFactory factory class}.
 *
 * <p>A main method makes it easy to create concatenated collections given the filenames of the component collections.
 */

public class ConcatenatedDocumentCollection extends AbstractDocumentCollection implements Serializable {
	private static final long serialVersionUID = 1L;
	/** The name of the collections composing this concatenated document collection. */
	private final String[] collectionName;
	/** The collections composing this concatenated document collection. */
	private transient DocumentCollection[] collection;
	/** The length of {@link #collection}. */
	private final int n;
	/** The array of starting documents (the last element is the overall number of documents). */
	private long startDocument[];
	
	/** Creates a new concatenated document collection using giving component collections.
	 * 
	 * @param collection a list of component collections.
	 */
	protected ConcatenatedDocumentCollection( final String[] collectionName, final DocumentCollection[] collection ) {
		if ( collection.length != collectionName.length ) throw new IllegalArgumentException();
		this.collectionName = collectionName;
		this.collection = collection;
		this.n = collection.length;

		startDocument = new long[ n + 1 ];
		for( int i = 0; i < n; i++ ) startDocument[ i + 1 ] = startDocument[ i ] + collection[ i ].size();
	}

	private void initCollections( final CharSequence filename, boolean rethrow ) throws IllegalArgumentException, SecurityException, IOException, ClassNotFoundException {
		try {
			collection = new DocumentCollection[ n ];
			File parent = filename != null ? new File( filename.toString() ).getParentFile() : null;
			for( int i = n; i-- != 0; ) collection[ i ] = (DocumentCollection)AbstractDocumentSequence.load( new File( parent, collectionName[ i ] ).toString() );
			if ( n > 0  ) {
				Class<? extends DocumentFactory> factoryClass = collection[ 0 ].factory().getClass();
				// TODO: this is crude. We should have a contract for equality of factories, and use equals().
				for( int i = 0; i < n; i++ ) if ( collection[ i ].factory().getClass() != factoryClass ) throw new IllegalArgumentException( "All segment in a concatenated document collection must used the same factory class" );
			}
			startDocument = new long[ n + 1 ];
			for( int i = 0; i < n; i++ ) startDocument[ i + 1 ] = startDocument[ i ] + collection[ i ].size();
		}
		catch( IOException e ) {
			if ( rethrow ) throw e;
		}
	}

	private void ensureCollections() {
		if ( collection == null ) {
			filename( null ); // We need collection, but no one called filename(), so we assume a relative path.
			if ( collection == null ) throw new IllegalStateException( "The collections composing this " + ConcatenatedDocumentCollection.class.getName() + " have not been loaded correctly; please use " + AbstractDocumentSequence.class.getSimpleName() + ".load() or call filename() after deserialising this instance, and ensure that the names stored are correct " );
		}
	}
	
	/** Creates a new, partially uninitialised concatenated document collection using giving component collections names.
	 * 
	 * @param collectionName a list of names of component collections.
	 */
	public ConcatenatedDocumentCollection( String... collectionName ) throws IllegalArgumentException, SecurityException {
		this.collectionName = collectionName;
		n = collectionName.length;
	}

	public void filename( CharSequence filename ) {
		try {
			initCollections( filename, true );
		}
		catch ( IllegalArgumentException e ) {
			throw new RuntimeException( e );
		}
		catch ( SecurityException e ) {
			throw new RuntimeException( e );
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
		catch ( ClassNotFoundException e ) {
			throw new RuntimeException( e );
		}
	}
	
	public DocumentCollection copy() {
		final DocumentCollection[] collection = new DocumentCollection[ n ];
		for( int i = n; i-- != 0; ) collection[ i ] = this.collection[ i ].copy();
		return new ConcatenatedDocumentCollection( collectionName, collection );
	}

	
	public Document document( final long index ) throws IOException {
		ensureDocumentIndex( index );
		ensureCollections();
		int segment = Arrays.binarySearch( startDocument, index );
		if ( segment < 0 ) segment = -segment - 2;
		return collection[ segment ].document(  (int)( index - startDocument[ segment ] ) );
	}

	
	public Reference2ObjectMap<Enum<?>,Object> metadata( long index ) throws IOException {
		ensureDocumentIndex( index );
		ensureCollections();
		int segment = Arrays.binarySearch( startDocument, index );
		if ( segment < 0 ) segment = -segment - 2;
		return collection[ segment ].metadata(  (int)( index - startDocument[ segment ] ) );
	}

	
	public long size() {
		return startDocument[ n ];
	}

	
	public InputStream stream( final long index ) throws IOException {
		ensureDocumentIndex( index );
		ensureCollections();
		int segment = Arrays.binarySearch( startDocument, index );
		if ( segment < 0 ) segment = -segment - 2;
		return collection[ segment ].stream(  (int)( index - startDocument[ segment ] ) );
	}

	
	public DocumentFactory factory() {
		ensureCollections();
		return collection[ 0 ].factory();
	}
	
	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		initCollections( null, false );
	}

	public void close() throws IOException {
		super.close();
		for( DocumentCollection c: collection ) c.close();
	}
	
	public static void main( final String[] arg ) throws IOException, JSAPException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		SimpleJSAP jsap = new SimpleJSAP(
				TRECDocumentCollection.class.getName(), "Saves a serialised concatenated document collection, given the filenames of the component collections.",
				new Parameter[] {
						new UnflaggedOption( "collection", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename of the resulting collection." ),
						new UnflaggedOption( "component", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.GREEDY, "Multiple filenames specifying a series of collections." ) 
		} );

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		BinIO.storeObject( new ConcatenatedDocumentCollection( jsapResult.getStringArray( "component" ) ), jsapResult.getString( "collection" ) );
	}

}
