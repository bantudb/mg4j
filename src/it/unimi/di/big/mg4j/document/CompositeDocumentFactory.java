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
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.FlyweightPrototypes;

import java.io.IOException;
import java.io.InputStream;

/** A composite factory that passes the input stream to a sequence of factories in turn.
 * 
 * <p>Factories can be composed. A composite factory will pass in turn
 * the input stream given to {@link #getDocument(InputStream, Reference2ObjectMap)}
 * to the underlying factories, after calling {@link java.io.InputStream#reset()}. 
 * {@linkplain it.unimi.di.big.mg4j.document.DocumentSequence Document sequences} using
 * composite factories must pass to {@link #getDocument(InputStream, Reference2ObjectMap)}
 * a {@link it.unimi.dsi.io.MultipleInputStream} that can be reset enough times.
 * 
 * <p>Note that in general composite factories support only sequential access to
 * field content (albeit skipping items is allowed). 
 */
public class CompositeDocumentFactory extends AbstractDocumentFactory {
	private static final long serialVersionUID = 1L;

	/** The array of document factories composing this composite document factory. */
	private final DocumentFactory[] documentFactory;
	/** The overall number of fields (i.e., the sum of {@link DocumentFactory#numberOfFields()} over {@link #documentFactory}. */
	private final int numberOfFields;
	/** The name of all fields in sequence. */
	private final String[] fieldName;
	/** The type of all fields in sequence. */
	private final FieldType[] fieldType;
	/** The map from field names to field indices. */
	private final Object2IntOpenHashMap<String> field2Index;
	
	// These fields are exposed for usage in CompositeDocumentSequence
	
	/** The factory of each field. */
	final int[] factoryIndex;
	/** The index of each field in its own factory. */
	final int[] originalFieldIndex;
	
	/** Creates a new composite document factory using the factories in a given array.
	 * 
	 * @param documentFactory an array of document factories that will composed. 
	 * @param fieldName an array of names for the resulting field, or <code>null</code>.
	 */
	protected CompositeDocumentFactory( final DocumentFactory[] documentFactory, String[] fieldName ) {
		this.documentFactory = documentFactory;
		int n = 0;
		for( int i = 0; i < this.documentFactory.length; i++ ) n += documentFactory[ i ].numberOfFields();
		numberOfFields = n;
		final boolean hasNames = (fieldName != null); 
		if ( hasNames && fieldName.length != numberOfFields ) throw new IllegalArgumentException( "There is a mismatch between the number of fields (" + numberOfFields + ") and the number of names (" + fieldName.length + ")" );
		
		this.fieldName = hasNames ? fieldName : new String[ numberOfFields ];
		
		fieldType = new FieldType[ numberOfFields ];
		factoryIndex = new int[ numberOfFields ];
		originalFieldIndex = new int[ numberOfFields ];
		
		n = 0;
		for( int i = 0; i < this.documentFactory.length; i++ ) {
			for( int j = 0; j < documentFactory[ i ].numberOfFields(); j++ ) {
				if ( ! hasNames ) this.fieldName[ n ] = documentFactory[ i ].fieldName( j );
				fieldType[ n ] = documentFactory[ i ].fieldType( j );
				factoryIndex[ n ] = i;
				originalFieldIndex[ n ] = j;
				n++;
			}
		}

		field2Index = new Object2IntOpenHashMap<String>( this.fieldName.length , .5f );
		field2Index.defaultReturnValue( -1 );
		for( int i = 0; i < this.fieldName.length; i++ ) field2Index.put( this.fieldName[ i ], i );
		if ( field2Index.size() != this.fieldName.length ) throw new IllegalArgumentException( "The field name array " + ObjectArrayList.wrap( fieldName ) + " contains duplicates" );
	}
	
	public CompositeDocumentFactory copy() {
		return new CompositeDocumentFactory( FlyweightPrototypes.copy( documentFactory ), fieldName );
	}
	
	/** Returns a document factory composing the given document factories.
	 *  
	 * <p>By passing an optional array of field names, it is possible to rename
	 * the fields of the composing factories.
	 * 
	 * @param documentFactory an array of document factories that will composed.
	 * @param fieldName an array of names for the resulting field, or <code>null</code>.
	 * @return a composed document factory (the first element of the argument, for arguments of length 1).
	 */
	public static DocumentFactory getFactory( final DocumentFactory[] documentFactory, final String[] fieldName ) {
		if ( documentFactory.length == 1 && fieldName == null ) return documentFactory[ 0 ];
		return new CompositeDocumentFactory( documentFactory, fieldName );
	}
	
	/** Returns a document factory composing the given document factories.
	 *  
	 * @param documentFactory document factories that will composed.
	 * @return a composed document factory (the first element of the argument, for arguments of length 1).
	 */
	public static DocumentFactory getFactory( final DocumentFactory... documentFactory ) {
		return getFactory( documentFactory, null );
	}
	
	
	public int numberOfFields() {
		return numberOfFields;
	}

	public String fieldName( final int field ) {
		ensureFieldIndex( field );
		return fieldName[ field ];
	}

	public int fieldIndex( final String fieldName ) {
		return field2Index.getInt( fieldName );
	}

	public FieldType fieldType( int field ) {
		ensureFieldIndex( field );
		return fieldType[ field ];
	}

	/** A document obtained by composition of documents of underyling factories. */
	
	protected class CompositeDocument extends AbstractDocument {
		/** The current document factory. */
		private int currFactory = 0;
		/** The last returned field. */
		private int currField = -1;
		/** The document returned by the current factory. */
		private Document currDocument;
		/** The title returned by the first factory. */
		private CharSequence title;
		/** The uri returned by the first factory. */
		private CharSequence uri;

		private final Reference2ObjectMap<Enum<?>,Object> metadata;
		private final InputStream rawContent;

		protected CompositeDocument( final Reference2ObjectMap<Enum<?>,Object> metadata, final InputStream rawContent ) throws IOException {
			this.metadata = metadata;
			this.rawContent = rawContent;
			currDocument = documentFactory[ 0 ].getDocument( rawContent, metadata );
			title = currDocument.title();
			uri = currDocument.uri();
		}

		public CharSequence title() {
			return title;
		}

		public String toString() {
			return title.toString();
		}

		public CharSequence uri() {
			return uri;
		}

		public Object content( final int field ) throws IOException {
			ensureFieldIndex( field );
			if ( field <= currField ) throw new IllegalStateException( "Composite document factories require sequential access" );
			if ( currFactory < factoryIndex[ field ] ) {
				while( currFactory < factoryIndex[ field ] ) {
					rawContent.reset();
					currFactory++;
				}
				if ( currDocument != null ) currDocument.close();
				currDocument = documentFactory[ currFactory ].getDocument( rawContent, metadata );
			}

			currField = field;
			return currDocument.content( originalFieldIndex[ field ] );
		}

		public WordReader wordReader( final int field ) {
			ensureFieldIndex( field );
			// ALERT: put back
			//if ( field != currField ) throw new IllegalStateException( "The specified field (" + field + ") is not the one of the last document returned (" + currField + ")" );
			return currDocument.wordReader( originalFieldIndex[ field ] ); 
		}

		public void close() throws IOException {
			if ( currDocument != null ) currDocument.close();
			super.close();
		}
	}

	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
		return new CompositeDocument( metadata, rawContent );
	}
}
