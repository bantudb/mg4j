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

import java.io.IOException;
import java.io.InputStream;

/** A factory that replicates a given factory several times. A special case of a composite factory.
 * 
 * <p>Note that in general replicated factories support only sequential access to
 * field content (albeit skipping items is allowed). 
 */
public class ReplicatedDocumentFactory extends AbstractDocumentFactory {
	private static final long serialVersionUID = 2L;

	/** The document factory that will be replicated. */
	public final DocumentFactory documentFactory;
	/** The number of copies. */
	public final int numberOfCopies;
	/** The map from field names to field indices. */
	private final Object2IntOpenHashMap<String> field2Index;
	/** The field names. */
	private final String[] fieldName;
	
	/** Creates a new replicated document factory.
	 * 
	 * @param documentFactory the factory that will be replicated. 
	 * @param numberOfCopies the number of copies.
	 * @param fieldName the names to be given to the fields of the new factory.
	 */
	private ReplicatedDocumentFactory( final DocumentFactory documentFactory, final int numberOfCopies, final String[] fieldName ) {
		this.documentFactory = documentFactory;
		this.numberOfCopies = numberOfCopies;
		this.fieldName = fieldName;
		if ( numberOfFields() != fieldName.length ) throw new IllegalArgumentException( "The number of field names (" + fieldName.length + ") is not equal to the number of fields in the replicated factory (" + numberOfFields() + ")" );
		field2Index = new Object2IntOpenHashMap<String>( fieldName.length, .5f );
		field2Index.defaultReturnValue( -1 );
		for( int i = 0; i < fieldName.length; i++ ) field2Index.put( fieldName[ i ], i );
		if ( field2Index.size() != fieldName.length ) throw new IllegalArgumentException( "The field name array " + ObjectArrayList.wrap( fieldName ) + " contains duplicates" );
	}
	
	protected ReplicatedDocumentFactory( final DocumentFactory documentFactory,  final int numberOfCopies, final String[] fieldName, final Object2IntOpenHashMap<String> field2Index ) {
		this.documentFactory = documentFactory;
		this.numberOfCopies = numberOfCopies;
		this.field2Index = field2Index;
		this.fieldName = fieldName;
	}
	
	/** Returns a document factory replicating the given factory. 
	 * 
	 * @param documentFactory the factory that will be replicated.
	 * @param numberOfCopies the number of copies. 
	 * @return a replicated document factory.
	 */
	public static DocumentFactory getFactory( final DocumentFactory documentFactory, final int numberOfCopies, final String[] fieldName ) {
		//if ( numberOfCopies == 1 ) return documentFactory; TODO: should be optimised if no renaming is done.
		return new ReplicatedDocumentFactory( documentFactory, numberOfCopies, fieldName );
	}
	
	public ReplicatedDocumentFactory copy() {
		return new ReplicatedDocumentFactory( documentFactory.copy(), numberOfCopies, fieldName, field2Index );
	}
	
	public int numberOfFields() {
		return numberOfCopies * documentFactory.numberOfFields();
	}

	public String fieldName( final int field ) {
		ensureFieldIndex( field );
		return fieldName[ field ];
	}

	public int fieldIndex( final String fieldName ) {
		return field2Index.getInt( fieldName );
	}

	public FieldType fieldType( final int field ) {
		ensureFieldIndex( field );
		return documentFactory.fieldType( field % documentFactory.numberOfFields() );
	}

	/** A document obtained by replication of the underlying-factory document. */

	protected class ReplicatedDocument extends AbstractDocument {
		/** The last returned field. */
		private int currField = -1;
		/** The current document. */
		private Document currDocument;
		/** The title returned by the first factory. */
		private CharSequence title;
		/** The uri returned by the first factory. */
		private CharSequence uri;

		private final InputStream rawContent;
		private final Reference2ObjectMap<Enum<?>,Object> metadata;

		protected ReplicatedDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
			this.rawContent = rawContent;
			this.metadata = metadata;
			currDocument = documentFactory.getDocument( rawContent, metadata );
			title = currDocument.title();
			uri = currDocument.uri();
		}

		public CharSequence title() {
			return title;
		}

		public String toString() {
			return title().toString();
		}

		public CharSequence uri() {
			return uri;
		}

		public Object content( final int field ) throws IOException {
			ensureFieldIndex( field );
			if ( field <= currField ) throw new IOException( "Composite document factories require sequential access" );
			while( currField < field ) {
				currField++;
				if ( currField % documentFactory.numberOfFields() == 0 ) { 
					if ( currField > 0 ) rawContent.reset();
					currDocument = documentFactory.getDocument( rawContent, metadata );
				}
			}
			return currDocument.content( field % documentFactory.numberOfFields() );
		}

		public WordReader wordReader( final int field ) {
			ensureFieldIndex( field );
			return currDocument.wordReader( field % documentFactory.numberOfFields() ); 
		}

	}
	
	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
		return new ReplicatedDocument( rawContent, metadata );
	}
}
