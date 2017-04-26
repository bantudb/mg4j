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


import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.lang.ObjectParser;

import java.io.IOException;
import java.io.InputStream;

/** A collection that exhibits a contiguous subsets of documents from a given collection.
 * 
 * <p>This class provides several string-based constructors that use the {@link ObjectParser}
 * conventions; they can be used to generate easily subcollections from the command line.
 * 
 * @author Sebastiano Vigna
 *
 */

public class SubDocumentCollection extends AbstractDocumentCollection {
	/** The underlying document collection. */
	final DocumentCollection underlyingCollection;
	/** The first document (inclusive) in this subcollection. */
	final long first;
	/** The last document (exclusive) in this subcollection. */
	final long last;

	/** Creates a new subcollection.
	 * 
	 * @param underlyingCollection the underlying document collection.
	 * @param first the first document (inclusive) in the subcollection.
	 * @param last the last document (exclusive) in this subcollection.
	 */
	public SubDocumentCollection( DocumentCollection underlyingCollection, long first, long last ) {
		this.underlyingCollection = underlyingCollection;
		this.first = first;
		this.last = last;
	}

	/** Creates a new subcollection starting from a given document.
	 * 
	 * <p>The new subcollection will contain all documents from the given one onwards.
	 * 
	 * @param underlyingCollection the underlying document collection.
	 * @param first the first document (inclusive) in the subcollection.
	 */
	public SubDocumentCollection( DocumentCollection underlyingCollection, long first ) {
		this( underlyingCollection, first, underlyingCollection.size() );
	}

	/** Creates a new subcollection.
	 * 
	 * @param underlyingCollectionFilename the filename of the underlying document collection.
	 * @param first the first document (inclusive) in the subcollection.
	 * @param last the last document (exclusive) in this subcollection.
	 */
	public SubDocumentCollection( String underlyingCollectionFilename, String first, String last ) throws NumberFormatException, IllegalArgumentException, SecurityException, IOException, ClassNotFoundException {
		this( (DocumentCollection)AbstractDocumentSequence.load( underlyingCollectionFilename ),
				Long.parseLong( first ), Long.parseLong( last ) );
	}

	/** Creates a new subcollection starting from a given document.
	 * 
	 * <p>The new subcollection will contain all documents from the given one onwards.
	 * 
	 * @param underlyingCollectionFilename the filename of the underlying document collection.
	 * @param first the first document (inclusive) in the subcollection.
	 */
	public SubDocumentCollection( String underlyingCollectionFilename, String first ) throws NumberFormatException, IllegalArgumentException, SecurityException, IOException, ClassNotFoundException {
		this( (DocumentCollection)AbstractDocumentSequence.load( underlyingCollectionFilename ),
				Long.parseLong( first ) );
	}
	
	public DocumentCollection copy() {
		return new SubDocumentCollection( underlyingCollection.copy(), first, last );
	}

	public Document document( long index ) throws IOException {
		ensureDocumentIndex( index );
		return underlyingCollection.document( first + index );
	}

	public long size() {
		return last - first;
	}

	public Reference2ObjectMap<Enum<?>, Object> metadata( long index ) throws IOException {
		ensureDocumentIndex( index );
		return underlyingCollection.metadata( first + index );
	}

	public InputStream stream( long index ) throws IOException {
		ensureDocumentIndex( index );
		return underlyingCollection.stream( first + index );
	}

	public DocumentFactory factory() {
		return underlyingCollection.factory();
	}
}
