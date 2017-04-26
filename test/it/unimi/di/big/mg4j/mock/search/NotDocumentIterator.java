package it.unimi.di.big.mg4j.mock.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2003-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;



public class NotDocumentIterator extends MockDocumentIterator {


	private final DocumentIterator documentIterator;
	private final long numberOfDocuments;

	/** Creates a NOT document iterator over a given iterator.
	 *
	 * @param documentIterator the iterator to be filtered.
	 * @param numberOfDocuments the number of documents.
	 */
	protected NotDocumentIterator( final DocumentIterator documentIterator, final long numberOfDocuments ) {
		this.documentIterator = documentIterator;
		this.numberOfDocuments = numberOfDocuments;
		indices.addAll( documentIterator.indices() );
		try {
			LongSet inside = new LongOpenHashSet();
			long d;
			while ( ( d = documentIterator.nextDocument() ) != END_OF_LIST ) inside.add( d );
			for ( int i = 0; i < numberOfDocuments; i++ )
				if ( ! inside.contains( i ) )
					for ( Index index: indices )						
						addTrueIteratorDocument( i, index );

			documentIterator.dispose();
			start( true );
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
		
	}

	/** Returns a NOT document iterator over a given iterator.
	 * @param it the iterator to be filtered.
	 * @param numberOfDocuments the number of documents in the collection.
	 */
	public static DocumentIterator getInstance( final DocumentIterator it, final long numberOfDocuments ) {
		return new NotDocumentIterator( it, numberOfDocuments );
	}

	public String toString() {
	   return this.getClass().getSimpleName() + "(" + documentIterator + ", " + numberOfDocuments + ")";
	}
	
}
