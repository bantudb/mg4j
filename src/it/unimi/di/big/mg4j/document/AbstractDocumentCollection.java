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

import it.unimi.di.big.mg4j.document.DocumentFactory.FieldType;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.SafelyCloseable;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.Reader;

/** An abstract, {@link it.unimi.dsi.io.SafelyCloseable safely closeable} implementation of a document collection. 
 * 
 * <p>This class provides ready-made implementation of the {@link #iterator()} method. 
 * Concrete subclasses are encouraged to provide optimised, reuse-oriented versions of the
 * {@link #iterator()} method. Note that since this implementation 
 * uses {@link it.unimi.di.big.mg4j.document.DocumentCollection#document(long) document()}
 * to implement the iterator, creating two iterators concurrently will usually lead
 * to unpredictable results.
 * 
 * <p>As a commodity, the {@link #ensureDocumentIndex(long)} method can be called
 * whenever it is necessary to check that a document index is not
 * out of range.
 */
public abstract class AbstractDocumentCollection extends AbstractDocumentSequence implements DocumentCollection, SafelyCloseable {
	
	/** Symbolic names for common properties of a {@link it.unimi.di.big.mg4j.document.DocumentCollection}. */
	public static enum PropertyKeys {
		/** The number of documents in the collection. */
		DOCUMENTS,
		/** The number of terms in the collection. */
		TERMS,
		/** The number of nonterms in the collection, or -1 if this collection does not contain nonterms. */
		NONTERMS,
		/** The name of the {@link DocumentCollection} class. */
		COLLECTIONCLASS,			
	}

	/** Checks that the index is correct (between 0, inclusive, and {@link DocumentCollection#size()},
	 *  exclusive), and throws an {@link IndexOutOfBoundsException} if the index is not correct.
	 * 
	 * @param index the index to be checked.
	 */
	protected void ensureDocumentIndex( final long index ) {
		if ( index < 0 || index >= size() )
			throw new IndexOutOfBoundsException( Long.toString( index ) ); 
	}
	
	public DocumentIterator iterator() throws IOException {

		return new AbstractDocumentIterator() {

			private int nextDocumentToBeReturned = 0;
			private Document last;
			
			public Document nextDocument() throws IOException {
				if ( last != null ) last.close();
				return last = nextDocumentToBeReturned < size() ? document( nextDocumentToBeReturned++ ) : null;
			}
		};
	}
	
	public String toString() {
		return this.getClass().getName() + "[size: " + size() + " factory: " + factory() + "]";
	}
	
	/** Prints all documents in a given sequence.
	 * 
	 * @param seq the sequence.
	 */
	public static void printAllDocuments( final DocumentSequence seq ) throws IOException {
		DocumentIterator it = seq.iterator();
		Document document;
		int doc = 0;
		while ( ( document = it.nextDocument() ) != null ) {
			System.out.println( "**** Document # " + doc );
			System.out.println( "* Title: " + document.title() );
			System.out.println( "* URI: " + document.uri() );
			System.out.println( "****" );
			for ( int f = 0; f < seq.factory().numberOfFields(); f++ ) {
				System.out.println( "** Field # " + f + ", " + seq.factory().fieldName( f ) );
				Object field = document.content( f );
				if ( seq.factory().fieldType( f ) == FieldType.TEXT ) {
					Reader reader = (Reader)field;
					WordReader wr = document.wordReader( f );
					wr.setReader( reader );
					MutableString word = new MutableString();
					MutableString nonWord = new MutableString();
						while ( wr.next( word, nonWord ) ) System.out.println( word.toString() + nonWord.toString() );
				} else System.out.println( field );
			}
			doc++;
		}
	}
	
	public static void main( final String[] arg ) throws Exception {
		DocumentSequence coll = (DocumentSequence)BinIO.loadObject( arg[ 0 ] );
		printAllDocuments( coll );
	}
}
