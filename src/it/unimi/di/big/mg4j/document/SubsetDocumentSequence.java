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
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.util.LongInterval;
import it.unimi.dsi.util.LongIntervals;

import java.io.IOException;
import java.io.Serializable;

/** A collection that exhibits a subset of documents (possibly not contiguous) from a given sequence.
 * 
 * <p>This class provides several string-based constructors that use the {@link ObjectParser}
 * conventions; they can be used to generate easily subcollections from the command line.
 * 
 * @author Paolo Boldi
 *
 */

public class SubsetDocumentSequence extends AbstractDocumentSequence implements Serializable {
	private static final long serialVersionUID = 1L;
	/** The underlying document sequence. */
	final DocumentSequence underlyingSequence;
	/** The set of document pointers to be retained. */
	final LongSet documents;

	/** Creates a new subsequence.
	 * 
	 * @param underlyingSequence the underlying document sequence.
	 * @param documents in the subsequence.
	 */
	public SubsetDocumentSequence( DocumentSequence underlyingSequence, LongSet documents ) {
		this.underlyingSequence = underlyingSequence;
		this.documents = documents;
	}

	/** Creates a new subsequence.
	 * 
	 * @param underlyingSequence the underlying document sequence.
	 * @param first the first document (inclusive) in the subsequence.
	 * @param last the last document (exclusive) in this subsequence.
	 */
	public SubsetDocumentSequence( DocumentSequence underlyingSequence, long first, long last ) {
		this.underlyingSequence = underlyingSequence;
		this.documents = last <= first? LongIntervals.EMPTY_INTERVAL : LongInterval.valueOf( first, last - 1 );
	}


	/** Creates a new subsequence.
	 * 
	 * @param underlyingSequenceFilename the filename of the underlying document sequence.
	 * @param documentFileFilename the filename of a file containing a serialized version of the set of document
	 * pointers to be retained.
	 */
	public SubsetDocumentSequence( String underlyingSequenceFilename, String documentFileFilename ) throws NumberFormatException, IllegalArgumentException, SecurityException, IOException, ClassNotFoundException {
		this( AbstractDocumentSequence.load( underlyingSequenceFilename ), (LongSet)BinIO.loadObject( documentFileFilename ) );
	}
	
	/** Creates a new subsequence.
	 * 
	 * @param underlyingSequenceFilename the filename of the underlying document sequence.
	 * @param first the first document (inclusive) in the subsequence.
	 * @param last the last document (exclusive) in this subsequence.
	 */
	public SubsetDocumentSequence( String underlyingSequenceFilename, String first, String last ) throws NumberFormatException, IllegalArgumentException, SecurityException, IOException, ClassNotFoundException {
		this( AbstractDocumentSequence.load( underlyingSequenceFilename ), Long.parseLong( first ), Long.parseLong( last ) );
	}

	
	@Override
	public DocumentIterator iterator() throws IOException {
		final DocumentIterator underlyingIterator = underlyingSequence.iterator();
		return new AbstractDocumentIterator() {
			long docPointer = -1;
			boolean over = false, closed = false;

			@Override
			public Document nextDocument() throws IOException {
				Document document;
				if ( over ) return null;
				for(;;) {
					document = underlyingIterator.nextDocument();
					docPointer++;
					if ( document != null ) {
						if ( documents.contains( docPointer ) ) break;
						document.close();
					}
					else break;
				}
				over = document == null;
				return document;
			}
			
			@Override
			public void close() throws IOException {
				if ( !closed ) {
					underlyingIterator.close();
					super.close();
				}
				closed = true;
			}
			
		};
	}

	
	@Override
	public DocumentFactory factory() {
		return underlyingSequence.factory();
	}

	@Override
	public void close() throws IOException {
		underlyingSequence.close();
		super.close();
	}
}
