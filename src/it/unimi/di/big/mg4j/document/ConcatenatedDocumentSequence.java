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

import java.io.File;
import java.io.IOException;

/** A document sequence exhibiting a list of underlying document sequences, called <em>segments</em>,
 * as a single sequence. The underlying sequences are (virtually) <em>concatenated</em>&mdash;that is,
 * the first document of the second sequence is renumbered to the size of the first sequence, and so on.
 * All underlying sequences must use the same {@linkplain DocumentFactory factory class}.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 */

public class ConcatenatedDocumentSequence extends AbstractDocumentSequence {
	/** The name of the sequences composing this concatenated document sequence, when using {@link #ConcatenatedDocumentSequence(String[])}. */
	private final String[] sequenceName;
	/** The sequences composing this concatenated document sequence, when using {@link #ConcatenatedDocumentSequence(DocumentSequence[])}. */
	private final DocumentSequence[] sequence;
	/** The factory of the current sequence, or <code>null</code> if no sequence has been read. */
	private DocumentFactory factory;
	private CharSequence filename;
	private DocumentSequence sequence0;
	private int n;
	
	/** Creates a new concatenated document sequence using giving component sequences.
	 * 
	 * @param sequence a list of component sequences.
	 */
	public ConcatenatedDocumentSequence( final DocumentSequence... sequence ) throws IOException {
		this( null, sequence );
	}

	/** Creates a new concatenated document sequence using giving serialised component sequences.
	 * 
	 * @param sequenceName a list of serialised component sequences.
	 */
	public ConcatenatedDocumentSequence( final String... sequenceName ) throws IOException {
		this( sequenceName, null );
	}

	/** Creates a new concatenated document sequence using giving component sequences.
	 * 
	 * @param sequenceName
	 * @param sequence a list of component sequences.
	 */
	protected ConcatenatedDocumentSequence( final String[] sequenceName, final DocumentSequence[] sequence ) throws IOException {
		this.sequenceName = sequenceName;
		this.sequence = sequence;
		n = sequenceName != null ? sequenceName.length : sequence.length;
		sequence0 = getSequence( 0 );
		factory = sequence0.factory();
	}
	
	private DocumentSequence getSequence( final int i ) throws IOException {
		final DocumentSequence s;
		if ( sequence != null  ) s = sequence[ i ];
		else {
			File parent = filename != null ? new File( filename.toString() ).getParentFile() : null;
			try {
				s = AbstractDocumentSequence.load( new File( parent, sequenceName[ i ] ).toString() );
			}
			catch ( SecurityException e ) {
				throw new RuntimeException( e );
			}
			catch ( ClassNotFoundException e ) {
				throw new RuntimeException( e );
			}
			// TODO: this is crude. We should have a contract for equality of factories, and use equals().
			if ( factory != null && s.factory().getClass() != factory.getClass() ) throw new IllegalArgumentException( "All segment in a concatenated document sequence must used the same factory class" );
		}
		
		return s;
	}

	public void filename( CharSequence filename ) {
		this.filename = filename;
	}
	
	public DocumentFactory factory() {
		return factory;
	}

	public void close() throws IOException {
		super.close();
		if ( sequence0 != null ) sequence0.close();
	}

	@Override
	public DocumentIterator iterator() throws IOException {
		return new AbstractDocumentIterator() {
			private DocumentSequence currentSequence = sequence0 == null ? getSequence( 0 ) : sequence0;
			private DocumentIterator currentIterator = currentSequence.iterator();
			private int currSequenceIndex;
			
			@Override
			public Document nextDocument() throws IOException {
				while( true ) {
					if ( currentIterator == null ) return null;
					final Document d;
					if ( ( d = currentIterator.nextDocument() ) != null ) return d;
					currentIterator.close();
					currentSequence.close();
					if ( currentSequence == sequence0 ) sequence0 = null;
					currentIterator = null;
					currentSequence = null;
					
					if ( currSequenceIndex < n - 1 ) {
						currentSequence = getSequence( ++currSequenceIndex );
						currentIterator = currentSequence.iterator();
					}
				}
			}
			
			@Override
			public void close() throws IOException {
				if ( currentIterator != null ) currentIterator.close();
				if ( currentSequence != null ) currentSequence.close();
				if ( currentSequence == sequence0 ) sequence0 = null;
			}
		};
	}
}
