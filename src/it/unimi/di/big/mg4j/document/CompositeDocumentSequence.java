package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Sebastiano Vigna 
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

import it.unimi.dsi.io.WordReader;

import java.io.IOException;

/** A document sequence composing a list of underlying sequences.
 *
 * <p>An instance of this class exposes documents formed by juxtaposition of the content
 * of the underlying document sequences. In particular, it instantiates a {@link CompositeDocumentFactory}
 * using the provided optional array of field names.
 * 
 * <p>For instance, by composing a document sequence whose documents have a single text field and a document sequence
 * whose documents have a single integer field we obtain a new sequence whose documents have two fields.
 * 
 * <p>The {@linkplain Document#title() title} and {@linkplain Document#uri() uri} of composed
 * documents are those of the documents returned by the first underlying sequence.   
 */

public class CompositeDocumentSequence extends AbstractDocumentSequence {
	/** The array of underlying document sequences. */
	private final DocumentSequence[] sequence;
	/** The composite factory representing the composed content of the underlying sequences. */
	private CompositeDocumentFactory factory;

	/** Creates a new composite document sequence using the provided underlying document sequences,
	 * which must possess distinct field names.
	 * 
	 * @param sequence an array of underlying document sequences (they <strong>must</strong>
	 * return the same number of documents).
	 */
	public CompositeDocumentSequence( final DocumentSequence... sequence ) {
		this( sequence, null );
	}
		
	/** Creates a new composite document sequence using the provided underlying document sequences.
	 * 
	 * @param sequence an array of underlying document sequences (they <strong>must</strong>
	 * return the same number of documents).
	 * @param fieldName an array of field names for all fields resulting from the composition of the
	 * document factories of the underlying sequences: it will be passed to
	 * {@link CompositeDocumentFactory#CompositeDocumentFactory(DocumentFactory[], String[])}
	 * (so in particular it may be <code>null</code> if all fields have distinct names).
	 */
	public CompositeDocumentSequence( final DocumentSequence[] sequence, final String[] fieldName ) {
		this.sequence = sequence;
		DocumentFactory[] factory = new DocumentFactory[ sequence.length ];
		for( int i = sequence.length; i-- != 0; ) factory[ i ] = sequence[ i ].factory();
		this.factory = new CompositeDocumentFactory( factory, fieldName );
	}

	public void close() throws IOException {
		for( DocumentSequence s: sequence ) s.close();
		super.close();
	}

	public DocumentFactory factory() {
		return factory;
	}

	public DocumentIterator iterator() throws IOException {
		
		return new DocumentIterator() {
			final DocumentIterator[] documentIterator = new DocumentIterator[ sequence.length ];
			final Document[] document = new Document[ sequence.length ];

			{
				for( int i = 0; i < sequence.length; i++ ) documentIterator[ i ] = sequence[ i ].iterator();
			}

			public void close() throws IOException {
				for( DocumentIterator d: documentIterator ) d.close();
			}

			public Document nextDocument() throws IOException {
				boolean someNull = false, allNull = true;
				for( int i = 0; i< documentIterator.length; i++ ) {
					document[ i ] = documentIterator[ i ].nextDocument();
					someNull |= document[ i ] == null;
					allNull &= document[ i ] == null;
				}
				
				if ( someNull != allNull ) throw new IllegalArgumentException( "The underlying document sequences have different lengths" );
				if ( allNull ) return null;
				
				return new AbstractDocument() {

					public Object content( final int field ) throws IOException {
						return document[ factory.factoryIndex[ field ] ].content( factory.originalFieldIndex[ field ] );
					}

					public CharSequence title() {
						return document[ 0 ].title();
					}

					public CharSequence uri() {
						return document[ 0 ].uri();
					}

					public WordReader wordReader( int field ) {
						return document[ factory.factoryIndex[ field ] ].wordReader( factory.originalFieldIndex[ field ] );
					}
					
					public void close() throws IOException {
						super.close();
						for( Document d: document ) d.close();
					}
				};
			}
			
		};
	}
}
