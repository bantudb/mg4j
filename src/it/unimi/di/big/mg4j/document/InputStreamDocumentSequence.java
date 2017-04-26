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

import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;

import java.io.IOException;
import java.io.InputStream;

/** A document sequence obtained by breaking an input stream at a specified separator. 
 * 
 * <p>This document sequences blindly passes to the indexer sequences of characters read
 * in a specified encoding and separated by a specified byte. 
 */

public class InputStreamDocumentSequence extends FastBufferedInputStream implements DocumentSequence {
	/** The byte separating documents in the input stream. */
	private final int separator;
	/** The factory used to return {@link Document}s. */
	private final DocumentFactory factory;
	/** If true, the last returned stream has been exhausted. This variable
	 * will be reset by a call to <code>nextDocument()</code>. */
	private boolean eod = true;
	/** If true, the stream has been exhausted. */
	private boolean eof;
	/** The sequence will not return more than this number of documents. */
	private final int maxDocs;
	
	/** Creates a new document sequence based on a given input stream and separator; the
	 * sequence will not return more than the given number of documents.
	 * 
	 * @param inputStream the input stream containing all documents.
	 * @param separator the separator.
	 * @param factory the factory that will be used to create documents.
	 * @param maxDocs the maximum number of documents returned.
	 */
	
	public InputStreamDocumentSequence( final InputStream inputStream, final int separator, final DocumentFactory factory, final int maxDocs ) {
		super( inputStream );
		this.separator = separator;
		this.factory = factory;
		this.maxDocs = maxDocs;
	}

	/** Creates a new document sequence based on a given input stream and separator.
	 * 
	 * @param inputStream the input stream containing all documents.
	 * @param separator the separator.
	 * @param factory the factory that will be used to create documents.
	 */
	
	public InputStreamDocumentSequence( final InputStream inputStream, final int separator, final DocumentFactory factory ) {
		this( inputStream, separator, factory, Integer.MAX_VALUE );
	}
	
	public DocumentIterator iterator() {
		final Reference2ObjectArrayMap<Enum<?>,Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 2 );
		
		return new DocumentIterator() {
			private long i;
			private Document last;
			
			public Document nextDocument() throws IOException {
				if ( last != null ) last.close();
				if ( eof || i >= maxDocs ) return last = null;
				// If eof is not true, the caller did not exhaust the current document. We do it, however.
				if ( ! eod ) InputStreamDocumentSequence.this.close();
				eod = false;
				final String documentIndex = Long.toString( i++ );
				metadata.put( MetadataKeys.TITLE, documentIndex );
				metadata.put( MetadataKeys.URI, documentIndex );
				return last = InputStreamDocumentSequence.this.noMoreBytes() ? null : factory.getDocument( InputStreamDocumentSequence.this, metadata );
			}

			public void close() {}
		};
	}

	public DocumentFactory factory() {
		return factory;
	}
	
	public boolean noMoreBytes() throws IOException {
		if ( avail > 0 ) return false;
		
		avail = is.read( buffer );
		if ( avail <= 0 ) {
			if ( avail == -1 ) eof = true;
			// Ooops, there's nothing more to read. Let us set up eof and return.
			avail = 0;
			eod = true;
			return true;
		}
    		pos = 0;
    		return false;
	}
	
	public int read() throws IOException {
		if ( eod ) return -1;
		final int nextByte = super.read();

		if ( nextByte == separator ) {
			eod = true;
			return -1;
		}

		if ( nextByte == -1 ) {
			eof = eod = true;
			return -1;
		}
		
		return nextByte;
	}
	
	public int read( final byte[] b ) throws IOException {
		if ( eod ) return -1;
		return read( b, 0, b.length );
	}
	
    public int read( final byte[] b, int offset, int length ) throws IOException {
    	if ( eod ) return -1;
    	if ( length == 0 ) return 0;
    	
    	final int startOffset = offset;
    	int i, l;
    	
        for(;;) {
        	l = Math.min( length, avail );
       		// We scan the buffer for the separator, copying elements in the mean time. 
       		for( i = 0; i < l; i++ ) {
       			if ( buffer[ pos + i ] == separator ) break;
       			b[ offset + i ] = buffer[ pos + i ];
       		}

        	pos += i;
        	avail -= i;
        	
        	offset += i;
        	length -= i;

        	// If we were able to read enough characters, it's over.
        	if ( length == 0 ) return offset - startOffset;
        	// Otherwise, if we found a separator we return.
        	if ( i < l ) {
        		// This will set up eod/eof.
        		read();
        		return offset - startOffset != 0 ? offset - startOffset : -1;
        	}
        	// Finally, in the last case (i == avail) we try to fill the buffer.
        	if ( noMoreBytes() ) return offset - startOffset != 0 ? offset - startOffset : -1;
        }
    }

	public void mark( final int readlimit ) {}

	public boolean markSupported() {
		return false;
	}
	
	/** Returns one if there is an available byte which is not a separator, zero otherwise.
	 * 
	 * <p>This behaviour tries to avoid calls to {@link InputStream#available()}s, which are
	 * unbelievably slow. Stream decoders presently require just to know whether it is
	 * possible to read a character in a nonblocking way or not.
	 * 
	 * @return one if there is an available byte which is not a separator, zero otherwise.
	 */
	
	public int available() throws IOException {
		if ( noMoreBytes() ) return 0;
		return buffer[ 0 ] == separator ? 0 : 1;
	}

	// TODO: rewrite skip()
	public long skip( final long skip ) {
		throw new UnsupportedOperationException();
	}
	
	@Deprecated
	public void reset() {
		throw new UnsupportedOperationException();
	}
	
	public void flush() {
		throw new UnsupportedOperationException();
	}
	
	public void close() throws IOException {
		if ( ! eod ) while( read() != -1 );
		super.close();
	}

	public void filename( CharSequence filename ) throws IOException {
	}
}
