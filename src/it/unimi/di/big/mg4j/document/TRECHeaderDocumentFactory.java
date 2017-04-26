package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna
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

import static it.unimi.di.big.mg4j.document.TRECDocumentCollection.DOCHDR_CLOSE;
import static it.unimi.di.big.mg4j.document.TRECDocumentCollection.DOCHDR_OPEN;
import static it.unimi.di.big.mg4j.document.TRECDocumentCollection.DOCNO_CLOSE;
import static it.unimi.di.big.mg4j.document.TRECDocumentCollection.DOCNO_OPEN;
import static it.unimi.di.big.mg4j.document.TRECDocumentCollection.DOC_OPEN;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.WordReader;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** A factory without fields that is used to interpret the header of a
 * TREC GOV2 document. It is usually the first factory to interpret
 * a document of a {@link it.unimi.di.big.mg4j.document.TRECDocumentCollection}.
 * <p>Presently, its only r&ocircumflex;le is that of parsing the document
 * URI and setting a metadata item with key {@link it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys#URI}.
 * 
 * @author Alessio Orlandi
 * @author Luca Natali
 */
public class TRECHeaderDocumentFactory extends AbstractDocumentFactory {
	private static final long serialVersionUID = -8671564750345493607L;

	private class TRECHeaderDocument extends AbstractDocument {

		final Reference2ObjectMap<Enum<?>,Object> metadata;

		public CharSequence title() {
			CharSequence t = (CharSequence)metadata.get( MetadataKeys.TITLE );
			return ( t == null ? "Title for " + uri() : t );
		}

		public CharSequence uri() {
			return (CharSequence)metadata.get( MetadataKeys.URI );
		}

		public Object content( int fieldIndex ) throws IOException {
			throw new IllegalArgumentException();
		}

		public WordReader wordReader( int fieldIndex ) {
			throw new IllegalArgumentException();
		}

		public TRECHeaderDocument( Reference2ObjectMap<Enum<?>, Object> metadata ) {
			this.metadata = metadata;
		}

	}

	public int numberOfFields() {
		return 0;
	}

	public String fieldName( int fieldIndex ) {
		throw new IllegalArgumentException();
	}

	public int fieldIndex( String fieldName ) {
		return -1;
	}

	public FieldType fieldType( int fieldIndex ) {
		throw new IllegalArgumentException();
	}

	private byte buffer[] = new byte[ 8 * 1024 ];

	protected static boolean startsWith( byte[] a, int l, byte[] b ) {
		int len = b.length;
		if ( len > l ) return false;
		while( len-- != 0 ) if ( a[ len ] != b[ len ] ) return false;
		return true;
	}

	protected static boolean startsWithIgnoreCase( byte[] a, int l, char[] b ) {
		int len = b.length;
		if ( len > l ) return false;
		while( len-- != 0 ) if ( Character.toLowerCase( (char)a[ len ] ) != Character.toLowerCase( b[ len ] ) ) return false;
		return true;
	}


	public Document getDocument( InputStream rawContent, Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException {
		
		@SuppressWarnings("resource")
		final FastBufferedInputStream fbis = new FastBufferedInputStream( rawContent );
		int startedHeader = 0; // 0 == false, 1 == true, 2 === header started and uri collected
		boolean foundDocNo = false;
		
		int l = fbis.readLine( buffer );
		if ( l < 0 ) throw new EOFException();
		if ( ! TRECDocumentCollection.equals( buffer, l, DOC_OPEN  ) ) throw new IllegalStateException ( "Document does not start with <DOC>: " + new String( buffer, 0, l ) );
		
		while ( ( l = fbis.readLine( buffer ) ) != -1 ) {
			if ( !foundDocNo && startsWith( buffer, l, DOCNO_OPEN ) ) {
				foundDocNo = true;
				metadata.put( MetadataKeys.TITLE, new String( buffer, DOCNO_OPEN.length, l - ( DOCNO_OPEN.length + DOCNO_CLOSE.length ) ) );
			}

			switch ( startedHeader ) {
			case 0:
				if ( TRECDocumentCollection.equals( buffer, l, DOCHDR_OPEN ) ) startedHeader = 1;
				break;
			case 1:
				startedHeader = 2;
				metadata.put( MetadataKeys.URI, new String( buffer, 0, l ) );
				break;
			case 2:

			}
			if ( TRECDocumentCollection.equals( buffer, l, DOCHDR_CLOSE ) ) break; 
		}

		return new TRECHeaderDocument( metadata );
	}
	
	public DocumentFactory copy() {
		return this;
	}
}
