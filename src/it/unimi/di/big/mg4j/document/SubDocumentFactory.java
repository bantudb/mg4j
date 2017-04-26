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


import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.WordReader;

import java.io.IOException;
import java.io.InputStream;

/** A factory that exposes a subset of the fields a given factory.
 * 
 * @author Sebastiano Vigna
 */

public class SubDocumentFactory extends AbstractDocumentFactory {
	private static final long serialVersionUID = 1L;
	/** The underlying document factory. */
	private final DocumentFactory underlyingFactory;
	/** The subfields of {@link #underlyingFactory} that will be exposed. */
	private final int[] visibleField;
	/** A map from the original field index to the new index; returns -1 for non-mapped fields. */
	private final Int2IntOpenHashMap field2Pos;

	/** Creates a new subfactory.
	 * 
	 * @param underlyingFactory the underlying document collection.
	 * @param visibleField the fields of the factory that will be visible in the
	 * new subfactory, in increasing order.
	 */
	public SubDocumentFactory( DocumentFactory underlyingFactory, int... visibleField ) {
		this.underlyingFactory = underlyingFactory;
		this.visibleField = visibleField;
		for( int i = visibleField.length; i-- > 1; ) if ( visibleField[ i - 1 ] >= visibleField[ i ] ) throw new IllegalArgumentException( "Fields must be provided in increasing order" );
		// TODO: turn into an ArrayMap?
		field2Pos = new Int2IntOpenHashMap( visibleField.length, Hash.VERY_FAST_LOAD_FACTOR );
		for( int i = visibleField.length; i-- != 0; ) field2Pos.put( visibleField[ i ], i );
		field2Pos.defaultReturnValue( -1 );
	}

	
	public DocumentFactory copy() {
		return new SubDocumentFactory( underlyingFactory.copy(), visibleField );
	}

	
	public int fieldIndex( String fieldName ) {
		return field2Pos.get( underlyingFactory.fieldIndex( fieldName ) );
	}

	
	public String fieldName( int field ) {
		ensureFieldIndex( field );
		return underlyingFactory.fieldName( visibleField[ field ] );
	}

	
	public FieldType fieldType( int field ) {
		ensureFieldIndex( field );
		return underlyingFactory.fieldType( visibleField[ field ] );
	}

	
	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>, Object> metadata ) throws IOException {
		return new AbstractDocument() {
			final Document underlyingDocument = underlyingFactory.getDocument( rawContent, metadata );

			public void close() throws IOException {
				underlyingDocument.close();
			}

			public Object content( int field ) throws IOException {
				return underlyingDocument.content( visibleField[ field ] );
			}

			public CharSequence title() {
				return underlyingDocument.title();
			}

			public CharSequence uri() {
				return underlyingDocument.uri();
			}

			public WordReader wordReader( int field ) {
				return underlyingDocument.wordReader( visibleField[ field ] );
			}
		};
	}

	
	public int numberOfFields() {
		return visibleField.length;
	}
}
