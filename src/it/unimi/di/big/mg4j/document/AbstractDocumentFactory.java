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

import it.unimi.dsi.lang.MutableString;

/** An abstract implementation of a factory, providing a protected method to check
 *  for field indices.
 */

public abstract class AbstractDocumentFactory implements DocumentFactory {
	private static final long serialVersionUID = 1L;

	/** Checks that the index is correct (between 0, inclusive, and {@link it.unimi.di.big.mg4j.document.DocumentFactory#numberOfFields()},
	 *  exclusive), and throws an {@link IndexOutOfBoundsException} if the index is not correct.
	 * 
	 * @param index the index to be checked.
	 */
	protected void ensureFieldIndex( final int index ) {
		if ( index < 0 || index >= numberOfFields() )
			throw new IndexOutOfBoundsException( Integer.toString( index ) ); 
	}

	public String toString() {
		final MutableString res = new MutableString();
		res.append( getClass().getName() );
		res.append( '[' );
		for ( int field = 0; field < numberOfFields(); field++ ) 
			res.append( ' ' ).append( fieldName( field ) ).append( ':' ).append( fieldType( field ).name() );
		return res.append( " ]" ).toString();
	}
}
