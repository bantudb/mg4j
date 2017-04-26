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

import it.unimi.dsi.io.WordReader;

import java.io.Closeable;
import java.io.IOException;

/** An indexable document.
 * 
 * <p>Instance of this class represent a single document. Documents provide access to possibly
 * several <em>fields</em>, which represent units of information that should be indexed
 * separately.
 *
 * <p>Each field is accessible by a call to {@link #content(int)}. Note, however, that unless specified
 * otherwise <strong>field content must be accessed in increasing order</strong>. You can skip some field,
 * but the contract of this class does not require that you can access fields in random order (although implementations
 * may provide this feature). Moreover, the data provided by a call to
 * {@link #content(int)} (e.g., a {@link java.io.Reader} 
 * for {@link it.unimi.di.big.mg4j.document.DocumentFactory.FieldType#TEXT TEXT} fields) may become invalid
 * at the next call (similarly to the behaviour of {@link it.unimi.di.big.mg4j.document.DocumentCollection#document(long)}).
 * The same holds for {@link #wordReader(int)}. 
 *
 * <p>After obtaining a document, it is your responsibility to {@linkplain java.io.Closeable#close() close} it.
 *  
 * <p>It is advisable, although not strictly required, that documents have
 * a <code>toString()</code> equal to their title.
 * 
 */

public interface Document extends Closeable {
	
	/** The title of this document. 
	 * 
	 * @return the title to be used to refer to this document.
	 */
	public CharSequence title();
	
	/** A URI that is associated with this document. 
	 * 
	 * @return the URI associated with this document, or <code>null</code>.
	 */
	public CharSequence uri();
	
	/** Returns the content of the given field.
	 * 
	 * @param field the field index.
	 * @return the field content; the actual type depends on the field type, as specified by the {@link DocumentFactory} that
	 * built this document. For example, the returned object is going to be a {@link java.io.Reader} if the field type is
	 * {@link it.unimi.di.big.mg4j.document.DocumentFactory.FieldType#TEXT}.
	 */
	public Object content( int field ) throws IOException;
	
	/** Returns a word reader for the given {@link it.unimi.di.big.mg4j.document.DocumentFactory.FieldType#TEXT} field.
	 * 
	 * @param field the field index.
	 * @return a word reader object that should be used to break the given field.
	 */
	public WordReader wordReader( int field );
	
	/** Closes this document, releasing all resources. 
	 * 
	 * <p>You should always call this method after manipulating a document. Implementations
	 * are invited to call this method in a finaliser as a safety net (even better, 
	 * implement {@link it.unimi.dsi.io.SafelyCloseable}), but since there
	 * is no guarantee as to when finalisers are invoked, you should not depend on this behaviour. 
	 */
	public void close() throws IOException;
}
