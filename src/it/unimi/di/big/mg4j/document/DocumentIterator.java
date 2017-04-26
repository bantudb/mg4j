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

import java.io.Closeable;
import java.io.IOException;

/** An iterator over documents.
 * 
 * <p>This interface provide a {@link #nextDocument()}
 * method returning the next document, or <code>null</code> if
 * no more documents are available. Usually
 * you would need to {@link Document#close()} each document when you
 * are finished with it, but in the present case it is
 * guaranteed that each call to {@link DocumentIterator#nextDocument()}
 * will close the previously returned document. 
 * 
 * <p>An additional {@link #close()} method releases all resources
 * used by the iterator. Implementations are invited to be 
 * {@link it.unimi.dsi.io.SafelyCloseable safely closeable}. 
 */
public interface DocumentIterator extends Closeable {

	/** Returns the next document.
	 * 
	 * @return the next document, or <code>null</code> if there are no other documents.
	 */
	public Document nextDocument() throws IOException;
	
	/** Closes this document iterator, releasing all resources. 
	 * 
	 * <p>You should always call this method after having finished with this iterator.
	 * Implementations are invited to call this method in a finaliser as a safety net, but since there
	 * is no guarantee as to when finalisers are invoked, you should not depend on this behaviour. 
	 */
	
	public void close() throws IOException;
}
