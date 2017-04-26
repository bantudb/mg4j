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

import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.lang.FlyweightPrototype;

import java.io.IOException;
import java.io.InputStream;

/** A collection of documents.
 * 
 * <p>Classes implementing this interface have additional responsibilities
 * w.r.t. {@link it.unimi.di.big.mg4j.document.DocumentSequence} in that they
 * must provide random access to the documents, and guarantee the possibility
 * of multiple calls to {@link it.unimi.di.big.mg4j.document.DocumentSequence#iterator()}.
 * 
 * <p>Note, however, that the objects returned by 
 * {@link it.unimi.di.big.mg4j.document.DocumentSequence#iterator() iterator()},
 * {@link #stream(long)} and {@link #document(long)} are, unless explicitly stated otherwise,
 * <strong>mutually exclusive</strong>. They share a single resource managed by the
 * collection (and disposed by a call to {@link java.io.Closeable#close() close()}), so each time
 * a stream or a document are returned by some method, the ones previously returned are no longer
 * valid, and access to their methods will cause unpredictable behaviour. If you need many
 * documents, you can {@linkplain FlyweightPrototype#copy() obtain a flyweight copy}
 * of the collection. 
 * 
 * <p><strong>Warning</strong>: implementations of this class are not required
 * to be thread-safe, but they provide {@linkplain FlyweightPrototype flyweight copies}.
 * The {@link #copy()} method is strengthened so to return a instance of this class.
 */

public interface DocumentCollection extends DocumentSequence, FlyweightPrototype<DocumentCollection> {
	/** The default extension for a serialised collection (including the dot). */
	public final static String DEFAULT_EXTENSION = ".collection";
	
	/** Returns the number of documents in this collection.
	 * 
	 * @return the number of documents in this collection.
	 */
	public long size();
	
	/** Returns the document given its index.
	 *
	 * @param index an index between 0 (inclusive) and {@link #size()} (exclusive).
	 * @return the <code>index</code>-th document.
	 */
	public Document document( long index ) throws IOException;

	/** Returns an input stream for the raw content of a document.
	 * 
	 * @param index an index between 0 (inclusive) and {@link #size()} (exclusive).
	 * @return the raw content of the document as an input stream.
	 */
	public InputStream stream( long index ) throws IOException;

	/** Returns the metadata map for a document.
	 * 
	 * @param index an index between 0 (inclusive) and {@link #size()} (exclusive).
	 * @return the metadata map for the document.
	 */
	public Reference2ObjectMap<Enum<?>,Object> metadata( long index ) throws IOException;
	
	public DocumentCollection copy();
}
