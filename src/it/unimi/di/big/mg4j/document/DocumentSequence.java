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

/** A sequence of documents.
 * 
 * <p>This is the most basic class available in MG4J for representing 
 * a sequence to documents to be indexed. Its only duty is to be able to
 * return once an iterator over the documents in sequence.
 * 
 * <p>The iterator returned by {@link #iterator()} must always return the
 * same documents in the same order, given the same external conditions
 * (standard input, file system, etc.).
 * 
 * <p>Document sequences must always return documents of the same type. This
 * is usually accomplished by providing at construction time a {@link DocumentFactory}
 * that will be used to build and parse documents. Of course, it is possible to
 * create document sequences with a hardwired factory 
 * (see, e.g., {@link it.unimi.di.big.mg4j.document.ZipDocumentCollection}).
 * 
 * <p>Some sequences might require invoking {@link #filename(CharSequence)} to
 * access ancillary data. {@link AbstractDocumentSequence#load(CharSequence)} is
 * the suggest method for deserialising sequences, as it will do it for you.
 */

public interface DocumentSequence extends Closeable {

	/** Returns an iterator over the sequence of documents. 
	 * 
	 * <p><strong>Warning</strong>: this method can be safely called
	 * just <em>one</em> time. For instance, implementations based
	 * on standard input will usually throw an exception if this
	 * method is called twice. 
	 * 
	 * <p>Implementations may decide to override this restriction
	 * (in particular, if they implement {@link DocumentCollection}). Usually,
	 * however, it is not possible to obtain <em>two</em> iterators at the
	 * same time on a collection. 
	 * 
	 * @return an iterator over the sequence of documents.
	 * @see DocumentCollection
	 */
	
	public DocumentIterator iterator() throws IOException;
	
	/** Returns the factory used by this sequence.
	 * 
	 * <P>Every document sequence is based on a document factory that
	 * transforms raw bytes into a sequence of characters. The factory
	 * contains useful information such as the number of fields.
	 * 
	 * @return the factory used by this sequence.
	 */
	
	public DocumentFactory factory();
	
	/** Closes this document sequence, releasing all resources. 
	 * 
	 * <p>You should always call this method after having finished with this document sequence.
	 * Implementations are invited to call this method in a finaliser as a safety net (even better, 
	 * implement {@link it.unimi.dsi.io.SafelyCloseable}), but since there
	 * is no guarantee as to when finalisers are invoked, you should not depend on this behaviour. 
	 */
	public void close() throws IOException;
	
	/** Sets the filename of this document sequence.
	 * 
	 * <p>Several document sequences (or {@linkplain DocumentCollection collections}) are stored using Java's
	 * standard serialisation mechanism; nonetheless, they require access to files
	 * that are stored as serialised filenames inside the instance. If all pieces are in the current directory, this works as expected.
	 * However, if the sequence was specified using a complete pathname, during deserialisation it will be
	 * impossible to recover the associated files. In this case, the class expects that this method is invoked
	 * over the newly deserialised instance so that pathnames can be relativised to the given filename. Classes
	 * that need this mechanism should not fail upon deserialisation if they do not find some support file, but
	 * rather wait for the first access.
	 * 
	 * <p>In several cases, this method can be a no-op (e.g., for an {@link InputStreamDocumentSequence} or a {@link FileSetDocumentCollection}).
	 * Other implementations, such as {@link SimpleCompressedDocumentCollection} or {@link ZipDocumentCollection}, require
	 * a specific treatment. {@link AbstractDocumentSequence} implements this method as a no-op.
	 * 
	 * @param filename the filename of this document sequence.
	 */
	public void filename( final CharSequence filename ) throws IOException;
}
