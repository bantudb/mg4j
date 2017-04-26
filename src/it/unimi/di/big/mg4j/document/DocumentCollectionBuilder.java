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


import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.util.List;

/** An interface for classes that can build collections during the indexing process.
 * 
 * <p>A builder is usually based on a <em>{@linkplain #basename() basename}</em>.
 * Many different collections can be built using the same builder, using {@link #open(CharSequence)}
 * to specify a suffix that will be added to the basename. Creating several collections
 * is a simple way to make collection construction scalable: for instance, {@link Scan} creates
 * several collections, one per batch, and then puts them together using a {@link ConcatenatedDocumentCollection}.
 * 
 * <p>After creating an instance of this class and after having opened a new collection, it is possible to add incrementally
 * new documents. Each document must be started with {@link #startDocument(CharSequence, CharSequence)}
 * and ended with {@link #endDocument()}; inside each document, each non-text field must be written by passing
 * an object to {@link #nonTextField(Object)}, whereas each text field must be
 * started with {@link #startTextField()} and ended with {@link #endTextField()}: inbetween, a call
 * to {@link #add(MutableString, MutableString)} must be made for each word/nonword pair retrieved
 * from the original collection. At the end, {@link #close()} returns a {@link it.unimi.di.big.mg4j.document.ZipDocumentCollection}
 * that must be serialised.
 * 
 * <p>Several collections (e.g., {@link SimpleCompressedDocumentCollection}, {@link ZipDocumentCollection}) can be
 * <em>exact</em> or <em>approximated</em>: in the latter case, nonwords are not recorded to decrease space usage.
 * 
 */

public interface DocumentCollectionBuilder {

	/** Returns the basename of this builder.
	 * 
	 * @return the basename
	 */
	String basename();
	
	/** Opens a new collection. 
	 *
	 * @param suffix a suffix that will be added to the basename provided at construction time.
	 */
	void open( CharSequence suffix ) throws IOException;
	
	/** Starts a document entry.
	 * 
	 * @param title the document title (usually, the result of {@link Document#title()}).
	 * @param uri the document uri (usually, the result of {@link Document#uri()}).
	 */

	void startDocument( final CharSequence title, final CharSequence uri ) throws IOException;

	/** Ends a document entry. 
	 */

	void endDocument() throws IOException;

	/** Starts a new text field.
	 */

	void startTextField();

	/** Ends a new text field. */
	void endTextField() throws IOException;

	/** Adds a non-text field.
	 * 
	 * @param o the content of the non-text field.
	 */
	void nonTextField( final Object o ) throws IOException;

	/** Adds a virtual field.
	 * 
	 *  @param fragments the virtual fragments to be added.
	 * 
	 */
	void virtualField( final List<VirtualDocumentFragment> fragments ) throws IOException;


	/** Adds a word and a nonword to the current text field, provided that a text field has {@linkplain #startTextField() started} but not yet {@linkplain #endTextField() ended};
	 *  otherwise, doesn't do anything.
	 *
	 * <p>Usually, <code>word</code> e <code>nonWord</code> are just the result of a call
	 * to {@link WordReader#next(MutableString, MutableString)}.
	 *  
	 * @param word a word.
	 * @param nonWord a nonword.
	 * */
	void add( final MutableString word, final MutableString nonWord ) throws IOException;

	/** Terminates the contruction of the collection. */

	void close() throws IOException;
}
