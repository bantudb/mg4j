package it.unimi.di.big.mg4j.tool;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Paolo Boldi and Sebastiano Vigna 
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


import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;

import java.io.Serializable;


/** A resolver for virtual documents.
 * 
 * <p>Fields of {@linkplain it.unimi.di.big.mg4j.document.DocumentFactory.FieldType#VIRTUAL virtual type} return
 * a list of {@linkplain VirtualDocumentFragment virtual document fragments}
 * containing a document specification (e.g., its URI) and the virtual text associated with the document. Since there are
 * many ways of defining the virtual document, {@link it.unimi.di.big.mg4j.tool.Scan} requires
 * a virtual-document resolver for each virtual field: the resolver takes in the string defining a document,
 * and returns a document number. See {@link it.unimi.di.big.mg4j.tool.URLMPHVirtualDocumentResolver} for a
 * natural example. 
 * 
 */

public interface VirtualDocumentResolver extends Serializable {
	/** Sets the context document. All successive calls to {@link #resolve(CharSequence)} will
	 * assume the virtual-document specification was found in <code>document</code>.
	 * 
	 * @param document the context document.
	 */
	public void context( Document document );

	/** Resolves a virtual document specification.
	 * 
	 * <p>Note that the resolution process is carried out in the context of the last document
	 * passed to {@link #context(Document)} (e.g., for relative URI resolution). If {@link #context(Document)}
	 * was never called, the behaviour is undefined.
	 * 
	 * @param virtualDocumentSpec the virtual document specification.
	 * @return the document <code>virtualDocumentSpec</code> refers to, or -1 if the specification could not be resolved.
	 */
	public long resolve( CharSequence virtualDocumentSpec );

	/** Returns the number of documents handled by this resolver, if it is known. A call
	 * to {@link #resolve(CharSequence)} will always return a number
	 * smaller than the one returned by this method.
	 * 
	 * @return the number of documents handled by this resolver.
	 */
	
	public long numberOfDocuments();
}
