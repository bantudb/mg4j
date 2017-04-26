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

import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.lang.FlyweightPrototype;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/** A factory parsing and building documents of the same type.
 * 
 * <p>Each document produced by the same factory has a number of <em>fields</em>, 
 * which represent units of information that should be indexed
 * separately. The number of available fields may be recovered calling
 * {@link #numberOfFields()}, their types calling {@link #fieldType(int)},
 * and their symbolic names using {@link #fieldName(int)}.
 * 
 * <p>Factories contain the parsing and document-level breaking logic. For instance,
 * a factory for HTML documents might extract the text into a title and a body, and
 * expose them as {@link FieldType#TEXT} fields. Additionally, the last modification
 * date might be exposed as a {@link FieldType#DATE} field, and so on.
 * 
 * <strong>Warning</strong>: implementations of this class are not required
 * to be thread-safe, but they provide {@link FlyweightPrototype flyweight copies}.
 * The {@link #copy()} method is strengthened so to return a instance of this class.
 */


public interface DocumentFactory extends Serializable, FlyweightPrototype<DocumentFactory> {

	/** A field type. */
	public static enum FieldType {
		/** The most basic type: indexable text. */
		TEXT,
		/** A virtual field: an {@link ObjectList} of {@link VirtualDocumentFragment}s. */
		VIRTUAL,
		/** A long integer between -2<sup>62</sup> (inclusive) and 2<sup>62</sup> (exclusive). */
		INT,
		/** A date (experimental). */
		DATE,
	}

	/** Returns the number of fields present in the documents produced by this factory. 
	 * 
	 * @return the number of fields present in the documents produced by this factory.
	 */
	public int numberOfFields();
	
	/** Returns the symbolic name of a field.
	 * 
	 * @param field the index of a field (between 0 inclusive and {@link #numberOfFields()} exclusive}).
	 * @return the symbolic name of the <code>field</code>-th field.
	 */
	public String fieldName( int field );
	
	/** Returns the index of a field, given its symbolic name.
	 * 
	 * @param fieldName the name of a field of this factory.
	 * @return the corresponding index, or -1 if there is no field with name <code>fieldName</code>.
	 */
	public int fieldIndex( String fieldName );
	
	/** Returns the type of a field.
	 * 
	 * <p>The possible types are defined in {@link FieldType}.
	 * 
	 * @param field the index of a field (between 0 inclusive and {@link #numberOfFields()} exclusive}).
	 * @return the type of the <code>field</code>-th field.
	 */
	public FieldType fieldType( int field );
	
	/** Returns the document obtained by parsing the given byte stream.
	 * 
	 * <p>The parameter <code>metadata</code> actually replaces the lack of a simple keyword-based
	 * parameter-passing system in Java. This method might take several different type of &ldquo;suggestions&rdquo;
	 * which have been collected by the collection: typically, the document title, a URI representing
	 * the document, its MIME type, its encoding and so on. Some of this information might be
	 * set by default (as it happens, for instance, in a {@link PropertyBasedDocumentFactory}).
	 * Implementations of this method must consult the metadata provided by the collection, possibly
	 * complete them with default factory metadata, and proceed to the document construction. 
	 * 
	 * @param rawContent the raw content from which the document should be extracted; it must not be closed, as
	 * resource management is a responsibility of the {@linkplain DocumentCollection}. 
	 * @param metadata a map from enums (e.g., keys taken in {@link PropertyBasedDocumentFactory}) to various kind of objects.
	 * 
	 * @return the document obtained by parsing the given character sequence.
	 */
	public Document getDocument( InputStream rawContent, Reference2ObjectMap<Enum<?>,Object> metadata ) throws IOException;
	
	public DocumentFactory copy();
}
