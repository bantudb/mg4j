package  it.unimi.di.big.mg4j.document.tika;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2011-2016 Paolo Boldi and Sebastiano Vigna  
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

import it.unimi.di.big.mg4j.document.DocumentFactory;

import java.io.Serializable;

import org.apache.tika.metadata.Metadata;

/** A Tika field represented inside MG4J. */

public class TikaField implements Serializable {
	private static final long serialVersionUID = 1L;

	/** The MG4J name of the field. */
	private final String mg4jName;

	/** The Tika name of the field. It is <code>null</code> for the Tika content. */
	private final String tikaName;

	/** Creates a new Tika field corresponding to the Tika content: its Tika name is <code>null</code> and its MG4J name <samp>text</samp>. */
	public TikaField() {
		this.mg4jName = "text";
		// This happens for the Tika content.
		this.tikaName = null;
	}

	/** Creates a new Tika field with given Tika name and the same MG4J name.
	 * 
	 * @param tikaName the Tika name of the field, which will be used also as MG4J name.
	 */
	public TikaField( String tikaName ) {
		this.mg4jName = tikaName;
		this.tikaName = tikaName;
	}

	/** Creates a new Tika field with given Tika name and given MG4J name.
	 * 
	 * @param mg4jName the MG4J name of the field.
	 * @param tikaName the Tika name of the field.
	 */
	public TikaField( String mg4jName, String tikaName ) {
		this.mg4jName = mg4jName;
		this.tikaName = tikaName;
	}

	/** The MG4J name of this field.
	 * 
	 * @return the MG4J name.
	 */
	public String mg4jName() {
		return mg4jName;
	}

	/** The Tika name of this field (<code>null</code> for the Tika content).
	 * 
	 * @return the Tika name.
	 */
	public String tikaName() {
		return tikaName;
	}

	/** Gets the content of this Tika field from the given metadata. 
	 * 
	 * @param metadata the metadata.
	 * @return the content of this Tika field.
	 */
	public String contentFromMetadata( Metadata metadata ) {
		return metadata.get( tikaName() );
	}

	/** The type of this field (currently only {@link it.unimi.di.big.mg4j.document.DocumentFactory.FieldType#TEXT} is supported).
	 * 
	 * @return the type of this field.
	 */
	public DocumentFactory.FieldType getType() {
		return DocumentFactory.FieldType.TEXT;
	}

	/** Returns <code>true</code> if this field represents the Tika content.
	 * 
	 * @return <code>true</code> iff this field is the Tika content.
	 */
	public boolean isBody() {
		return tikaName == null;
	}
}
