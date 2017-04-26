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

import org.apache.tika.metadata.Metadata;

/** The set of all Tika metadata represented as a single field inside MG4J. 
 * 
 * <P>When using an {@link AutoDetectDocumentFactory} or any other factory in which
 * metadata fields are user-definable or otherwise variable, it is impossible to
 * provide a static listing of all available fields, as they depend on the
 * actual factory used to parse the document. In this case, an instance of 
 * this class is used to return some useful data to the caller.
 */

public class GreedyTikaField extends TikaField {
	private static final long serialVersionUID = 1L;
	public static String NAME = "meta";

	public GreedyTikaField( String tikaName ) {
		super( tikaName );
	}

	@Override
	public String contentFromMetadata( Metadata metadata ) {
		return metadata.toString();
	}
}
