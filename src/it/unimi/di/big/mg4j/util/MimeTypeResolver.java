package it.unimi.di.big.mg4j.util;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
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

import java.io.File;
import java.io.IOException;

import javax.activation.MimetypesFileTypeMap;

/** A thin wrapper around a singleton instance of {@link javax.activation.MimetypesFileTypeMap}
 * that tries to load <samp>/etc/mime.types</samp> into the map.
 */

public class MimeTypeResolver {
	/** A MIME type file map, stuffed with some basic types. */
	private final static String UNIX_MIME_TYPES_FILENAME = "/etc/mime.types";

	private final static MimetypesFileTypeMap MIME_TYPES_FILE_TYPE_MAP;

	private MimeTypeResolver() {}
	
	static {
		try {
			MIME_TYPES_FILE_TYPE_MAP = ( new File( UNIX_MIME_TYPES_FILENAME ) ).exists() 
			? new MimetypesFileTypeMap( UNIX_MIME_TYPES_FILENAME )
			: new MimetypesFileTypeMap();
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}
	
	public static String getContentType( final File file ) {
		return MIME_TYPES_FILE_TYPE_MAP.getContentType( file );
	}

	public static String getContentType( final CharSequence name ) {
		return MIME_TYPES_FILE_TYPE_MAP.getContentType( name.toString() );
	}
}
