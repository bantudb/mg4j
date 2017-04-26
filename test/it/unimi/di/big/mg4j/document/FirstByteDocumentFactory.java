package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.NullReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.util.Properties;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;


/** A factory (with a single text field named <samp>text</samp>) that reads the first byte
 * in the input stream and returns it as a character.
 */

@SuppressWarnings("unused")
public class FirstByteDocumentFactory extends PropertyBasedDocumentFactory {
	private static final long serialVersionUID = 1L;

	public FirstByteDocumentFactory() {}
	public FirstByteDocumentFactory( final Reference2ObjectMap<Enum<?>,Object> defaultMetadata ) {}
	public FirstByteDocumentFactory( final Properties properties ) {}
	public FirstByteDocumentFactory( final String[] property ) {}
		
	public FirstByteDocumentFactory copy() {
		return this;
	}
	
	public int numberOfFields() {
		return 1;
	}
	
	public String fieldName( final int field ) {
		if ( field != 0 ) throw new IndexOutOfBoundsException();
		return "text";
	}
	
	public int fieldIndex( final String fieldName ) {
		return fieldName.equals( "text" ) ? 0: -1;
	}

	public FieldType fieldType( final int field ) {
		return FieldType.TEXT;
	}
	
	public Document getDocument( final InputStream rawContent, final Reference2ObjectMap<Enum<?>,Object> metadata ) {
		return new AbstractDocument() {
			
			public CharSequence title() {
				return "dummy title";
			}
			
			public String toString() {
				return title().toString();
			}

			public CharSequence uri() {
				return "dummy://";
			}

			public Object content( final int field ) throws IOException {
				if ( field != 0 ) throw new IndexOutOfBoundsException();
				int firstByte = rawContent.read();
				if ( firstByte == -1 ) return NullReader.getInstance();
				return new CharArrayReader( new char[] { (char)firstByte } ); 
			}

			public WordReader wordReader( final int field ) {
				return new FastBufferedReader(); 
			}
		};
	}
}
