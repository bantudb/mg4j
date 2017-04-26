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

import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.di.big.mg4j.util.MimeTypeResolver;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.NullInputStream;
import it.unimi.dsi.lang.MutableString;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** A {@link it.unimi.di.big.mg4j.document.DocumentCollection} corresponding to
 *  a given set of files.
 * 
 * <P>This class provides a main method with a flexible syntax that serialises
 * into a document collection a list of files given on the command line or
 * piped into standard input. Optionally, you can provide a parallel list of URIs
 * that will be associated with each file.
 * 
 * <p><strong>Warning</strong>: the number of file is limited by {@link Integer#MAX_VALUE}.
 */
public class FileSetDocumentCollection extends AbstractDocumentCollection implements Serializable {

	private static final long serialVersionUID = 0L;
	
	/** The files in this collection. */
	private final String[] file;
	/** URIs for each file in this collection, or <code>null</code>, in which case the filename will be used as URI. */
	private final String[] uri;
	/** The factory to be used by this collection. */
	private final DocumentFactory factory;
	/** The last returned file input stream. */
	private InputStream last = NullInputStream.getInstance();

	/** Builds a document collection corresponding to a given set of files specified as an array.
	 * 
	 *  <p><strong>Beware.</strong> This class is not guaranteed to work if files are
	 *  deleted or modified after creation!
	 * 
	 * @param file an array containing the files that will be contained in the collection.
	 * @param factory the factory that will be used to create documents.
	 */
	public FileSetDocumentCollection( final String[] file, final DocumentFactory factory ) {
		this( file, null, factory );
	}
	
	/** Builds a document collection corresponding to a given set of files specified as an array and
	 * a parallel array of URIs, one for each file.
	 * 
	 *  <p><strong>Beware.</strong> This class is not guaranteed to work if files are
	 *  deleted or modified after creation!
	 * 
	 * @param file an array containing the files that will be contained in the collection.
	 * @param uri an array, parallel to <code>file</code>, containing URIs to be associated with each element of <code>file</code>.
	 * @param factory the factory that will be used to create documents.
	 */
	public FileSetDocumentCollection( final String[] file, final String uri[], final DocumentFactory factory ) {
		this.file = file;
		this.uri = uri;
		this.factory = factory;
	}

	public DocumentFactory factory() {
		return factory;
	}
	
	public long size() {
		return file.length;
	}

	public Reference2ObjectMap<Enum<?>,Object> metadata( final long index ) {
		ensureDocumentIndex( index );
		final Reference2ObjectArrayMap<Enum<?>, Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 2 );
		metadata.put( MetadataKeys.TITLE, file[ (int)index ] );
		if ( uri != null ) metadata.put( MetadataKeys.URI, uri[ (int)index ] );
		else metadata.put( MetadataKeys.URI, new File( file[ (int)index ] ).toURI().toString() );
		metadata.put( MetadataKeys.MIMETYPE, MimeTypeResolver.getContentType( file[ (int)index ] ) );
		return metadata;
	}

	public Document document( final long index ) throws IOException {
		return factory.getDocument( stream( index ), metadata( index ) );
	}
	
	public InputStream stream( final long index ) throws IOException {
		ensureDocumentIndex( index );
		last.close();
		return last = new FileInputStream( file[ (int)index ] );
	}
	
	public FileSetDocumentCollection copy() {
		return new FileSetDocumentCollection( file, uri, factory.copy() ); 
	}
	
	public void close() throws IOException {
		last.close();
		super.close();
	}
	
	public static void main( final String[] arg ) throws IOException, JSAPException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		SimpleJSAP jsap = new SimpleJSAP( FileSetDocumentCollection.class.getName(), "Saves a serialised document collection based on a set of files.",
				new Parameter[] {
					new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor." ),
					new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
					new FlaggedOption( "uris", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'u', "uris", "A file containing a list of URIs in ASCII encoding, one per line, that will be associated with each file" ),
					new UnflaggedOption( "collection", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename for the serialised collection." ),
					new UnflaggedOption( "file", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.GREEDY, "A list of files that will be indexed. If missing, a list of files will be read from standard input." )
				}
		);
		

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		String uri[] = null;
		if ( jsapResult.getString( "uris" ) != null ) {
			Collection<MutableString> lines = new FileLinesCollection( jsapResult.getString( "uris" ), "ASCII" ).allLines();
			uri = new String[ lines.size() ];
			int i = 0;
			for( Object l: lines ) uri[ i++ ] = l.toString();
		}
		
		final DocumentFactory factory = PropertyBasedDocumentFactory.getInstance( jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ) ); 
		
		String[] file = (String[])jsapResult.getObjectArray( "file", new String[ 0 ] );
		if ( file.length == 0 ) {
			final ObjectArrayList<String> files = new ObjectArrayList<String>();
			BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( System.in ) );
			String s;
			while( ( s = bufferedReader.readLine() ) != null ) files.add( s );
			file = files.toArray( new String[ 0 ] );
		}
		
		if ( file.length == 0 ) System.err.println( "WARNING: empty file set." );
		if ( uri != null && file.length != uri.length ) throw new IllegalArgumentException( "The number of files (" + file.length + ") and the number of URIs (" + uri.length + ") differ" );
		BinIO.storeObject( new FileSetDocumentCollection( file, uri, factory ), jsapResult.getString( "collection" ) );
	}
}
