package it.unimi.di.big.mg4j.document;

/*		 
 * Copyright (C) 2005-2016 Massimo Santini 
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
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.io.MultipleInputStream;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** A {@link it.unimi.di.big.mg4j.document.DocumentCollection} corresponding to a given set of records in a comma separated file. */
public class CSVDocumentCollection extends AbstractDocumentSequence implements Serializable {

	private static final long serialVersionUID = 1L;
	/** The CSV filename. */
	private final String fileName;
	/** The field separator. */
	private final String separator;
	/** The column names. */
	private final String[] column;
	/** If nonnegative, the index of the colulmn to be used as a title. */
	private final int titleColumn;
	/** The factory to be used by this collection. */
	private final DocumentFactory factory;
	
	private transient BufferedReader reader;
	private transient int readLines;
	
	public CSVDocumentCollection( final String fileName, final String separator, final String[] column, final int titleColumn, final DocumentFactory factory ) throws FileNotFoundException {
		this.fileName = fileName;
		this.separator = separator;
		this.column = column;
		if ( titleColumn >= column.length ) throw new IllegalArgumentException( "The title column (" + titleColumn + ") is larger than or equal to the number of columns (" + column.length + ")" );
		this.titleColumn = titleColumn;
		this.factory = factory;
		// TODO: this won't work--the charset is different when encoding and when decoding.
		reader = new BufferedReader( new InputStreamReader( new FileInputStream( fileName ) ) );
		readLines = -1;
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		reader = new BufferedReader( new InputStreamReader( new FileInputStream( fileName ) ) );
		readLines = -1;
	}
	
	public DocumentIterator iterator() {
		return new AbstractDocumentIterator() {
			final Reference2ObjectArrayMap<Enum<?>, Object> metadata = new Reference2ObjectArrayMap<Enum<?>,Object>( 2 );

			public Document nextDocument() throws IOException {
				String line = reader.readLine();
				if ( line == null ) return null;
				readLines++;				
				String[] field = line.split( separator, -1 );
				if ( field.length != column.length ) throw new IOException( "Line " + readLines + " has less (" + field.length + ") fields than the number of columns (" + column.length + ")." );
				InputStream[] a = new InputStream[ column.length ];
				// TODO: which encoding for getBytes()?
				for( int i = 0; i < column.length; i++ ) a[ i ] = new ByteArrayInputStream( field[ i ].getBytes() );
				String title = titleColumn >= 0 ? field[ titleColumn ] : Integer.toString( readLines );
				metadata.put( MetadataKeys.TITLE, title );
				metadata.put( MetadataKeys.URI, Integer.toString( readLines ) );
				return factory.getDocument( MultipleInputStream.getStream( a ), metadata );
			}
		};
	}

	public DocumentFactory factory() {
		return factory;
	}

	public void close() throws IOException {
		super.close();
		reader.close();
	}
	
	public static void main( final String[] arg ) throws JSAPException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, InstantiationException {
		SimpleJSAP jsap = new SimpleJSAP( JdbcDocumentCollection.class.getName(), "Saves a serialised document collection based on a set of database rows.",
				new Parameter[] {
					new FlaggedOption( "separator", JSAP.STRING_PARSER, ",", JSAP.NOT_REQUIRED, 's', "separator", "The regexp used to split lines into fields." ),
					new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor." ),
					new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
					new FlaggedOption( "titleColumn", JSAP.INTEGER_PARSER, "-1", JSAP.NOT_REQUIRED, 't', "title-column", "The index of the column to be used as a title (starting from 0)." ),
					new UnflaggedOption( "collection", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename for the serialised collection." ),
					new UnflaggedOption( "fileName", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename of the source CSV file." ),
					new UnflaggedOption( "column", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "Columns names that will be indexed." ),
				}
		);
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		final int titleColumn = jsapResult.getInt( "titleColumn" );
		final String collection = jsapResult.getString( "collection" );
		final String fileName = jsapResult.getString( "fileName" );
		final String separator = jsapResult.getString( "separator" ).equals( "\\t" ) ? "\t" : jsapResult.getString( "separator" );
		final String[] column = jsapResult.getStringArray( "column" );
		
		final DocumentFactory[] factory = new DocumentFactory[ column.length ];
		for( int i = 0; i < factory.length; i++ )
			factory[ i ] = PropertyBasedDocumentFactory.getInstance( jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ) );
		
		BinIO.storeObject( new CSVDocumentCollection( fileName, separator, column, titleColumn, CompositeDocumentFactory.getFactory( factory, column ) ), collection ); 
	}

}
