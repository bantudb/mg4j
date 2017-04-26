package it.unimi.di.big.mg4j.tool;

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

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;


/** Scans a document sequence and prints on standard output the corresponding URIs.
 * 
 * <p>This tool is a necessary intermediate step for the construction of an index with
 * virtual fields.
 * 
 * @author Sebastiano Vigna
 * @since 1.1
 */

public class ScanMetadata {
	private final static Logger LOGGER = LoggerFactory.getLogger( ScanMetadata.class );

	protected static final char[] LINE_TERMINATORS = new char[] { '\n', '\r' };
	protected static final char[] SPACES = new char[] { ' ', ' ' };
	
	public static void main( final String[] arg ) throws JSAPException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, IllegalAccessException, InstantiationException, IllegalArgumentException, SecurityException {

		SimpleJSAP jsap = new SimpleJSAP( ScanMetadata.class.getName(), "Scans and prints to standard output metadata of a collection. All line terminators in the metadata will be substituted with spaces.",
			new Parameter[] {
				new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
				new FlaggedOption( "objectSequence", new ObjectParser( DocumentSequence.class, MG4JClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-sequence", "An object specification describing a document sequence that will be used instead of stdin." ),
				new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( Scan.DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter." ),
				new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor." ),
				new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
				new FlaggedOption( "renumber", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "renumber", "The filename of a document renumbering." ),
				new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
				new FlaggedOption( "titles", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "titles", "The resulting document titles." ),
				new FlaggedOption( "uris", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'u', "uris", "The resulting document URIs." ),
		});

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		if ( ! jsapResult.userSpecified( "uris" ) && ! jsapResult.userSpecified( "titles" ) ) throw new IllegalArgumentException( "You specify either a title or a URI output file" );

		if ( jsapResult.userSpecified( "sequence" ) && jsapResult.userSpecified( "objectSequence" ) ) throw new IllegalArgumentException( "You cannot specify both a serialised and an parseable-object sequence" );
		
		final DocumentSequence documentSequence = jsapResult.userSpecified( "objectSequence" ) ? (DocumentSequence)jsapResult.getObject( "objectSequence" ) : Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );

		final DocumentIterator documentIterator = documentSequence.iterator();

		Document document;
		PrintStream uriStream = null;
		PrintStream titleStream = null;
		
		if ( jsapResult.userSpecified( "uris" ) ) uriStream = new PrintStream( jsapResult.getString( "uris" ), "UTF-8" );
		if ( jsapResult.userSpecified( "titles" ) ) titleStream = new PrintStream( jsapResult.getString( "titles" ), "UTF-8" );
		
		MutableString s = new MutableString();

		ProgressLogger progressLogger = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ), TimeUnit.MILLISECONDS, "documents" );
		if ( documentSequence instanceof DocumentCollection ) progressLogger.expectedUpdates = ((DocumentCollection)documentSequence).size();
		progressLogger.start( "Scanning..." );
		
		while( ( document = documentIterator.nextDocument() ) != null ) {
			if ( uriStream != null ) {
				if ( document.uri() != null ) {
					s.replace( document.uri() );
					s.replace( LINE_TERMINATORS, SPACES );
					uriStream.print( s );
				}
				uriStream.println();
			}
			if ( titleStream != null ) {
				if ( document.title() != null ) {
					s.replace( document.title() );
					s.replace( LINE_TERMINATORS, SPACES );
					titleStream.print( s );
				}
				titleStream.println();
			}
			progressLogger.lightUpdate();
			document.close();
		}
		
		progressLogger.done();
		documentSequence.close();
		if ( uriStream != null ) uriStream.close();
		if ( titleStream != null ) titleStream.close();
	}
}
