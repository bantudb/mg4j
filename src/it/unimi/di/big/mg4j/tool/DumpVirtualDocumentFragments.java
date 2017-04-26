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
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;


/** Scans a document sequence and prints on standard output virtual document fragments as a document specifier (usually, a URL) TAB-separated from the associated text.
 * 
 * @author Sebastiano Vigna
 */

public class DumpVirtualDocumentFragments {
	private final static Logger LOGGER = LoggerFactory.getLogger( DumpVirtualDocumentFragments.class );

	private static final char[] WHITESPACE = new char[] { '\n', '\r', '\t' };
	private static final char[] SPACES = new char[] { ' ', ' ', ' ' };

	@SuppressWarnings({ "resource", "deprecation" })
	public static void main( final String[] arg ) throws JSAPException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, IllegalAccessException, InstantiationException, IllegalArgumentException, SecurityException {

		SimpleJSAP jsap = new SimpleJSAP( DumpVirtualDocumentFragments.class.getName(), "Scans a document sequence and prints on standard output virtual document fragments as a document specifier (usually, a URL) TAB-separated from the associated text. All whitespace in anchor text will be substituted with spaces.",
			new Parameter[] {
				new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
				new FlaggedOption( "objectSequence", new ObjectParser( DocumentSequence.class, MG4JClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-sequence", "An object specification describing a document sequence that will be used instead of stdin." ),
				new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( Scan.DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter." ),
				new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor." ),
				new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
				new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
				new UnflaggedOption( "anchorField", JSAP.STRING_PARSER, JSAP.REQUIRED, "The name of the virtual field containing anchors." ),  
		});

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		if ( jsapResult.userSpecified( "sequence" ) && jsapResult.userSpecified( "objectSequence" ) ) throw new IllegalArgumentException( "You cannot specify both a serialised and an parseable-object sequence" );
		final DocumentSequence documentSequence = jsapResult.userSpecified( "objectSequence" ) ? (DocumentSequence)jsapResult.getObject( "objectSequence" ) : Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );
		final DocumentIterator documentIterator = documentSequence.iterator();
		final int fieldIndex = documentSequence.factory().fieldIndex( jsapResult.getString( "anchorField" ) );
		if ( fieldIndex == -1 ) throw new IllegalArgumentException( "Unknown field \"" + jsapResult.getString( "anchorField" ) + "\"" );

		Document document;
		
		ProgressLogger progressLogger = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ), TimeUnit.MILLISECONDS, "documents" );
		if ( documentSequence instanceof DocumentCollection ) progressLogger.expectedUpdates = ((DocumentCollection)documentSequence).size();
		progressLogger.start( "Scanning..." );
		
		while( ( document = documentIterator.nextDocument() ) != null ) {
			@SuppressWarnings("unchecked")
			final List<VirtualDocumentFragment> l = (List<VirtualDocumentFragment>)document.content( fieldIndex );
			for( int i = 0; i < l.size(); i++ ) {
				l.get( i ).documentSpecifier().writeUTF8( System.out );
				System.out.print( '\t' );
				l.get( i ).text().replace( WHITESPACE, SPACES ).writeUTF8( System.out );
				System.out.println();
			}
			progressLogger.lightUpdate();
			document.close();
		}
		
		progressLogger.done();
		documentSequence.close();
	}
}
