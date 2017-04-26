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

import it.unimi.di.big.mg4j.document.DocumentFactory.FieldType;
import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** A builder for {@linkplain ZipDocumentCollection zipped document collections}. */
public class ZipDocumentCollectionBuilder implements DocumentCollectionBuilder {
	private static final Logger LOGGER = LoggerFactory.getLogger( ZipDocumentCollectionBuilder.class );

	private static final boolean DEBUG = false;
	/** The basename of the collection. */
	private final String basename;
	/** The basename of the collection plus the current suffix.. */
	private String basenameSuffix;
	/** The output stream of the zip file. */
	private ZipOutputStream zipOut;
	/** The a wrapper around the output stream of the zip file. */
	private DataOutputStream zipDataOutputStream;
	/** The number of documents written so far. */
	private int numberOfDocuments;
	/** True iff also non-words should be reproduced. */
	private final boolean exact;
	/** The factory of the base document sequence. */
	private final DocumentFactory factory;
	/** Whether a text field has started but not yet ended. */
	private boolean inTextField;

	/** Creates a new zipped collection builder.
	 * 
	 * @param factory the factory of the base document sequence.
	 * @param exact true iff also non-words should be preserved.
	 */
	public ZipDocumentCollectionBuilder( final String basename, final DocumentFactory factory, final boolean exact ) {
		this.basename = basename;
		this.factory = factory;
		this.exact = exact;
	}

	public void open( CharSequence suffix ) throws FileNotFoundException {
		basenameSuffix = basename + suffix;
		zipDataOutputStream = new DataOutputStream( zipOut = new ZipOutputStream( new FileOutputStream( basenameSuffix + ZipDocumentCollection.ZIP_EXTENSION ) ) );
		numberOfDocuments = 0;
	}
	
	public String basename() {
		return basename;
	}
	
	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#startDocument(java.lang.CharSequence, java.lang.CharSequence)
	 */
	
	public void startDocument( final CharSequence title, final CharSequence uri ) throws IOException {
		final ZipEntry currEntry = new ZipEntry( Integer.toString( numberOfDocuments ) );
		currEntry.setComment( title.toString() );
		zipOut.putNextEntry( currEntry );
		new MutableString( uri != null ? uri : "" ).writeSelfDelimUTF8( zipOut );
		
	}

	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#endDocument()
	 */
	
	public void endDocument() throws IOException {
		zipOut.closeEntry();
		numberOfDocuments++;
	}

	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#startTextField()
	 */
	
	public void startTextField() {
		inTextField = true;
	}

	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#nonTextField(java.lang.Object)
	 */
	public void nonTextField( final Object o ) throws IOException {
		if ( DEBUG ) LOGGER.debug( "Going to write non-text field " + o + " of class " + o.getClass() + " for document #" + numberOfDocuments );
		ObjectOutputStream oos = new ObjectOutputStream( zipOut );
		oos.writeObject( o );
		oos.flush();
	}
	
	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#virtualField(it.unimi.dsi.fastutil.objects.ObjectList)
	 */
	public void virtualField( final List<VirtualDocumentFragment> fragments ) throws IOException {
		if ( DEBUG ) LOGGER.debug( "Going to write virtual field " + fragments + " for document #" + numberOfDocuments );
		zipDataOutputStream.writeInt( fragments.size() );
		for ( VirtualDocumentFragment fragment: fragments ) {
			fragment.documentSpecifier().writeSelfDelimUTF8( zipOut );
			fragment.text().writeSelfDelimUTF8( zipOut );
		}
	}
	
	//This method can only be called if {@link #inTextField} is <code>true</code>, otherwise it will throw an {@link IllegalStateException}.
	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#endTextField()
	 */
	public void endTextField() throws IOException {
		// Writing a 0 is like writing an empty string.
		if ( ! inTextField ) throw new IllegalStateException();
		inTextField = false;
		zipOut.write( 0 );
		if ( exact ) zipOut.write( 0 );
	}

	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#add(it.unimi.dsi.lang.MutableString, it.unimi.dsi.lang.MutableString)
	 */	
	public void add( final MutableString word, final MutableString nonWord ) throws IOException {
		if ( ! inTextField ) return;
		if ( DEBUG ) LOGGER.debug( "Going to write pair <" + word + "|" + nonWord + ">" );
		if ( exact || word.length() > 0 ) word.writeSelfDelimUTF8( zipOut );
		if ( exact ) nonWord.writeSelfDelimUTF8( zipOut );
	}
	
	/* (non-Javadoc)
	 * @see it.unimi.di.big.mg4j.document.DocumentCollectionBuilder#close()
	 */
	
	public void close() throws IOException {
		if ( numberOfDocuments == 0 ) zipOut.putNextEntry( new ZipEntry( "dummy" ) );
		zipDataOutputStream.close();
		final ZipDocumentCollection zipDocumentCollection = new ZipDocumentCollection( basenameSuffix + ZipDocumentCollection.ZIP_EXTENSION, factory, numberOfDocuments, exact );
		BinIO.storeObject( zipDocumentCollection, basenameSuffix + DocumentCollection.DEFAULT_EXTENSION );
		zipDocumentCollection.close();
	}
	
	@SuppressWarnings("unchecked")
	public void build( final DocumentSequence inputSequence ) throws IOException {
		numberOfDocuments = 0;

		final DocumentIterator docIt = inputSequence.iterator();
		if ( factory != inputSequence.factory() ) throw new IllegalStateException( "The factory provided by the constructor does not correspond to the factory of the input sequence" );
		final int numberOfFields = factory.numberOfFields();
		WordReader wordReader;
		MutableString word = new MutableString();
		MutableString nonWord = new MutableString();
		open( "" );
		for (;;) {
			Document document = docIt.nextDocument();
			if ( document == null ) break;
			startDocument( document.title(), document.uri() );
			
			for ( int field = 0; field < numberOfFields; field++ ) {
				Object content = document.content( field );
				if ( factory.fieldType( field ) == FieldType.TEXT ) {
					startTextField();
					wordReader = document.wordReader( field );
					wordReader.setReader( (Reader)content );
					while ( wordReader.next( word, nonWord ) ) add( word, nonWord );
					endTextField();
				}
				else if ( factory.fieldType( field ) == FieldType.VIRTUAL ) virtualField( (List<VirtualDocumentFragment>)content );
				else nonTextField( content );
			}
			document.close();
			endDocument();
		}
		docIt.close();
		close();
	}
	

	
	public static void main( final String[] arg ) throws JSAPException, IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, IllegalArgumentException, SecurityException {

		SimpleJSAP jsap = new SimpleJSAP( ZipDocumentCollectionBuilder.class.getName(), "Produces a zip document collection from an existing document sequence.",
				new Parameter[] {
					new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
					new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor." ),
					new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file" ).setAllowMultipleDeclarations( true ),
					new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( Scan.DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter." ),
					new Switch( "approximated", 'a', "approximated", "If specified, non-words will not be copied." ),
					new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
					new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename for the collection." ),
				}
		);
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		DocumentSequence documentSequence = Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );
		final ProgressLogger progressLogger = new ProgressLogger( LOGGER, "documents" );
		if ( documentSequence instanceof DocumentCollection ) progressLogger.expectedUpdates = ((DocumentCollection)documentSequence).size();
		final ZipDocumentCollectionBuilder zipDocumentCollectionBuilder = new ZipDocumentCollectionBuilder( jsapResult.getString( "basename" ), documentSequence.factory(), !jsapResult.getBoolean( "approximated") );
		zipDocumentCollectionBuilder.open( "" );
		zipDocumentCollectionBuilder.build( documentSequence );
	}
}
