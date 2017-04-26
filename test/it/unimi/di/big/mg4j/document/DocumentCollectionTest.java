package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Paolo Boldi 
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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongIterators;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.StringTokenizer;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;


public class DocumentCollectionTest {

	/* We consider documents abstractly described by two fields each. 
	 * 
	 * WARNING: the first string MUST be a prefix of the second string. */
	private final static String[][] document = new String[][] {
			//              0   1   2   3      0   1   2   3   4   5   6   7   8   9   10  11  12  13  14
			new String[] { "xxx yyy zzz xxx", "xxx yyy zzz xxx aaa xxx aaa yyy aaa yyy aaa zzz aaa www aaa" },
			new String[] { "aaa xxx aaa aaa", "aaa xxx aaa aaa xxx aaa zzz uuu" },
			new String[] { "aaa uuu aaa"    , "aaa uuu aaa xxx xxx xxx aaa xxx" },
			// This tests that zipped collections handle properly initial spaces and
			// that word readers are propagated correctly.
			new String[] { " aaa uuu aaa"    , " aaa uuu aaa _ __ xxx _ xxx xxx aaa xxx" },
	};

	private final static String[][] document2 = new String[][] {
		//              0   1   2   3      0   1   2   3   4   5   6   7   8   9   10  11  12  13  14
		new String[] { "xxx yyy zzz xxx", "xxx yyy zzz xxx aaa xxx aaa yyy aaa yyy aaa zzz aaa www aaa" },
		new String[] { "aaa xxx aaa aaa", "aaa xxx aaa aaa xxx aaa zzz uuu" },
		new String[] { "aaa uuu aaa"    , "aaa uuu aaa xxx xxx xxx aaa xxx" },
		// This tests that zipped collections handle properly initial spaces and
		// that word readers are propagated correctly.
		new String[] { " aaa uuu aaa"    , " aaa uuu aaa _ __ xxx _ xxx xxx aaa xxx" },
		new String[] { "xxx yyy zzz xxx", "xxx yyy zzz xxx aaa xxx aaa yyy aaa yyy aaa zzz aaa www aaa" },
		new String[] { "aaa xxx aaa aaa", "aaa xxx aaa aaa xxx aaa zzz uuu" },
		new String[] { "aaa uuu aaa"    , "aaa uuu aaa xxx xxx xxx aaa xxx" },
		// This tests that zipped collections handle properly initial spaces and
		// that word readers are propagated correctly.
		new String[] { " aaa uuu aaa"    , " aaa uuu aaa _ __ xxx _ xxx xxx aaa xxx" },
	};

	private final static Properties DEFAULT_PROPERTIES = new Properties();
	static {
		DEFAULT_PROPERTIES.setProperty( PropertyBasedDocumentFactory.MetadataKeys.ENCODING, "ASCII" );
		DEFAULT_PROPERTIES.setProperty( PropertyBasedDocumentFactory.MetadataKeys.WORDREADER, it.unimi.dsi.io.FastBufferedReader.class.getName() + "(_)" );
	}
	
	/** The number of documents. */
	private final static int ndoc = document.length;
	/** The temporary directory where all tests are run. */
	private static File tempDir;
	/** The set of files in the HTML directory. */
	private static String[] htmlFileSet;
	
	/** Given a two-field document, produce an HTML document with the first field as title and
	 *  the second field as body.
	 *  
	 *  @param document the document.
	 *  @return the HTML version of the document.
	 */	
	private static String getHTMLDocument( String[] document ) {
		MutableString res = new MutableString();
		res.append( "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" );
		res.append( "<HTML>\n<HEAD>\n<TITLE>" + document[ 0 ] + "</TITLE>\n" );
		// Do NOT append the first part of the body
		res.append( "<BODY>\n" + document[ 1 ].substring( document[ 0 ].length() ) );
		res.append( "\n</BODY>\n" );
		res.append( "</HTML>" );
		return res.toString();
	}
	
	/** Given a two-field document, produce a mbox document with the first field as subject and
	 *  the second field as body.
	 *  
	 *  @param document the document.
	 *  @return the HTML version of the document.
	 */
	private static String getMboxDocument( String[] document ) {
		MutableString res = new MutableString();
		res.append( "From MAILER-DAEMON Fri Apr 15 16:22:32 2005\n" );
		res.append( "Date: 15 Apr 2005 16:22:32 +0200\n" );
		res.append( "From: Mail System Internal Data <MAILER-DAEMON@sliver.usr.dsi.unimi.it>\n" );
		res.append( "Subject: " + document[ 0 ] + "\n" );
		res.append( "Message-ID: <1113574952@sliver.usr.dsi.unimi.it>\n" );
		res.append( "X-IMAP: 1102967122 0000138458\n" );
		res.append( "Return-Path: <matteo.xxx@unimi.it>\n" );
		res.append( "Received: from localhost (localhost.localdomain [127.0.0.1])\n" );
        res.append( "\tby sliver.usr.dsi.unimi.it (8.12.11/8.12.11) with ESMTP id iAUNtadn007305\n");
        res.append( "\tfor <vigna@localhost>; Wed, 1 Dec 2004 00:55:36 +0100\n" );
        res.append( "Received: from law5.usr.dsi.unimi.it [159.149.146.241]\n" );
        res.append( "\tby localhost with IMAP (fetchmail-6.2.5)\n" );
        res.append( "\tfor vigna@localhost (single-drop); Wed, 01 Dec 2004 00:55:36 +0100 (CET)\n" );
        res.append( "To: vigna@dsi.unimi.it\n" );
        res.append( "Message-id: <Pine.WNT.4.33.0412010051240.-209505@p233-mmx>\n" );
        res.append( "Content-type: TEXT/PLAIN; charset=iso-8859-15\n" );
        res.append( "X-Warning: UNAuthenticated Sender\n" );
        res.append( "Content-Transfer-Encoding: 8bit\n" );
        res.append( "Content-Length: " + document[ 1 ].length() + "\n" );
		res.append( "\n" );
		res.append( document[ 1 ] + "\n" );
		return res.toString();
	}

	
	/** Checks that the tokenizer and the word reader return exactly the same sequence of words. 
	 * 
	 * @param wordReader the word reader.
	 * @param tok the tokenizer.
	 * @throws IOException
	 */
	private void checkSameWords( WordReader wordReader, StringTokenizer tok ) throws IOException {
		MutableString word = new MutableString();
		MutableString nonWord = new MutableString();
		boolean aWordInDocum, aWordInDocument;
		boolean firstTime = true;
		for (;;) {
			aWordInDocum = wordReader.next( word, nonWord );
			if ( firstTime ) {
				firstTime = false;
				if ( word.equals( "" ) ) continue;
			}
			assertFalse( aWordInDocum && word.equals( "" ) );
			aWordInDocument = tok.hasMoreElements();
			assertTrue( aWordInDocum == aWordInDocument );
			if ( !aWordInDocum ) break;
			assertEquals( tok.nextElement(), word.toString() );
		}
	}
	
	/** Checks that the documents in the collection have the same sequence of words as in
	 *  document: the names of the fields to be checked are specified in the array.
	 *  
	 * @param coll the collection.
	 * @param fieldName the field names.
	 * @param document documents to be checked against.
	 * @throws IOException
	 */
	private void checkAllDocuments( final DocumentCollection coll, final String[] fieldName, final String[][] document ) throws IOException {
		final int nfields = fieldName.length;
		final int[] fieldNumber = new int[ nfields ];
		final int[] arrayIndex = new int[ nfields ];
		// Look for field indices
		for ( int i = 0; i < nfields; i++ ) {
			arrayIndex[ i ] = i;
			int j;
			for ( j = 0; j < coll.factory().numberOfFields(); j++ )
				if ( coll.factory().fieldName( j ).equals( fieldName[ i ] ) ) {
					fieldNumber[ i ] = j;
					break;
				}
			assert j < coll.factory().numberOfFields();
		}
		// Sort fields to guarantee that they are correctly numbered
		Arrays.quickSort( 0, nfields, new AbstractIntComparator() {
			private static final long serialVersionUID = 1L;

			public int compare( int x, int y ) {
				return fieldNumber[ x ] - fieldNumber[ y ];
			}}, new Swapper() {
				public void swap( int x, int y ) {
					int t = fieldNumber[ x ]; fieldNumber[ x ] = fieldNumber[ y ]; fieldNumber[ y ] = t;
					t = arrayIndex[ x ]; arrayIndex[ x ] = arrayIndex[ y ]; arrayIndex[ y ] = t;
					String q = fieldName[ x ]; fieldName[ x ] = fieldName[ y ]; fieldName[ y ] = q;
				}} );
		// Start checking
		for ( int doc = 0; doc < coll.size(); doc++ ) {
			Document docum = coll.document( doc );
			for ( int i = 0; i < nfields; i++ ) {
				int field = fieldNumber[ i ];
				Reader content = (Reader)docum.content( field );
				WordReader wordReader = docum.wordReader( field );
				wordReader.setReader( content );
				StringTokenizer tok = new StringTokenizer( document[ doc ][ arrayIndex[ i ] ] );
				System.err.println( "Checking document " + doc + " field " + fieldName[ i ] + " (" + field + ")" );
				checkSameWords( wordReader, tok );
			}
			docum.close();
		}
	}

	/** Checks that the documents in the sequence have the same sequence of words as in
	 *  <code>document</code>: the names of the fields to be checked are specified in the array.
	 *  
	 * @param seq the sequence.
	 * @param fieldName the field names.
	 * @param document documents to be checked against.
	 * @throws IOException
	 */
	private void checkAllDocumentsSeq( final DocumentSequence seq, final String[] fieldName, final String[][] document ) throws IOException {
		final int nfields = fieldName.length;
		final int[] fieldNumber = new int[ nfields ];
		final int[] arrayIndex = new int[ nfields ];
		// Look for field indices
		for ( int i = 0; i < nfields; i++ ) {
			arrayIndex[ i ] = i;
			int j;
			for ( j = 0; j < seq.factory().numberOfFields(); j++ )
				if ( seq.factory().fieldName( j ).equals( fieldName[ i ] ) ) {
					fieldNumber[ i ] = j;
					break;
				}
			assert j < seq.factory().numberOfFields();
		}
		// Sort fields to guarantee that they are correctly numbered
		Arrays.quickSort( 0, nfields, new AbstractIntComparator() {
			private static final long serialVersionUID = 1L;

			public int compare( int x, int y ) {
				return fieldNumber[ x ] - fieldNumber[ y ];
			}}, new Swapper() {
				public void swap( int x, int y ) {
					int t = fieldNumber[ x ]; fieldNumber[ x ] = fieldNumber[ y ]; fieldNumber[ y ] = t;
					t = arrayIndex[ x ]; arrayIndex[ x ] = arrayIndex[ y ]; arrayIndex[ y ] = t;
					String q = fieldName[ x ]; fieldName[ x ] = fieldName[ y ]; fieldName[ y ] = q;
				}} );
		// Start checking
		DocumentIterator iterator = seq.iterator();
		Document docum;
		int doc = 0;
		while ( ( docum = iterator.nextDocument() ) != null ) {
			for ( int i = 0; i < nfields; i++ ) {
				int field = fieldNumber[ i ];
				Reader content = (Reader)docum.content( field );
				WordReader wordReader = docum.wordReader( field );
				wordReader.setReader( content );
				StringTokenizer tok = new StringTokenizer( document[ doc ][ arrayIndex[ i ] ] );
				System.err.println( "Checking sequentially document " + doc + " field " + fieldName[ i ] + " (" + field + ")" );
				checkSameWords( wordReader, tok );
			}
			docum.close();
			doc++;
		}
		iterator.close();
	}

	@BeforeClass
	public static void setUp() throws IOException, ConfigurationException {
		// Create a new directory under /tmp
		tempDir = File.createTempFile( "mg4jtest", null );
		tempDir.delete();
		tempDir.mkdir();
		// Now create the hierarchy for HTML files
		File htmlDir = new File( tempDir, "html" );
		htmlDir.mkdir();
		System.err.println( "Temporary directory: " + tempDir );
		htmlFileSet = new String[ ndoc ];
		for ( int i = 0; i < ndoc; i++ ) {
			String docFile = new File( htmlDir, "doc" + i + ".html" ).toString();
			htmlFileSet[ i ] = docFile;
			Writer docWriter = new OutputStreamWriter( new FileOutputStream( docFile ), "ISO-8859-1" );
			docWriter.write( getHTMLDocument( document[ i ] ) );
			docWriter.close();
		}
		// Now create the mbox file
		Writer mboxWriter = new OutputStreamWriter( new FileOutputStream( new File( tempDir, "mbox" ) ), "ISO-8859-1" );
		for ( int i = 0; i < ndoc; i++ ) 
			mboxWriter.write( getMboxDocument( document[ i ] ) );
		mboxWriter.close();

		// Now create the zip collections
		FileSetDocumentCollection fileSetDocumentCollection = new FileSetDocumentCollection( htmlFileSet, new HtmlDocumentFactory( DEFAULT_PROPERTIES ) );
		ZipDocumentCollectionBuilder zipCollBuilder = new ZipDocumentCollectionBuilder( new File( tempDir, "zip" ).toString(), 
				fileSetDocumentCollection.factory(), true );
		zipCollBuilder.build( fileSetDocumentCollection );
		
		ZipDocumentCollectionBuilder apprZipCollBuilder = new ZipDocumentCollectionBuilder( new File( tempDir, "azip" ).toString(), 
				fileSetDocumentCollection.factory(), false );
		apprZipCollBuilder.build( fileSetDocumentCollection );
		fileSetDocumentCollection.close();

		// Now create the simple collections
		SimpleCompressedDocumentCollectionBuilder simpleCollBuilder = new SimpleCompressedDocumentCollectionBuilder( new File( tempDir, "simple" ).toString(), 
				fileSetDocumentCollection.factory(), true );
		simpleCollBuilder.build( fileSetDocumentCollection );
		
		SimpleCompressedDocumentCollectionBuilder apprSimpleCollBuilder = new SimpleCompressedDocumentCollectionBuilder( new File( tempDir, "asimple" ).toString(), 
				fileSetDocumentCollection.factory(), false );
		apprSimpleCollBuilder.build( fileSetDocumentCollection );
		fileSetDocumentCollection.close();
	}

	protected void tearDown() throws IOException {
		FileUtils.forceDelete( tempDir );
	}

	@Test
	public void testFileSetDocumentCollection() throws IOException, ConfigurationException {
		System.err.println( "Checking fileset collection" );
		FileSetDocumentCollection coll = new FileSetDocumentCollection( htmlFileSet, new HtmlDocumentFactory( DEFAULT_PROPERTIES ) );
		assertEquals( coll.size(), ndoc );
		checkAllDocuments( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testFileSetDocumentCollectionSeq() throws IOException, ConfigurationException {
		System.err.println( "Checking fileset collection sequentially" );
		FileSetDocumentCollection coll = new FileSetDocumentCollection( htmlFileSet, new HtmlDocumentFactory( DEFAULT_PROPERTIES ) );
		checkAllDocumentsSeq( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testZipDocumentCollection() throws IOException, ClassNotFoundException {
		System.err.println( "Checking zipped collection" );
		ZipDocumentCollection coll = (ZipDocumentCollection)BinIO.loadObject( new File( tempDir, "zip.collection" ).toString() );
		checkAllDocuments( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testZipDocumentCollectionSeq() throws IOException, ClassNotFoundException {
		System.err.println( "Checking zipped collection sequentially" );
		ZipDocumentCollection coll = (ZipDocumentCollection)BinIO.loadObject( new File( tempDir, "zip.collection" ).toString() );
		checkAllDocumentsSeq( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testZipDocumentCollectionAppr() throws IOException, ClassNotFoundException {
		System.err.println( "Checking approximated zipped collection" );
		ZipDocumentCollection coll = (ZipDocumentCollection)BinIO.loadObject( new File( tempDir, "azip.collection" ).toString() );
		checkAllDocuments( coll, new String[] { "title", "text" }, document );
		coll.close();
	}
	
	@Test
	public void testZipDocumentCollectionApprSeq() throws IOException, ClassNotFoundException {
		System.err.println( "Checking approximated zipped collection sequentially" );
		ZipDocumentCollection coll = (ZipDocumentCollection)BinIO.loadObject( new File( tempDir, "azip.collection" ).toString() );
		checkAllDocumentsSeq( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testSimpleCompressedDocumentCollection() throws IOException, ClassNotFoundException {
		System.err.println( "Checking simple compressed collection" );
		SimpleCompressedDocumentCollection coll = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "simple.collection" ).toString() );
		checkAllDocuments( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testSimpleCompressedDocumentCollectionSeq() throws IOException, ClassNotFoundException {
		System.err.println( "Checking simple compressed collection sequentially" );
		SimpleCompressedDocumentCollection coll = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "simple.collection" ).toString() );
		checkAllDocumentsSeq( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testSimpleCompressedDocumentCollectionAppr() throws IOException, ClassNotFoundException {
		System.err.println( "Checking approximated simple compressed collection" );
		SimpleCompressedDocumentCollection coll = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );
		checkAllDocuments( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testSimpleCompressedDocumentCollectionApprSeq() throws IOException, ClassNotFoundException {
		System.err.println( "Checking approximated simple compressed collection sequentially" );
		SimpleCompressedDocumentCollection coll = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );
		checkAllDocumentsSeq( coll, new String[] { "title", "text" }, document );
		coll.close();
	}

	@Test
	public void testConcatenated() throws IOException, ClassNotFoundException {
		SimpleCompressedDocumentCollection coll0 = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );
		SimpleCompressedDocumentCollection coll1 = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );
		
		ConcatenatedDocumentCollection concatenatedDocumentCollection = new ConcatenatedDocumentCollection( new String[] { new File( tempDir, "asimple.collection" ).toString(), new File( tempDir, "asimple.collection" ).toString() } );
		ConcatenatedDocumentSequence concatenatedDocumentSequence0 = new ConcatenatedDocumentSequence( coll0, coll1 );
		ConcatenatedDocumentSequence concatenatedDocumentSequence1 = new ConcatenatedDocumentSequence( new File( tempDir, "asimple.collection" ).toString(), new File( tempDir, "asimple.collection" ).toString() );
		checkAllDocumentsSeq( concatenatedDocumentSequence0, new String[] { "title", "text" }, document2 );
		checkAllDocumentsSeq( concatenatedDocumentSequence1, new String[] { "title", "text" }, document2 );
		checkAllDocuments( concatenatedDocumentCollection, new String[] { "title", "text" }, document2 );
		concatenatedDocumentCollection.close();
		concatenatedDocumentSequence0.close();
		concatenatedDocumentSequence0.close();
	}

	@Test
	public void testInputStreamSequence() throws IOException, ConfigurationException {
		System.err.println( "Checking input stream (text field only)" );
		// Extract only field number 1, and write it out with separator '\u0000'
		MutableString res = new MutableString();
		String[][] justSecondField = new String[ ndoc ][ 1 ];
		for ( int i = 0; i < ndoc; i++ ) {
			res.append( document[ i ][ 1 ] + "\u0000" );
			justSecondField[ i ][ 0 ] = document[ i ][ 1 ];
		}
		String resString = res.toString();
		// Write the sequence on a file (in UTF-8)
		Writer resWriter = new OutputStreamWriter( new FileOutputStream( new File( tempDir, "stream" ) ), "UTF-8" );
		resWriter.write( resString );
		resWriter.close();
		// Read it as a input stream document sequence
		InputStream is = new FileInputStream( new File( tempDir, "stream" ) );
		DocumentSequence seq = new InputStreamDocumentSequence( is, '\u0000', new IdentityDocumentFactory( DEFAULT_PROPERTIES ) );
		checkAllDocumentsSeq( seq, new String[] { "text" }, justSecondField );
		seq.close();
	}

	@Test
	public void testSubsetDocumentSequence() throws IOException, ClassNotFoundException {
		// All documents
		DocumentSequence seq = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );
		LongSet allDocuments = new LongOpenHashSet( LongIterators.fromTo( 0, document.length ) );
		SubsetDocumentSequence trivialSubsetDocumentSequence = new SubsetDocumentSequence( seq, allDocuments );
		checkAllDocumentsSeq( trivialSubsetDocumentSequence, new String[] { "title", "text" }, document );
		seq.close();
		trivialSubsetDocumentSequence.close();

		// Even documents only
		seq = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );		
		String[][] evenDocuments = new String[ document.length / 2 ][];
		LongSet evenDocumentPointers = new LongOpenHashSet();
		for ( int i = 0; i < evenDocuments.length; i++ ) {
			evenDocuments[ i ] = document[ 2 * i ];
			evenDocumentPointers.add( 2 * i );
		}
		SubsetDocumentSequence evenSubsetDocumentSequence = new SubsetDocumentSequence( seq, evenDocumentPointers );
		checkAllDocumentsSeq( evenSubsetDocumentSequence, new String[] { "title", "text" }, evenDocuments );
		seq.close();
		evenSubsetDocumentSequence.close();

		// All but number 3
		seq = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );		
		String[][] almostAll = new String[ document.length - 1 ][];
		LongSet almostAllDocumentPointers = new LongOpenHashSet();
		for ( int i = 0, j = 0; i < document.length; i++ ) 
			if ( i != 3 ) {
				almostAll[ j ] = document[ i ];
				almostAllDocumentPointers.add( j );
				j++;
			}
		SubsetDocumentSequence almostAllDocumentSequence = new SubsetDocumentSequence( seq, almostAllDocumentPointers );
		checkAllDocumentsSeq( almostAllDocumentSequence, new String[] { "title", "text" }, almostAll );
		seq.close();
		almostAllDocumentSequence.close();
		
		// None
		seq = (SimpleCompressedDocumentCollection)BinIO.loadObject( new File( tempDir, "asimple.collection" ).toString() );		
		String[][] none = new String[ 0 ][];
		LongSet noneDocumentPointers = new LongOpenHashSet();
		SubsetDocumentSequence noneDocumentSequence = new SubsetDocumentSequence( seq, noneDocumentPointers );
		checkAllDocumentsSeq( noneDocumentSequence, new String[] { "title", "text" }, none );
		seq.close();
		noneDocumentSequence.close();
	}
	

}
