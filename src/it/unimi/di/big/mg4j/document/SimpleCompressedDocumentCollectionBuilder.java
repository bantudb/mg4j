package it.unimi.di.big.mg4j.document;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2009-2016 Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.document.SimpleCompressedDocumentCollection.FrequencyCodec;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;

/** A builder for {@linkplain SimpleCompressedDocumentCollection simple compressed document collections}.
 * 
 * @author Sebastiano Vigna
 */

public class SimpleCompressedDocumentCollectionBuilder implements DocumentCollectionBuilder {
	private static final int INITIAL_TERM_MAP_SIZE = 1000;
	/** The I/O factory that will be used to create files. */
	private final IOFactory ioFactory;
	/** The factory of the base document sequence. */
	private final DocumentFactory documentFactory;
	/** Whether will are building an exact collection (i.e., whether it stores nonwords). */
	private final boolean exact;
	/** Whether we should make the collection relative by taking just the basename of {@link #basenameSuffix}. */
	private boolean relative;
	/** A frequency keeper used to compress document terms. */
	private final FrequencyCodec termsFrequencyKeeper;
	/** A frequency keeper used to compress document nonterms, or <code>null</code> if {@link #exact} is false. */
	private final FrequencyCodec nonTermsFrequencyKeeper;
	/** The basename of the builder. */
	private String basename;
	/** The basename of current collection. */
	private String basenameSuffix;	
	/** The output bit stream for documents. */
	private OutputBitStream documentsOutputBitStream;
	/** The output stream for terms. */
	private CountingOutputStream termsOutputStream;
	/** The output stream for nonterms, or <code>null</code> if {@link #exact} is false. */
	private CountingOutputStream nonTermsOutputStream;
	/** The output bit stream for document offsets. */
	private OutputBitStream documentOffsetsObs;
	/** The output bit stream for term offsets. */
	private OutputBitStream termOffsetsObs;
	/** The output bit stream for nonterms offsets, or <code>null</code> if {@link #exact} is false. */
	private OutputBitStream nonTermOffsetsObs;
	/** A temporary cache for the content of a field as a list of global term numbers. If the collection is exact, it alternates terms and nonterms. */
	private IntArrayList fieldContent;
	/** The map from term to global term numbers, in order of appearance. */
	private Object2IntOpenHashMap<MutableString> terms;
	/** The map from term to global nonterm numbers, in order of appearance, or <code>null</code> if {@link #exact} is false. */
	private Object2IntOpenHashMap<MutableString> nonTerms;
	/** The number of documents indexed so far. */
	private int documents;
	/** The number of words indexed so far. */
	private long words;
	/** The number of fields indexed so far. */
	private long fields;
	/** The number of bits used to code words. */
	private long bitsForWords;
	/** The number of bits used to code nonwords. */
	private long bitsForNonWords;
	/** The number of bits used to code field lengths (the number of words/nonwords pairs). */
	private long bitsForFieldLengths;
	/** The number of bits used to code URIs. */
	private long bitsForUris;
	/** The number of bits used to code document titles. */
	private long bitsForTitles;
	/** Whether we are compressing non-text or virtual fields. */
	private boolean hasNonText;
	/** The zip output stream used to store non-text and virtual fields if {@link #hasNonText} is true, or  <code>null</code> otherwise. */
	private ZipOutputStream nonTextZipOutputStream;
	/** {@link #nonTextZipOutputStream} wrapped in a {@link DataOutputStream}. */
	private DataOutputStream nonTextZipDataOutputStream;

	public SimpleCompressedDocumentCollectionBuilder( final String basename, final DocumentFactory documentFactory, final boolean exact ) {
		this( IOFactory.FILESYSTEM_FACTORY, basename, documentFactory, exact );
	}

	public SimpleCompressedDocumentCollectionBuilder( final IOFactory ioFactory, final String basename, final DocumentFactory documentFactory, final boolean exact ) {
		this( ioFactory, basename, documentFactory, exact, false );
	}

	public SimpleCompressedDocumentCollectionBuilder( final IOFactory ioFactory, final String basename, final DocumentFactory documentFactory, final boolean exact, final boolean relative ) {
		this.ioFactory = ioFactory;
		this.basename = basename;
		this.documentFactory = documentFactory;
		this.exact = exact;
		this.relative = relative;
		this.termsFrequencyKeeper = new SimpleCompressedDocumentCollection.FrequencyCodec();
		this.nonTermsFrequencyKeeper = exact ? new SimpleCompressedDocumentCollection.FrequencyCodec() : null;

		boolean hasNonText = false;
		for( int i = documentFactory.numberOfFields(); i-- != 0; ) hasNonText |= documentFactory.fieldType( i ) != FieldType.TEXT;
		this.hasNonText = hasNonText;
				
		terms = new Object2IntOpenHashMap<MutableString>( INITIAL_TERM_MAP_SIZE );
		terms.defaultReturnValue( -1 );
		if ( exact ) {
			nonTerms = new Object2IntOpenHashMap<MutableString>( INITIAL_TERM_MAP_SIZE );
			nonTerms.defaultReturnValue( -1 );
		}
		else nonTerms = null;
	}

	public String basename() {
		return basename;
	}
		
	public void open( final CharSequence suffix ) throws IOException {
		basenameSuffix = basename + suffix;
		documentsOutputBitStream = new OutputBitStream( ioFactory.getOutputStream( basenameSuffix + SimpleCompressedDocumentCollection.DOCUMENTS_EXTENSION ), false );
		termsOutputStream = new CountingOutputStream( new FastBufferedOutputStream( ioFactory.getOutputStream( basenameSuffix + SimpleCompressedDocumentCollection.TERMS_EXTENSION ) ) );
		nonTermsOutputStream = exact ? new CountingOutputStream( new FastBufferedOutputStream( ioFactory.getOutputStream( basenameSuffix + SimpleCompressedDocumentCollection.NONTERMS_EXTENSION ) ) ) : null;
		documentOffsetsObs = new OutputBitStream( ioFactory.getOutputStream( basenameSuffix + SimpleCompressedDocumentCollection.DOCUMENT_OFFSETS_EXTENSION ), false );
		termOffsetsObs = new OutputBitStream( ioFactory.getOutputStream( basenameSuffix + SimpleCompressedDocumentCollection.TERM_OFFSETS_EXTENSION ), false );
		nonTermOffsetsObs = exact? new OutputBitStream( ioFactory.getOutputStream( basenameSuffix + SimpleCompressedDocumentCollection.NONTERM_OFFSETS_EXTENSION ), false ) : null;
		fieldContent = new IntArrayList();

		if ( hasNonText ) nonTextZipDataOutputStream = new DataOutputStream( nonTextZipOutputStream = new ZipOutputStream( new FastBufferedOutputStream( ioFactory.getOutputStream( basenameSuffix + ZipDocumentCollection.ZIP_EXTENSION ) ) ) );

		terms.clear();
		terms.trim( INITIAL_TERM_MAP_SIZE );
		if ( exact ) {
			nonTerms.clear();
			nonTerms.trim( INITIAL_TERM_MAP_SIZE );
		}
		words = fields = bitsForWords = bitsForNonWords = bitsForFieldLengths = bitsForUris = bitsForTitles = documents = 0;

		// First offset
		documentOffsetsObs.writeDelta( 0 );
		termOffsetsObs.writeDelta( 0 );
		if ( exact ) nonTermOffsetsObs.writeDelta( 0 );
		
	}
	
	
	public void add( MutableString word, MutableString nonWord ) throws IOException {
		int t = terms.getInt( word );
		if ( t == -1 ) {
			terms.put( word.copy(), t = terms.size() );
			termsOutputStream.resetByteCount();
			word.writeSelfDelimUTF8( termsOutputStream );
			termOffsetsObs.writeLongDelta( termsOutputStream.getByteCount() );
		}
		fieldContent.add( t );
		if ( exact ) {
			t = nonTerms.getInt( nonWord );
			if ( t == -1 ) {
				nonTerms.put( nonWord.copy(), t = nonTerms.size() );
				nonTermsOutputStream.resetByteCount();
				nonWord.writeSelfDelimUTF8( nonTermsOutputStream );
				nonTermOffsetsObs.writeLongDelta( nonTermsOutputStream.getByteCount() );
			}
			fieldContent.add( t );
		}
	}

	
	public void close() throws IOException {
		documentsOutputBitStream.close();
		termsOutputStream.close();
		IOUtils.closeQuietly( nonTermsOutputStream );
		documentOffsetsObs.close();
		termOffsetsObs.close();
		if ( nonTermOffsetsObs != null ) nonTermOffsetsObs.close();
		if ( hasNonText ) {
			if ( documents == 0 ) nonTextZipOutputStream.putNextEntry( new ZipEntry( "dummy" ) );
			nonTextZipDataOutputStream.close();
		}

		final SimpleCompressedDocumentCollection simpleCompressedDocumentCollection = new SimpleCompressedDocumentCollection( relative ? new File( basenameSuffix ).getName().toString() : basenameSuffix, documents, terms.size(), nonTerms != null ? nonTerms.size() : -1, exact, documentFactory );
		IOFactories.storeObject( ioFactory, simpleCompressedDocumentCollection, basenameSuffix + DocumentCollection.DEFAULT_EXTENSION );
		simpleCompressedDocumentCollection.close();
		
		final PrintStream stats = new PrintStream( ioFactory.getOutputStream( basenameSuffix + SimpleCompressedDocumentCollection.STATS_EXTENSION ) );
		final long overallBits = bitsForTitles + bitsForUris + bitsForFieldLengths + bitsForWords + bitsForNonWords;
		stats.println( "Documents: " + Util.format( documents ) + " (" + Util.format( overallBits ) + ", " + Util.format( overallBits / (double)documents ) + " bits per document)" );
		stats.println( "Terms: " + Util.format( terms.size() ) + " (" + Util.format( words ) + " words, " + Util.format( bitsForWords ) + " bits, " + Util.format( bitsForWords / (double)words ) + " bits per word)" );
		if ( exact ) stats.println( "Nonterms: " + Util.format( nonTerms.size() ) + " (" + Util.format( words ) + " nonwords, " + Util.format( bitsForNonWords ) + " bits, " + Util.format( bitsForNonWords / (double)words ) + " bits per nonword)" );
		stats.println( "Bits for field lengths: " + Util.format( bitsForFieldLengths ) + " (" + Util.format( bitsForFieldLengths / (double)fields ) + " bits per field)" );
		stats.println( "Bits for URIs: " + Util.format( bitsForUris ) + " (" + Util.format( bitsForUris / (double)documents ) + " bits per URI)" );
		stats.println( "Bits for titles: " + Util.format( bitsForTitles ) + " (" + Util.format( bitsForTitles / (double)documents ) + " bits per title)" );
		stats.close();

	}

	
	public void endDocument() throws IOException {
		documentOffsetsObs.writeLongDelta( documentsOutputBitStream.writtenBits() );
		if ( hasNonText ) nonTextZipOutputStream.closeEntry();
	}
	

	public void endTextField() throws IOException {
		final int size = fieldContent.size();
		words += size / ( exact ? 2 : 1 );
		bitsForFieldLengths += documentsOutputBitStream.writeDelta( size / ( exact ? 2 : 1 ) );
		termsFrequencyKeeper.reset();
		if ( exact ) {
			nonTermsFrequencyKeeper.reset();
			for( int i = 0; i < size; i += 2 ) {
				bitsForWords += documentsOutputBitStream.writeDelta( termsFrequencyKeeper.encode( fieldContent.getInt( i ) ) );
				bitsForNonWords += documentsOutputBitStream.writeDelta( nonTermsFrequencyKeeper.encode( fieldContent.getInt( i + 1 ) ) );
			}
		}
		else for( int i = 0; i < size; i++ ) bitsForWords += documentsOutputBitStream.writeDelta( termsFrequencyKeeper.encode( fieldContent.getInt( i ) ) );
	}

	public void nonTextField( Object o ) throws IOException {
		final ObjectOutputStream oos = new ObjectOutputStream( nonTextZipDataOutputStream );
		oos.writeObject( o );
		oos.flush();
	}

	public static int writeSelfDelimitedUtf8String( final OutputBitStream obs, final CharSequence s ) throws IOException {
		final int len = s.length();
		int bits = 0;
		bits += obs.writeDelta( len );
		for( int i = 0; i < len; i++ ) bits += obs.writeZeta( s.charAt( i ), 7 );
		return bits;
	}
	

	
	public void startDocument( CharSequence title, CharSequence uri ) throws IOException {
		documentsOutputBitStream.writtenBits( 0 );
		bitsForUris += writeSelfDelimitedUtf8String( documentsOutputBitStream, uri == null ? "" : uri );
		bitsForTitles += writeSelfDelimitedUtf8String( documentsOutputBitStream, title == null ? "" : title );
		if ( hasNonText ) {
			final ZipEntry currEntry = new ZipEntry( Integer.toString( documents ) );
			nonTextZipOutputStream.putNextEntry( currEntry );

		}
		documents++;
	}

	
	public void startTextField() {
		fieldContent.size( 0 );
		fields++;
	}

	public void virtualField( final List<VirtualDocumentFragment> fragments ) throws IOException {
		nonTextZipDataOutputStream.writeInt( fragments.size() );
		for ( VirtualDocumentFragment fragment: fragments ) {
			fragment.documentSpecifier().writeSelfDelimUTF8( nonTextZipOutputStream );
			fragment.text().writeSelfDelimUTF8( nonTextZipOutputStream );
		}
	}

	@SuppressWarnings("unchecked")
	public void build( final DocumentSequence inputSequence ) throws IOException {
		final DocumentIterator docIt = inputSequence.iterator();
		// ALERT: this should be cloned. Doesn't word correctly (e.g., for a custom word reader).
		//if ( ! documentFactory.equals( inputSequence.factory() ) ) throw new IllegalStateException( "The factory provided by the constructor does not correspond to the factory of the input sequence" );
		final int numberOfFields = documentFactory.numberOfFields();
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
				if ( documentFactory.fieldType( field ) == FieldType.TEXT ) {
					startTextField();
					wordReader = document.wordReader( field );
					wordReader.setReader( (Reader)content );
					while ( wordReader.next( word, nonWord ) ) add( word, nonWord );
					endTextField();
				}
				else if ( documentFactory.fieldType( field ) == FieldType.VIRTUAL ) virtualField( (ObjectList<VirtualDocumentFragment>)content );
				else nonTextField( content );
			}
			document.close();
			endDocument();
		}
		docIt.close();
		close();
	}
}
