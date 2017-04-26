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
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.di.big.mg4j.util.parser.callback.AnchorExtractor;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.AbstractLongIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;
import it.unimi.dsi.io.ByteBufferInputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.NullInputStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;
import java.util.zip.ZipFile;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.IOUtils;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** A basic, compressed document collection that can be easily built at indexing time.
 *
 * <p>Instances of this class record virtual and non-text fields just like {@link ZipDocumentCollection}&mdash;that is,
 * in a zip file. However, text fields are recorded in a simple but highly efficient format. Terms (and nonterms) are numbered globally
 * in an increasing way as they are met. While we scan each document, we keep track of frequencies for a limited number of terms:
 * terms are encoded with their frequency rank if we know their statistics, or by a special code derived from their
 * global number if we have no statistics about them. Every number involved is written in delta code.
 * 
 * <p>A collection can be <em>exact</em> or <em>approximated</em>: in the latter case, nonwords will not be recorded, and will
 * be turned into spaces when decompressing.
 * 
 * <p>A instance of this collection will be, as any other collection, serialised on a file, but it will refer to several other files
 * that are derived from the instance basename. Please use {@link AbstractDocumentSequence#load(CharSequence)}
 * to load instances of this collection.
 * 
 * <p>This class suffers the same scalability problem of {@link ZipDocumentCollection} if you compress non-text or virtual fields. Text
 * compression, on the other hand, is extremely efficient and scalable.
 *  
 * @author Sebastiano Vigna
 */

public class SimpleCompressedDocumentCollection extends AbstractDocumentCollection implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static final boolean DEBUG = false;
	protected static final boolean ASSERTS = false;

	/** Standard extension for the file containing encoded documents. */
	public static final String DOCUMENTS_EXTENSION = ".documents";
	/** Standard extension for the file containing document offsets stored as &delta;-encoded gaps. */
	public static final String DOCUMENT_OFFSETS_EXTENSION = ".docoffsets";
	/** Standard extension for the file containing terms in {@link MutableString#writeSelfDelimUTF8(java.io.OutputStream)} format. */
	public static final String TERMS_EXTENSION  = ".terms";
	/** Standard extension for the file containing term offsets stored as &delta;-encoded gaps. */
	public static final String TERM_OFFSETS_EXTENSION = ".termoffsets";
	/** Standard extension for the file containing nonterms in {@link MutableString#writeSelfDelimUTF8(java.io.OutputStream)} format. */
	public static final String NONTERMS_EXTENSION  = ".nonterms";
	/** Standard extension for the file containing nonterm offsets stored as &delta;-encoded gaps. */
	public static final String NONTERM_OFFSETS_EXTENSION = ".nontermoffsets";
	/** Standard extension for the stats file. */
	public static final String STATS_EXTENSION = ".stats";

	/** The basename of this collection. */
	private final String basename;
	/** Whether this collection is exact (i.e., whether it stores nonwords). */
	private final boolean exact;
	/** The number of documents in this collection. */
	private final long documents;
	/** The number of terms in this collection. */
	private final long terms;
	/** The number of nonterms in this collection, or -1 if {@link #exact} is false. */
	private final long nonTerms;
	/** The document offsets. */
	private transient EliasFanoMonotoneLongBigList docOffsets;
	/** The term offsets. */
	private transient EliasFanoMonotoneLongBigList termOffsets;
	/** The nonterm offsets, or <code>null</code> if {@link #exact} is false. */
	private transient EliasFanoMonotoneLongBigList nonTermOffsets;
	/** The input bit stream for documents. */
	private transient InputBitStream documentsInputBitStream;
	/** The input bit stream for terms. */
	private transient FastBufferedInputStream termsInputStream;
	/** The input bit stream for nonterms, or <code>null</code> if {@link #exact} is false. */
	private transient FastBufferedInputStream nonTermsInputStream;
	/** A frequency keeper used to decompress document terms. */
	private transient FrequencyCodec termsFrequencyKeeper;
	/** A frequency keeper used to decompress document nonterms, or <code>null</code> if {@link #exact} is false. */
	private transient FrequencyCodec nonTermsFrequencyKeeper;
	/** The underlying factory. */
	private final DocumentFactory factory;
	/** Whether this collection contains non-text or virtual fields. */
	private final boolean hasNonText;
	/** The zip file used to store non-text and virtual fields if {@link #hasNonText} is true, or  <code>null</code> if this collection does not store such fields. */
	private transient ZipFile zipFile;
	/** The input stream obtained by memory-mapping the file containing documents, or <code>null</code>. */
	private transient ByteBufferInputStream documentsByteBufferInputStream;
	/** The input stream obtained by memory-mapping the file containing terms, or <code>null</code>. */
	private transient ByteBufferInputStream termsByteBufferInputStream;
	/** The input stream obtained by memory-mapping the file containing nonterms, or <code>null</code>. */
	private transient ByteBufferInputStream nonTermsByteBufferInputStream;
	/** True if ancillary files have been all correctly opened. */
	private boolean fileOpenOk;
	/** True if memory mappings have been all been obtained. */
	private boolean fileMappingOk;

	/** An iterator used to load &delta;-encoded offset gaps. */
	private static final class OffsetsLongIterator extends AbstractLongIterator {
		private final long numberOfItems;
		private long currIndex;
		private long currValue;
		private final InputBitStream ibs;
		
		public OffsetsLongIterator( InputBitStream ibs, long numberOfItems ) {
			this.ibs = ibs;
			this.numberOfItems = numberOfItems;
		}
		
		public boolean hasNext() {
			return currIndex < numberOfItems;
		}
		
		@Override
		public long nextLong() {
			if ( ! hasNext() ) throw new NoSuchElementException();
			try {
				currIndex++;
				return currValue += ibs.readDelta();
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			}
		}
	}

	/** A simple codec for integers that remaps frequent numbers to smaller numbers. */
	protected static class FrequencyCodec {
		/** The size of the symbol queue. */
		private final static int MAX_QUEUE_SIZE = 2048;
		/** The symbol queue. */
		private final int[] queue;
		/** An array parallel to  {@link  #queue} containing frequencies. */
		private final int[] freq; 
		/** A map from input symbols to positions in {@link #queue}. */
		private final Int2IntOpenHashMap code2Pos;
		/** The current size of {{@link #queue}. */
		private int queueSize;
		
		public FrequencyCodec() {
			code2Pos = new Int2IntOpenHashMap();
			code2Pos.defaultReturnValue( -1 );
			queue = new int[ MAX_QUEUE_SIZE ];
			freq = new int[ MAX_QUEUE_SIZE ];
		}
		
		/** Empties the queue and the symbol-to-position map. */
		public void reset() {
			queueSize = 0;
			code2Pos.clear();
		}
		
		private final void newSymbol( final int symbol ) {
			if ( queueSize == MAX_QUEUE_SIZE ) {
				// Queue filled up. First, we guarantee that there are elements with frequency one.
				if ( freq[ MAX_QUEUE_SIZE -1 ] != 1 ) for( int j = MAX_QUEUE_SIZE; j-- != 0; ) freq[ j ] /= freq[ MAX_QUEUE_SIZE - 1 ];
				// Then, we remove half of them.
				int j = MAX_QUEUE_SIZE;
				while( j-- != 0 ) if ( freq[ j ] > 1 ) break;
				for( int k = j + ( MAX_QUEUE_SIZE - j ) / 2; k < MAX_QUEUE_SIZE; k++ ) {
					if ( ASSERTS ) assert freq[ k ] == 1; 
					code2Pos.remove( queue[ k ] );
				}
				queueSize = j + ( MAX_QUEUE_SIZE - j ) / 2;
			}
			
			// Now we know that we have space.
			if ( ASSERTS ) assert queueSize < MAX_QUEUE_SIZE;
			code2Pos.put( symbol, queueSize );
			queue[ queueSize ] = symbol;
			freq[ queueSize ] = 1;
			queueSize++;
		}
		
		private final void oldSymbol( final int pos ) {
			// Term already in list
			// Find term to exchange for change of frequency
			int ex = pos;
			while( ex >= 0 && freq[ ex ] == freq[ pos ] ) ex--;
			++ex;
			freq[ pos ]++;
			// Exchange
			int t = queue[ pos ];
			queue[ pos ] = queue[ ex ];
			queue[ ex ] = t;
			t = freq[ pos ];
			freq[ pos ] = freq[ ex ];
			freq[ ex ] = t;
			code2Pos.put( queue[ ex ], ex );
			code2Pos.put( queue[ pos ], pos );
		}
		
		/** Encodes a symbol, returning a (hopefully smaller) symbol.
		 * 
		 * @param symbol the input symbol.
		 * @return the output symbol.
		 */
		public int encode( final int symbol ) {
			final int pos = code2Pos.get( symbol );
			if ( pos == -1 ) {
				final int result = queueSize + symbol;
				newSymbol( symbol );
				return result;
			}
			else {
				if ( DEBUG ) System.err.println( "Symbol " + symbol + " in list; writing " + pos  + " " + code2Pos + " "  + IntArrayList.wrap( queue, queueSize ) + " " + IntArrayList.wrap( freq, queueSize ) );
				oldSymbol( pos );
				return pos;
			}
		}

		/** Decodes a symbol, returning the original symbol.
		 * 
		 * @param symbol a symbol an encoded file.
		 * @return the corresponding original input symbol.
		 */
		public int decode( final int symbol ) {
			
			if ( symbol < queueSize ) {
				final int result = queue[ symbol ];
				oldSymbol( symbol );
				return result;
			}
			else {
				int term = symbol - queueSize;
				newSymbol( term );
				return term;
			}
		}
	}
	
	private SimpleCompressedDocumentCollection( String basename, DocumentFactory factory,
			EliasFanoMonotoneLongBigList docOffsets, EliasFanoMonotoneLongBigList termOffsets, EliasFanoMonotoneLongBigList nonTermOffsets, ByteBufferInputStream documentsByteBufferInputStream,
			ByteBufferInputStream termsByteBufferInputStream, ByteBufferInputStream nonTermsByteBufferInputStream ) {
		this.basename = basename;
		this.documents = docOffsets.size64() - 1;
		this.terms = termOffsets.size64() - 1;
		this.exact = nonTermOffsets != null;
		this.nonTerms = exact ? termOffsets.size64() - 1 : -1;
		this.docOffsets = docOffsets;
		this.termOffsets = termOffsets;
		this.nonTermOffsets = nonTermOffsets;
		this.factory = factory;
		this.termsFrequencyKeeper = new FrequencyCodec();
		this.nonTermsFrequencyKeeper = exact ? new FrequencyCodec() : null;
		this.documentsByteBufferInputStream = documentsByteBufferInputStream;
		this.termsByteBufferInputStream = termsByteBufferInputStream;
		this.nonTermsByteBufferInputStream = nonTermsByteBufferInputStream;
		this.hasNonText = hasNonText( factory );

	}

	protected SimpleCompressedDocumentCollection( final String basename, final long documents, final long terms, final long nonTerms, final boolean exact, final DocumentFactory factory ) {
		this.hasNonText = hasNonText( factory );
		this.basename = basename;
		this.documents = documents;
		this.terms = terms;
		this.nonTerms = nonTerms;
		this.exact = exact;
		this.factory = factory;
		this.termsFrequencyKeeper = null;
		this.nonTermsFrequencyKeeper = null;
		docOffsets = termOffsets = nonTermOffsets = null;
		documentsInputBitStream = null;
		termsInputStream = nonTermsInputStream = null;
		zipFile = null;
		try {
			super.close();
		}
		catch ( IOException cantHappen ) {
			throw new RuntimeException( cantHappen );
		}
	}

	private static boolean hasNonText( final DocumentFactory factory ) {
		boolean hasNonText = false;
		for( int i = factory.numberOfFields(); i-- != 0; ) hasNonText |= factory.fieldType( i ) != FieldType.TEXT;
		return hasNonText;
	}

	private void initMappings( final String basename, final boolean rethrow ) throws IOException {
		try {
			// TODO: This is too risky: we will have to make it optional at some point
			// documentsByteBufferInputStream = ByteBufferInputStream.map( new FileInputStream( basename + DOCUMENTS_EXTENSION ).getChannel(), MapMode.READ_ONLY );
			termsByteBufferInputStream = ByteBufferInputStream.map( new FileInputStream( basename + TERMS_EXTENSION ).getChannel(), MapMode.READ_ONLY );
			nonTermsByteBufferInputStream = nonTermOffsets != null ? ByteBufferInputStream.map( new FileInputStream( basename + NONTERMS_EXTENSION ).getChannel(), MapMode.READ_ONLY ) : null;
			fileMappingOk = true;
		}
		catch( IOException e ) {
			// We leave the possibility for a filename() to fix the problem and map the files.
			if ( rethrow ) throw e;
		}
	}
	
	private void loadOffsets( final String basename, final boolean rethrow ) throws IOException {
		try {
			docOffsets = loadOffsetsSuccinctly( basename + DOCUMENT_OFFSETS_EXTENSION, documents, new File( basename + DOCUMENTS_EXTENSION ).length() * Byte.SIZE + 1 );
			termOffsets = loadOffsetsSuccinctly( basename + TERM_OFFSETS_EXTENSION, terms, new File( basename + TERMS_EXTENSION ).length() + 1 );
			nonTermOffsets = nonTerms < 0 ? null : loadOffsetsSuccinctly( basename + NONTERM_OFFSETS_EXTENSION, nonTerms, new File( basename + NONTERMS_EXTENSION ).length() + 1 );
		}
		catch( IOException e ) {
			// We leave the possibility for a filename() to fix the problem and load the right files.
			if ( rethrow ) throw e;
		}
	}
			
	private void initFiles(  final String basename, final boolean rethrow ) throws IOException {
		try {
			documentsInputBitStream = documentsByteBufferInputStream != null ? new InputBitStream( documentsByteBufferInputStream ) : new InputBitStream( basename + DOCUMENTS_EXTENSION );
			termsInputStream = new FastBufferedInputStream( termsByteBufferInputStream != null ? termsByteBufferInputStream : new FileInputStream( basename + TERMS_EXTENSION ) );
			nonTermsInputStream = exact ? new FastBufferedInputStream( nonTermsByteBufferInputStream != null ? nonTermsByteBufferInputStream : new FileInputStream( basename + NONTERMS_EXTENSION ) ) : null;
			zipFile = hasNonText ? new ZipFile(  basename + ZipDocumentCollection.ZIP_EXTENSION ) : null;
			fileOpenOk = true;
		}
		catch( IOException e ) {
			// We leave the possibility for a filename() to fix the problem and load the right files.
			if ( rethrow ) throw e;
		}
	}
	
	private void ensureFiles() {
		if ( ! fileOpenOk ) throw new IllegalStateException( "Some of the files used by this " + SimpleCompressedDocumentCollection.class.getSimpleName() + " have not been loaded correctly; please use " + AbstractDocumentSequence.class.getName() + ".load() or call filename() after deserialising this instance" );
	}
	
	private static EliasFanoMonotoneLongBigList loadOffsetsSuccinctly( final CharSequence filename, final long numberOfItems, final long upperBound ) throws IOException {
		final InputBitStream ibs = new InputBitStream( filename.toString() );
		final EliasFanoMonotoneLongBigList offsets = new EliasFanoMonotoneLongBigList( numberOfItems + 1, upperBound, new OffsetsLongIterator( ibs, numberOfItems + 1 ) );
		ibs.close();
		return offsets;
	}
		
	@Override
	public void filename( CharSequence filename ) throws IOException {
		if ( ! fileMappingOk ) initMappings( new File( new File( filename.toString() ).getParentFile(), basename ).toString(), true );
		if ( ! fileOpenOk ) {
			loadOffsets( new File( new File( filename.toString() ).getParentFile(), basename ).toString(), true );
			initFiles( new File( new File( filename.toString() ).getParentFile(), basename ).toString(), true );
		}
	}
	
	public DocumentCollection copy() {
		ensureFiles();
		try {
			SimpleCompressedDocumentCollection copy = new SimpleCompressedDocumentCollection( 
					basename,
					factory.copy(),
					docOffsets, 
					termOffsets, 
					nonTermOffsets,
					documentsByteBufferInputStream != null ? documentsByteBufferInputStream.copy() : null,
					termsByteBufferInputStream != null ? termsByteBufferInputStream.copy() : null,
					nonTermsByteBufferInputStream != null ? nonTermsByteBufferInputStream.copy() : null );
			copy.loadOffsets( basename, true );
			copy.initFiles( basename, true );
			return copy;
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}

	private static MutableString readSelfDelimitedUtf8String( final InputBitStream ibs, final MutableString s ) throws IOException {
		s.length( 0 );
		for( int length = ibs.readDelta(); length-- != 0; ) s.append( (char)ibs.readZeta( 7 ) );
		return s;
	}
	
	
	public Document document( long index ) throws IOException {
		ensureDocumentIndex( index );
		ensureFiles();
		documentsInputBitStream.position( docOffsets.getLong( index ) );
		final DataInputStream nonTextDataInputStream = hasNonText ? new DataInputStream( new FastBufferedInputStream( zipFile.getInputStream( zipFile.getEntry( Long.toString( index ) ) ) ) ) : null;
		final MutableString uri = readSelfDelimitedUtf8String( documentsInputBitStream, new MutableString() );
		final MutableString title = readSelfDelimitedUtf8String( documentsInputBitStream, new MutableString() );

		return new AbstractDocument() {
			final MutableString fieldContent = new MutableString();
			
			@SuppressWarnings("unchecked")
			final Document fakeDocument = factory.getDocument( NullInputStream.getInstance(), Reference2ObjectMaps.EMPTY_MAP );
			
			int nextField = 0;

			public Object content( int field ) throws IOException {
				FieldType fieldType = factory.fieldType( field );

				if ( nextField > field ) throw new IllegalStateException();
				// Skip fields
				final MutableString s = new MutableString();
				int len;
				while( nextField < field ) {
					switch( factory.fieldType( nextField ) ) {
					case TEXT:
						len = documentsInputBitStream.readDelta();
						if ( exact ) len *= 2;
						documentsInputBitStream.skipDeltas( len );
						break;
					case VIRTUAL:
						final int nfrag = nonTextDataInputStream.readInt();
						for ( int i = 0; i < 2 * nfrag; i++ ) MutableString.skipSelfDelimUTF8( nonTextDataInputStream );
						break;
					default:
						try { new ObjectInputStream( nonTextDataInputStream ).readObject(); } catch ( ClassNotFoundException e ) { throw new RuntimeException( e ); }
					}
					nextField++;
				}
				
				// Read field
				nextField++;

				switch( fieldType ) {
				case TEXT:
					len = documentsInputBitStream.readDelta();
					fieldContent.length( 0 );

					termsFrequencyKeeper.reset();
					if ( exact ) nonTermsFrequencyKeeper.reset();

					while( len-- != 0 ) {
						termsInputStream.position( termOffsets.getLong( termsFrequencyKeeper.decode( documentsInputBitStream.readDelta() ) ) );
						s.readSelfDelimUTF8( termsInputStream );
						fieldContent.append( s );
						if ( exact ) {
							nonTermsInputStream.position( nonTermOffsets.getLong( nonTermsFrequencyKeeper.decode( documentsInputBitStream.readDelta() ) ) );
							s.readSelfDelimUTF8( nonTermsInputStream );
							fieldContent.append( s );
						}
						else fieldContent.append( ' ');
					}
					return new FastBufferedReader( fieldContent );
				case VIRTUAL:
					final int nfrag = nonTextDataInputStream.readInt();
					MutableString doc = new MutableString();
					MutableString text = new MutableString();
					VirtualDocumentFragment[] fragArray = new VirtualDocumentFragment[ nfrag ];
					for ( int i = 0; i < nfrag; i++ ) {
						doc.readSelfDelimUTF8( (InputStream)nonTextDataInputStream );
						text.readSelfDelimUTF8( (InputStream)nonTextDataInputStream );
						fragArray[ i ] = new AnchorExtractor.Anchor( doc.copy(), text.copy() );
					}
					return new ObjectArrayList<VirtualDocumentFragment>( fragArray );

				default:
					try { return new ObjectInputStream( nonTextDataInputStream ).readObject(); } catch ( ClassNotFoundException e ) { throw new RuntimeException( e ); }
				}
			
				
			}

			public CharSequence title() {
				return title;
			}

			public CharSequence uri() {
				return uri.length() == 0 ? null : uri;
			}

			public WordReader wordReader( int field ) {
				switch( factory.fieldType( field ) ) {
				case TEXT:
				case VIRTUAL: return fakeDocument.wordReader( field );
				default: return null;
				}
			}
			
			public void close() throws IOException {
				super.close();
				if ( hasNonText ) nonTextDataInputStream.close();
			}

		};
	}

	
	public Reference2ObjectMap<Enum<?>,Object> metadata( long index ) throws IOException {
		throw new UnsupportedOperationException();
	}

	
	public long size() {
		return documents;
	}

	
	public InputStream stream( long index ) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	public void close() throws IOException {
		super.close();
		if ( documentsInputBitStream != null ) documentsInputBitStream.close();
		IOUtils.closeQuietly( termsInputStream );
		IOUtils.closeQuietly( nonTermsInputStream );
	}

	public DocumentFactory factory() {
		return factory;
	}

	private void readObject( final ObjectInputStream s ) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		loadOffsets( basename, false );
		initMappings( basename, false );
		initFiles( basename, false );
		termsFrequencyKeeper = new FrequencyCodec();
		if ( exact ) nonTermsFrequencyKeeper = new FrequencyCodec();
	}
		
	
	// Unfinished, experimental method
	@SuppressWarnings("resource")
	public static void optimize( final CharSequence basename ) throws IOException, ClassNotFoundException {
		final SimpleCompressedDocumentCollection collection = (SimpleCompressedDocumentCollection)AbstractDocumentCollection.load( basename );
		final long[] termFrequency = new long[ (int)collection.terms ];
		final long[] nonTermFrequency = collection.exact ? new long[ (int)collection.nonTerms ] : null;
		final InputBitStream documentsIbs = collection.documentsInputBitStream;
		final DocumentFactory factory = collection.factory;
		final boolean exact = collection.exact;
		final MutableString s = new MutableString();
		documentsIbs.position( 0 );
		for( int i = (int)collection.documents; i-- != 0; ) {
			readSelfDelimitedUtf8String( documentsIbs, s ); // Skip URI
			readSelfDelimitedUtf8String( documentsIbs, s ); // Skip title
			for( int f = factory.numberOfFields() - 1; f-- !=0; ) {
				int len = documentsIbs.readDelta();
				while( len-- != 0 ) {
					termFrequency[ documentsIbs.readDelta() ]++;
					if ( exact ) nonTermFrequency[ documentsIbs.readDelta() ]++;
				}
			}
		}
		
		int[] termPerm = new int[ termFrequency.length ];
		for( int i = termPerm.length; i-- != 0; ) termPerm[ i ] = i;
		IntArrays.quickSort( termPerm, 0, termPerm.length, new AbstractIntComparator() {
			private static final long serialVersionUID = 1L;

			public int compare( int arg0, int arg1 ) {
				return termFrequency[ arg1 ] - termFrequency[ arg0 ] < 0 ? -1 : termFrequency[ arg1 ] == termFrequency[ arg0 ] ? 0 : 1;
			}
		});
		
		int[] invTermPerm = new int[ termFrequency.length ];
		for( int i = invTermPerm.length; i-- != 0; ) invTermPerm[ termPerm[ i ] ] = i;
		
		int[] nonTermPerm = null, invNonTermPerm = null;
		if ( exact ) {
			nonTermPerm = new int[ termFrequency.length ];
			for( int i = nonTermPerm.length; i-- != 0; ) nonTermPerm[ i ] = i;
			IntArrays.quickSort( nonTermPerm, 0, nonTermPerm.length, new AbstractIntComparator() {
				private static final long serialVersionUID = 1L;

				public int compare( int arg0, int arg1 ) {
					return termFrequency[ arg1 ] - termFrequency[ arg0 ] < 0 ? -1 : termFrequency[ arg1 ] == termFrequency[ arg0 ] ? 0 : 1;
				}
			});
			invNonTermPerm = new int[ nonTermFrequency.length ];
			for( int i = invNonTermPerm.length; i-- != 0; ) invNonTermPerm[ nonTermPerm[ i ] ] = i;
		}

		File newDocumentsFile = File.createTempFile( SimpleCompressedDocumentCollection.class.getSimpleName(), "temp", new File( basename.toString() ).getParentFile() );
		OutputBitStream newDocumentsObs = new OutputBitStream( newDocumentsFile );
		documentsIbs.position( 0 );
		for( int i = (int)collection.documents; i-- != 0; ) {
			readSelfDelimitedUtf8String( documentsIbs, s ); // Skip URI
			SimpleCompressedDocumentCollectionBuilder.writeSelfDelimitedUtf8String( newDocumentsObs, s );
			readSelfDelimitedUtf8String( documentsIbs, s ); // Skip title
			SimpleCompressedDocumentCollectionBuilder.writeSelfDelimitedUtf8String( newDocumentsObs, s );
			for( int f = factory.numberOfFields() - 1; f-- !=0; ) {
				int len = documentsIbs.readDelta();
				newDocumentsObs.writeDelta( len );
				while( len-- != 0 ) {
					newDocumentsObs.writeDelta( invTermPerm[ documentsIbs.readDelta() ] );
					if ( exact ) newDocumentsObs.writeDelta( invNonTermPerm[ documentsIbs.readDelta() ] );
				}
			}
		}
		newDocumentsObs.close();
		new File( basename + DOCUMENTS_EXTENSION ).delete();
		newDocumentsFile.renameTo( new File( basename + DOCUMENTS_EXTENSION ) );
		newDocumentsObs = null;
		invTermPerm = invNonTermPerm = null;
		
		FastBufferedInputStream termsStream = new FastBufferedInputStream( new FileInputStream( basename + TERMS_EXTENSION ) ) ;
		MutableString term[] = new MutableString[ (int)collection.terms ];
		for( int i = 0; i < term.length; i++ ) term[ i ] = new MutableString().readSelfDelimUTF8( termsStream );
		termsStream.close();

		new FastBufferedOutputStream( new FileOutputStream( basename + TERMS_EXTENSION ) );
	}

	public static void main( final String[] arg ) throws IOException, JSAPException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ConfigurationException, ClassNotFoundException {

		SimpleJSAP jsap = new SimpleJSAP( FileSetDocumentCollection.class.getName(), "Optimises a simple compressed document collection.",
				new Parameter[] {
					new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filename of the collection." ),
				}
		);
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		
		optimize( jsapResult.getString( "basename" ) );
	}

}
