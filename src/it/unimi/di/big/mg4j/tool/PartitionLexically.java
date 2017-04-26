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

import it.unimi.di.big.mg4j.index.BitStreamHPIndex;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndex;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexWriter.LongWordOutputBitStream;
import it.unimi.di.big.mg4j.index.cluster.ContiguousLexicalStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalCluster;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.index.cluster.LexicalCluster;
import it.unimi.di.big.mg4j.index.cluster.LexicalPartitioningStrategy;
import it.unimi.di.big.mg4j.index.cluster.LexicalStrategies;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.BloomFilter;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.util.PrefixMap;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.util.StringMap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.io.IOUtils;
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

/** Partitions an index lexically.
 * 
 * <p>A global index is partitioned lexically by providing a {@link LexicalPartitioningStrategy}
 * that specifies a destination local index for each term, and a local term number. The global index
 * is read directly at the bit level, and the posting lists are divided among the 
 * local indices using the provided strategy. For instance,
 * an {@link ContiguousLexicalStrategy} divides an index into 
 * contiguous blocks (of terms) specified by the given strategy.
 * 
 * <p>By choice, document pointers are not remapped. Thus, it may happen that one of the local indices 
 * contains <em>no</em> posting with a certain document. However, computing the subset of documents contained
 * in each local index to remap them in a contiguous interval is not a good idea, as usually the subset
 * of documents appearing in the postings of each local index is large.
 *
 * <p>To speed up the search of the right local index of a not-so-frequent term (in
 * particular with a {@linkplain it.unimi.di.big.mg4j.index.cluster.ChainedLexicalClusteringStrategy chained strategy}), 
 * after partitioning an index you can create {@linkplain BloomFilter Bloom filters} that will be used to try to avoid
 * inquiring indices that do not contain a term. The filters will be automatically loaded
 * by {@link it.unimi.di.big.mg4j.index.cluster.IndexCluster#getInstance(CharSequence, boolean, boolean)}.
 * 
 * <p>Note that the size file is the same for each local index and <em>is not copied</em>. Please use
 * standard operating system features such as symbolic links to provide size files to 
 * local indices. 
 * 
 * <p>If you plan to {@linkplain LexicalCluster cluster} the partitioned indices and you need document sizes 
 * (e.g., for {@linkplain BM25Scorer BM25 scoring}), you can use the index property 
 * {@link it.unimi.di.big.mg4j.index.Index.UriKeys#SIZES} to load the original size file.  
 * 
 * If you plan on partitioning an index requiring
 * document sizes, you should consider a custom index loading scheme 
 * that shares the {@linkplain it.unimi.di.big.mg4j.index.BitStreamIndex#sizes size list}
 * among all local indices.
 *
 * <strong>Important</strong>: this class just partitions the index. No auxiliary files (most notably, {@linkplain StringMap term maps} 
 * or {@linkplain PrefixMap prefix maps}) will be generated. Please refer to a {@link StringMap} implementation (e.g.,
 * {@link ShiftAddXorSignedStringMap} or {@link ImmutableExternalPrefixMap}).
 *
 * <h2>Write-once output and distributed index partitioning</h2>
 * 
 * <p>The partitioning process writes each index file sequentially exactly once, so index partitioning
 * can output its results to <em>pipes</em>, which in
 * turn can spill their content, for instance, through the network. In other words, albeit this
 * class theoretically creates a number of local indices on disk, those indices can be
 * substituted with suitable pipes creating remote local indices without affecting the partitioning process.
 * For instance, the following <samp>bash</samp> code creates three sets of pipes:
 * <pre style="margin: 1em 0">
 * for i in 0 1 2; do
 *   for e in frequencies occurrencies index offsets properties sizes terms; do 
 *     mkfifo pipe-$i.$e
 *   done
 * done
 * </pre> 
 * 
 * <p>Each pipe must be emptied elsewhere, for instance (assuming
 * you want local indices <samp>index0</samp>, <samp>index1</samp> and <samp>index2</samp> on <samp>example.com</samp>):
 * <pre style="margin: 1em 0">
 * for i in 0 1 2; do 
 *   for e in frequencies occurrencies index offsets properties sizes terms; do 
 *     (cat pipe-$i.$e | ssh -x example.com "cat >index-$i.$e" &)
 *   done
 * done
 * </pre> 
 * <p>If we now start a partitioning process generating three local indices named <samp>pipe-0</samp>,
 * <samp>pipe-1</samp> and <samp>pipe-2</samp>
 * all pipes will be written to by the process, and the data will create remotely
 * indices <samp>index-0</samp>, <samp>index-1</samp> and <samp>index-2</samp>.
 *
 * @author Sebastiano Vigna
 * 
 * @since 1.0.1
 */

public class PartitionLexically {
	private static final Logger LOGGER = LoggerFactory.getLogger( PartitionLexically.class );

	/**  The default buffer size for all involved indices. */
	public final static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	
	protected final static class LongWordInputBitStream {
		/** The underlying channel. */
		private final FileChannel fileChannel;
		/** The buffer. */
		private final ByteBuffer byteBuffer;
		/** The 64-bit buffer, whose lower {@link #filled} bits contain data. */
		private long buffer;
		/** The number of lower used bits {@link #buffer} (less than {@link Long#SIZE}). */
		private int filled;

		@SuppressWarnings("resource")
		public LongWordInputBitStream( final String filename, final int bufferSize, final ByteOrder byteOrder ) throws FileNotFoundException {
			fileChannel = new FileInputStream( filename ).getChannel();
			byteBuffer = ByteBuffer.allocateDirect( bufferSize ).order( byteOrder );
			byteBuffer.flip();
		}

		public long extract( final int width ) throws IOException {
			if ( width <= filled ) {
				long result = buffer & ( 1L << width ) - 1;
				filled -= width;
				buffer >>>= width;
				return result;
			}
			else {
				if ( ! byteBuffer.hasRemaining() ) {
					byteBuffer.clear();
					fileChannel.read( byteBuffer );
					byteBuffer.flip();
				}
				
				if ( filled == 0 && width == Long.SIZE ) return byteBuffer.getLong();
				
				long result = buffer;
				final int remainder = width - filled;
				buffer = byteBuffer.getLong();
				result |= ( buffer & ( 1L << remainder ) - 1 ) << filled;
				buffer >>>= remainder;
				filled = Long.SIZE - remainder;
				return result;
			}
		}
		
		public void close() throws IOException {
			fileChannel.close();
		}
		
	}

	
	/** The number of local indices. */
	private final int numIndices;
	/** The output basenames. */
	private final String outputBasename;
	/** The array of local output basenames. */
	private final String[] localBasename;
	/** The input basename. */
	private final String inputBasename;
	/** The size of I/O buffers. */
	private final int bufferSize;
	/** The filename of the strategy used to partition the index. */
	private final String strategyFilename;
	/** The strategy used to partition the index. */
	private final LexicalPartitioningStrategy strategy;
	/** The additional local properties of each local index. */
	private final Properties[] strategyProperties;
	/** The logging interval. */
	private final long logInterval;
	
	public PartitionLexically( final String inputBasename, 
			final String outputBasename,
			final LexicalPartitioningStrategy strategy,
			final String strategyFilename,
			final int bufferSize,
			final long logInterval ) {

		this.inputBasename = inputBasename;
		this.outputBasename = outputBasename;
		this.strategy = strategy;
		this.strategyFilename = strategyFilename;
		this.bufferSize = bufferSize & ~( Long.SIZE - 1 );
		this.logInterval = logInterval;
		numIndices = strategy.numberOfLocalIndices();
		strategyProperties = strategy.properties();
		localBasename = new String[ numIndices ];
		for( int i = 0; i < numIndices; i++ ) localBasename[ i ] = outputBasename + "-" + i;
	}
	
	public void runTermsOnly() throws IOException {
		final ProgressLogger pl = new ProgressLogger( LOGGER, logInterval, TimeUnit.MILLISECONDS );
		
		final PrintWriter[] localTerms = new PrintWriter[ numIndices ]; 
		final long numTerms[] = new long[ numIndices ];
		@SuppressWarnings("resource")
		final FastBufferedReader terms = new FastBufferedReader( new InputStreamReader( new FileInputStream( inputBasename + DiskBasedIndex.TERMS_EXTENSION ), "UTF-8" ) );
		
		for( int i = 0; i < numIndices; i++ ) localTerms[ i ] = new PrintWriter( new OutputStreamWriter( new FastBufferedOutputStream( new FileOutputStream( localBasename[ i ] + DiskBasedIndex.TERMS_EXTENSION ) ), "UTF-8" ) );

		// The current term
		final MutableString currTerm = new MutableString();
		
		pl.itemsName = "terms";
		pl.logInterval = logInterval;
		pl.start( "Partitioning index terms..." );

		long termNumber = 0;
		int k;
		
		while( terms.readLine( currTerm ) != null ) {
			k = strategy.localIndex( termNumber ); // The local index for this term
			if ( numTerms[ k ] != strategy.localNumber( termNumber ) ) throw new IllegalStateException();
			numTerms[ k ]++;
			currTerm.println( localTerms[ k ] );
			pl.update();
			termNumber++;
		}

		terms.close();
		for( int i = 0; i < numIndices; i++ ) localTerms[ i ].close();

		pl.done();
	}
	
	@SuppressWarnings("resource")
	public void run() throws ConfigurationException, IOException, ClassNotFoundException {
		final ProgressLogger pl = new ProgressLogger( LOGGER, logInterval, TimeUnit.MILLISECONDS );
		final byte[] buffer = new byte[ bufferSize ];

		final Properties properties = new Properties( inputBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
		final long numberOfTerms = properties.getLong( Index.PropertyKeys.TERMS );
		final Class<?> indexClass = Class.forName( properties.getString( Index.PropertyKeys.INDEXCLASS ) );
		final boolean isHighPerformance = BitStreamHPIndex.class.isAssignableFrom( indexClass );
		final boolean isQuasiSuccinct = QuasiSuccinctIndex.class.isAssignableFrom( indexClass );
		final ByteOrder byteOrder = isQuasiSuccinct ? DiskBasedIndex.byteOrder( properties.getString( QuasiSuccinctIndex.PropertyKeys.BYTEORDER ) ) : null; 
		
		final OutputBitStream[] localIndex = new OutputBitStream[ numIndices ];
		final OutputBitStream[] localPositions = new OutputBitStream[ numIndices ];		
		final OutputBitStream[] localOffsets = new OutputBitStream[ numIndices ];
		
		final LongWordOutputBitStream[] localQSPointers = isQuasiSuccinct ? new LongWordOutputBitStream[ numIndices ] : null;
		final LongWordOutputBitStream[] localQSCounts = isQuasiSuccinct ? new LongWordOutputBitStream[ numIndices ] : null;
		final LongWordOutputBitStream[] localQSPositions = isQuasiSuccinct ? new LongWordOutputBitStream[ numIndices ] : null;
		final OutputBitStream[] localCountsOffsets = isQuasiSuccinct ? new OutputBitStream[ numIndices ] : null;
		final OutputBitStream[] localPositionsOffsets = isQuasiSuccinct ? new OutputBitStream[ numIndices ] : null;

		final OutputBitStream[] localPosNumBits = new OutputBitStream[ numIndices ];
		final OutputBitStream[] localSumsMaxPos = new OutputBitStream[ numIndices ];
		final OutputBitStream[] localFrequencies = new OutputBitStream[ numIndices ];
		final OutputBitStream[] localOccurrencies = new OutputBitStream[ numIndices ];
		final PrintWriter[] localTerms = new PrintWriter[ numIndices ]; 
		final long numTerms[] = new long[ numIndices ];
		final long localOccurrences[] = new long[ numIndices ];
		final long numberOfPostings[] = new long[ numIndices ];
		
		final InputBitStream globalIndex = isQuasiSuccinct ? null : new InputBitStream( inputBasename + DiskBasedIndex.INDEX_EXTENSION, bufferSize );
		final long globalPositionsLength = new File( inputBasename + DiskBasedIndex.POSITIONS_EXTENSION ).length();
		final InputBitStream globalPositions = isHighPerformance ? new InputBitStream( inputBasename + DiskBasedIndex.POSITIONS_EXTENSION, bufferSize ) : null;
		final FastBufferedReader terms = new FastBufferedReader( new InputStreamReader( new FileInputStream( inputBasename + DiskBasedIndex.TERMS_EXTENSION ), "UTF-8" ) );
		final InputBitStream globalOffsets = new InputBitStream( inputBasename + ( isQuasiSuccinct ? DiskBasedIndex.POINTERS_EXTENSIONS + DiskBasedIndex.OFFSETS_POSTFIX : DiskBasedIndex.OFFSETS_EXTENSION ) );
		final InputBitStream globalPositionsOffsets = isQuasiSuccinct ? new InputBitStream( inputBasename + DiskBasedIndex.POSITIONS_EXTENSION + DiskBasedIndex.OFFSETS_POSTFIX, bufferSize ) : null;
		final InputBitStream globalCountsOffsets = isQuasiSuccinct ? new InputBitStream( inputBasename + DiskBasedIndex.COUNTS_EXTENSION + DiskBasedIndex.OFFSETS_POSTFIX, bufferSize ) : null;
		
		final LongWordInputBitStream globalPointersIbs = isQuasiSuccinct ? new LongWordInputBitStream( inputBasename + DiskBasedIndex.POINTERS_EXTENSIONS, bufferSize, byteOrder ) : null;
		final LongWordInputBitStream globalCountsIbs = isQuasiSuccinct ? new LongWordInputBitStream( inputBasename + DiskBasedIndex.COUNTS_EXTENSION, bufferSize, byteOrder ) : null;
		final LongWordInputBitStream globalPositionsIbs = isQuasiSuccinct ? new LongWordInputBitStream( inputBasename + DiskBasedIndex.POSITIONS_EXTENSION, bufferSize, byteOrder ) : null;
		
		final File posNumBitsFile = new File( inputBasename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION );
		final InputBitStream posNumBits = posNumBitsFile.exists() ? new InputBitStream( inputBasename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION ) : null;
		final File sumsMaxPosFile = new File( inputBasename + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION );
		final InputBitStream sumsMaxPos = sumsMaxPosFile.exists() ? new InputBitStream( sumsMaxPosFile ) : null;
		final InputBitStream frequencies = new InputBitStream( inputBasename + DiskBasedIndex.FREQUENCIES_EXTENSION );
		final InputBitStream occurrencies = new InputBitStream( inputBasename + DiskBasedIndex.OCCURRENCIES_EXTENSION );
		globalOffsets.readGamma();
		if ( globalCountsOffsets != null ) globalCountsOffsets.readGamma();
		if ( globalPositionsOffsets != null ) globalPositionsOffsets.readGamma();
		
		for( int i = 0; i < numIndices; i++ ) {
			if ( ! isQuasiSuccinct ) localIndex[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.INDEX_EXTENSION, bufferSize );
			if ( isHighPerformance ) localPositions[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.POSITIONS_EXTENSION, bufferSize );
			if ( isQuasiSuccinct ) {
				localQSPointers[ i ] = new LongWordOutputBitStream( new FileOutputStream( localBasename[ i ] + DiskBasedIndex.POINTERS_EXTENSIONS ).getChannel(), byteOrder );
				localQSCounts[ i ] = new LongWordOutputBitStream( new FileOutputStream( localBasename[ i ] + DiskBasedIndex.COUNTS_EXTENSION ).getChannel(), byteOrder );
				localQSPositions[ i ] = new LongWordOutputBitStream( new FileOutputStream( localBasename[ i ] + DiskBasedIndex.POSITIONS_EXTENSION ).getChannel(), byteOrder );
			}
			localFrequencies[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.FREQUENCIES_EXTENSION );
			localOccurrencies[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.OCCURRENCIES_EXTENSION );
			localTerms[ i ] = new PrintWriter( new OutputStreamWriter( new FastBufferedOutputStream( new FileOutputStream( localBasename[ i ] + DiskBasedIndex.TERMS_EXTENSION ) ), "UTF-8" ) );
			localOffsets[ i ] = new OutputBitStream( localBasename[ i ] + ( isQuasiSuccinct ? DiskBasedIndex.POINTERS_EXTENSIONS + DiskBasedIndex.OFFSETS_POSTFIX : DiskBasedIndex.OFFSETS_EXTENSION ) );
			if ( posNumBits != null ) localPosNumBits[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION );
			if ( sumsMaxPos != null ) localSumsMaxPos[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION );
			localOffsets[ i ].writeGamma( 0 );
			if ( isQuasiSuccinct ) {
				localCountsOffsets[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.COUNTS_EXTENSION + DiskBasedIndex.OFFSETS_POSTFIX );
				localPositionsOffsets[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.POSITIONS_EXTENSION + DiskBasedIndex.OFFSETS_POSTFIX );
				localCountsOffsets[ i ].writeGamma( 0 );
				localPositionsOffsets[ i ].writeGamma( 0 );
			}
		}

		// The current term
		final MutableString currTerm = new MutableString();
		
		pl.expectedUpdates = numberOfTerms;
		pl.itemsName = "bits";
		pl.logInterval = logInterval;
		pl.start( "Partitioning index..." );

		long termNumber = 0;
		int k, prevK = -1;
		long previousHeaderLength = 0, newHeaderLength = 0;
		long length, positionsOffset = 0;
		
		while( terms.readLine( currTerm ) != null ) {
			k = strategy.localIndex( termNumber ); // The local index for this term
			if ( numTerms[ k ] != strategy.localNumber( termNumber ) ) throw new IllegalStateException();
			numTerms[ k ]++;
			
			if ( isHighPerformance ) {
				final long temp = globalIndex.readBits();
				positionsOffset = globalIndex.readLongDelta();
				previousHeaderLength = (int)( globalIndex.readBits() - temp );
				if ( prevK != -1 ) {
					length = positionsOffset - globalPositions.readBits();
					copy( buffer, globalPositions, localPositions[ prevK ], length );
				}
				newHeaderLength = localIndex[ k ].writeLongDelta( localPositions[ k ].writtenBits() );
			}
			
			final long frequency = frequencies.readLongGamma();
			localFrequencies[ k ].writeLongGamma( frequency );
			numberOfPostings[ k ] += frequency;

			if ( posNumBits != null ) localPosNumBits[ k ].writeLongGamma( posNumBits.readLongGamma() );
			if ( sumsMaxPos != null ) localSumsMaxPos[ k ].writeLongDelta( sumsMaxPos.readLongDelta() );
			
			final long occurrency = occurrencies.readLongGamma();
			localOccurrences[ k ] += occurrency;
			localOccurrencies[ k ].writeLongGamma( occurrency );
			
			currTerm.println( localTerms[ k ] );
			
			if ( isQuasiSuccinct ) {
				localOffsets[ k ].writeLongGamma( length = globalOffsets.readLongGamma() );
				copy( globalPointersIbs, localQSPointers[ k ], length );
				localCountsOffsets[ k ].writeLongGamma( length = globalCountsOffsets.readLongGamma() );
				copy( globalCountsIbs, localQSCounts[ k ], length );
				localPositionsOffsets[ k ].writeLongGamma( length = globalPositionsOffsets.readLongGamma() );
				copy( globalPositionsIbs, localQSPositions[ k ], length );
			}
			else {
				length = globalOffsets.readLongGamma() - previousHeaderLength;
				localOffsets[ k ].writeLongGamma( length + newHeaderLength );

				copy( buffer, globalIndex, localIndex[ k ], length );
			}
			
			pl.update();
			prevK = k;
			termNumber++;
		}

		// We pour the last piece of positions
		if ( isHighPerformance ) {
			if ( prevK != -1 ) {
				length = globalPositionsLength * 8 - globalPositions.readBits();
				copy( buffer, globalPositions, localPositions[ prevK ], length );
			}
		}

		pl.done();

		terms.close();
		globalOffsets.close();
		if ( globalIndex != null ) globalIndex.close();
		if ( globalPointersIbs != null ) globalPointersIbs.close();
		if ( globalCountsIbs != null ) globalCountsIbs.close();
		if ( globalCountsOffsets != null ) globalCountsOffsets.close();
		if ( globalPositionsIbs != null ) globalPositionsIbs.close();
		if ( globalPositionsOffsets != null ) globalPositionsOffsets.close();

		if ( globalPositions != null ) globalPositions.close();
		frequencies.close();
		occurrencies.close();
		if ( posNumBits != null ) posNumBits.close();
		if ( sumsMaxPos != null ) sumsMaxPos.close();
		if ( isHighPerformance ) globalPositions.close();
		
		// We copy the relevant properties from the original 
		Properties globalProperties = new Properties();
		if ( strategyFilename != null ) globalProperties.setProperty( IndexCluster.PropertyKeys.STRATEGY, strategyFilename );
		globalProperties.setProperty( DocumentalCluster.PropertyKeys.BLOOM, false );
		globalProperties.setProperty( Index.PropertyKeys.INDEXCLASS, LexicalCluster.class.getName() );
		for( int i = 0; i < numIndices; i++ ) globalProperties.addProperty( IndexCluster.PropertyKeys.LOCALINDEX, localBasename[ i ] );
		globalProperties.setProperty( Index.PropertyKeys.FIELD, properties.getProperty( Index.PropertyKeys.FIELD ) );
		globalProperties.setProperty( Index.PropertyKeys.POSTINGS, properties.getProperty( Index.PropertyKeys.POSTINGS ) );
		globalProperties.setProperty( Index.PropertyKeys.OCCURRENCES, properties.getProperty( Index.PropertyKeys.OCCURRENCES ) );
		globalProperties.setProperty( Index.PropertyKeys.DOCUMENTS, properties.getProperty( Index.PropertyKeys.DOCUMENTS ) );
		globalProperties.setProperty( Index.PropertyKeys.TERMS, properties.getProperty( Index.PropertyKeys.TERMS ) );
		globalProperties.setProperty( Index.PropertyKeys.TERMPROCESSOR, properties.getProperty( Index.PropertyKeys.TERMPROCESSOR ) );
		globalProperties.setProperty( Index.PropertyKeys.MAXCOUNT, properties.getProperty( Index.PropertyKeys.MAXCOUNT ) );
		globalProperties.setProperty( Index.PropertyKeys.MAXDOCSIZE, properties.getProperty( Index.PropertyKeys.MAXDOCSIZE ) );
		globalProperties.save( outputBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
		LOGGER.debug( "Properties for clustered index " + outputBasename + ": " + new ConfigurationMap( globalProperties ) );
		
		for( int i = 0; i < numIndices; i++ ) {
			if ( isQuasiSuccinct ) {
				localQSPointers[ i ].close();
				if ( localQSCounts != null ) {
					localCountsOffsets[ i ].close();
					localQSCounts[ i ].close();
				}
				if ( localQSPositions != null ) {
					localPositionsOffsets[ i ].close();
					localQSPositions[ i ].close();
				}
			}
			else {
				localIndex[ i ].close();
				if ( isHighPerformance ) localPositions[ i ].close();
			}
			
			localOffsets[ i ].close();

			if ( posNumBits != null ) localPosNumBits[ i ].close();
			if ( sumsMaxPos != null ) localSumsMaxPos[ i ].close();

			localFrequencies[ i ].close();
			localOccurrencies[ i ].close();
			localTerms[ i ].close();
			final InputStream input = new FileInputStream( inputBasename + DiskBasedIndex.SIZES_EXTENSION );
			final OutputStream output = new FileOutputStream( localBasename[ i ] + DiskBasedIndex.SIZES_EXTENSION );
			IOUtils.copy( input, output );
			input.close();
			output.close();
			Properties localProperties = new Properties();
			localProperties.addAll( globalProperties );
			localProperties.setProperty( Index.PropertyKeys.TERMS, numTerms[ i ] );
			localProperties.setProperty( Index.PropertyKeys.OCCURRENCES, localOccurrences[ i ] );
			localProperties.setProperty( Index.PropertyKeys.POSTINGS, numberOfPostings[ i ] );
			localProperties.setProperty( Index.PropertyKeys.POSTINGS, numberOfPostings[ i ] );
			localProperties.setProperty( Index.PropertyKeys.INDEXCLASS, properties.getProperty( Index.PropertyKeys.INDEXCLASS ) );
			localProperties.setProperty( QuasiSuccinctIndex.PropertyKeys.BYTEORDER, properties.getProperty( QuasiSuccinctIndex.PropertyKeys.BYTEORDER ) );
			localProperties.addProperties( Index.PropertyKeys.CODING, properties.getStringArray( Index.PropertyKeys.CODING ) );
			localProperties.setProperty( BitStreamIndex.PropertyKeys.SKIPQUANTUM, properties.getProperty( BitStreamIndex.PropertyKeys.SKIPQUANTUM ) );
			localProperties.setProperty( BitStreamIndex.PropertyKeys.SKIPHEIGHT, properties.getProperty( BitStreamIndex.PropertyKeys.SKIPHEIGHT ) );
			if ( strategyProperties != null && strategyProperties[ i ] != null ) localProperties.addAll( strategyProperties[ i ] );
			localProperties.save( localBasename[ i ] + DiskBasedIndex.PROPERTIES_EXTENSION );
			LOGGER.debug( "Post-partitioning properties for index " + localBasename[ i ] + ": " + new ConfigurationMap( localProperties ) );
		}
	}

	private static void copy( final byte[] buffer, final InputBitStream from, final OutputBitStream to, long length ) throws IOException {
		int res;
		final int bufferSize = buffer.length;
		while( length > 0 ) {
			res = (int)Math.min( bufferSize * 8, length );
			from.read( buffer, res );
			to.write( buffer, res );
			length -= res;
		}
	}

	private static void copy( final LongWordInputBitStream from, final LongWordOutputBitStream to, long length ) throws IOException {
		while( length > 0 ) {
			final int width = (int)Math.min( Long.SIZE, length );
			to.append( from.extract( width ), width );
			length -= width;
		}
	}

	public static void main( final String[] arg ) throws JSAPException, ConfigurationException, IOException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException {
		
		SimpleJSAP jsap = new SimpleJSAP( PartitionLexically.class.getName(), "Partitions an index lexically.",
				new Parameter[] {
				new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer." ),
				new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
				new FlaggedOption( "strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "strategy", "A serialised lexical partitioning strategy." ),
				new FlaggedOption( "uniformStrategy", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'u', "uniform", "Requires a uniform partitioning in the given number of parts." ),
				new Switch( "termsOnly", 't', "terms-only", "Just partition the term list." ),
				new UnflaggedOption( "inputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the global index." ),
				new UnflaggedOption( "outputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the local indices." )
		});
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		String inputBasename = jsapResult.getString( "inputBasename" );
		String outputBasename = jsapResult.getString( "outputBasename" );
		String strategyFilename = jsapResult.getString( "strategy" );
		LexicalPartitioningStrategy strategy = null;

		if ( jsapResult.userSpecified( "uniformStrategy" ) ) {
			strategy = LexicalStrategies.uniform( jsapResult.getInt( "uniformStrategy" ), DiskBasedIndex.getInstance( inputBasename, false, false, true ) );
			BinIO.storeObject( strategy, strategyFilename = outputBasename + IndexCluster.STRATEGY_DEFAULT_EXTENSION );
		}
		else if ( strategyFilename != null ) strategy = (LexicalPartitioningStrategy)BinIO.loadObject( strategyFilename );
		else throw new IllegalArgumentException( "You must specify a splitting strategy" );

		final PartitionLexically partitionLexically = new PartitionLexically( inputBasename,
				outputBasename, 
				strategy, 
				strategyFilename,
				jsapResult.getInt( "bufferSize" ),
				jsapResult.getLong( "logInterval" ) );

		
		if ( jsapResult.getBoolean( "termsOnly" ) ) partitionLexically.runTermsOnly();
		else partitionLexically.run();
	}
}
