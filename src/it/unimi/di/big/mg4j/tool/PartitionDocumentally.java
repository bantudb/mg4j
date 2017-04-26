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

import it.unimi.di.big.mg4j.index.BitStreamHPIndexWriter;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.BitStreamIndexWriter;
import it.unimi.di.big.mg4j.index.CachingOutputBitStream;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.IndexWriter;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndex;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexWriter;
import it.unimi.di.big.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalCluster;
import it.unimi.di.big.mg4j.index.cluster.DocumentalConcatenatedCluster;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.di.big.mg4j.index.cluster.DocumentalPartitioningStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalStrategies;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.dsi.Util;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.io.BinIO;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;


/** Partitions an index documentally.
 *
 * <p>A global index is partitioned documentally by providing a {@link DocumentalPartitioningStrategy}
 * that specifies a destination local index for each document, and a local document pointer. The global index
 * is scanned, and the postings are partitioned among the local indices using the provided strategy. For instance,
 * a {@link ContiguousDocumentalStrategy} divides an index into blocks of contiguous documents.
 * 
 * <p>Since each local index contains a (proper) subset of the original set of documents, it contains in general a (proper)
 * subset of the terms in the global index. Thus, the local term numbers and the global term numbers will not in general coincide.
 * As a result, when a set of local indices is accessed transparently as a single index
 * using a {@link it.unimi.di.big.mg4j.index.cluster.DocumentalCluster}, 
 * a call to {@link it.unimi.di.big.mg4j.index.Index#documents(long)} will throw an {@link java.lang.UnsupportedOperationException},
 * because there is no way to map the global term numbers to local term numbers.
 * 
 * <p>On the other hand, a call to {@link it.unimi.di.big.mg4j.index.Index#documents(CharSequence)} will be passed each local index to
 * build a global iterator. To speed up this phase for not-so-frequent terms, when partitioning an index you can require
 * the construction of {@linkplain BloomFilter Bloom filters} that will be used to try to avoid
 * inquiring indices that do not contain a term. The precision of the filters is settable.
 *
 * <p>The property file will use a {@link it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster} unless you provide
 * a {@link ContiguousDocumentalStrategy}, in which case a 
 * {@link it.unimi.di.big.mg4j.index.cluster.DocumentalConcatenatedCluster} will be used instead. Note that there might
 * be other cases in which the latter is adapt, in which case you can edit manually the property file.
 *
 * <p><strong>Important</strong>: this class just partitions the index. No auxiliary files (most notably, {@linkplain StringMap term maps} 
 * or {@linkplain PrefixMap prefix maps}) will be generated. Please refer to a {@link StringMap} implementation (e.g.,
 * {@link ShiftAddXorSignedStringMap} or {@link ImmutableExternalPrefixMap}).
 * 
 * <p><strong>Warning</strong>: variable quanta are not supported by this class, as it is impossible to predict accurately
 * the number of bits used for positions when partitioning documentally. If you want to use variable quanta, use a
 * simple interleaved index without skips as an intermediate step, and pass it through {@link Combine}.
 * 
 * <h2>Sizes</h2>
 * 
 * <p>Partitioning the file containing document sizes is a tricky issue. For the time being this class
 * implements a very simple policy: if {@link DocumentalPartitioningStrategy#numberOfDocuments(int)} returns the number of
 * documents of the global index, the size file for a local index is generated by replacing all sizes of documents not
 * belonging to the index with a zero. Otherwise, the file is generated by appending in order the sizes of the documents
 * belonging to the index. This simple strategy works well with contiguous splitting and with splittings that do not
 * change the document numbers (e.g., the inverse operation of a {@link Merge}). However, more complex splittings might give rise
 * to inconsistent size files.  
 * 
 * <h2>Write-once output and distributed index partitioning</h2>
 * 
 * Please see {@link it.unimi.di.big.mg4j.tool.PartitionLexically}&mdash;the same comments apply.
 * 
 * @author Alessandro Arrabito
 * @author Sebastiano Vigna
 * 
 * @since 1.0.1
 */

public class PartitionDocumentally {
	private final static Logger LOGGER = LoggerFactory.getLogger( PartitionDocumentally.class );

	/**  The default buffer size for all involved indices. */
	public final static int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	
	/** The number of local indices. */
	private final int numIndices;
	/** The output basenames. */
	private final String outputBasename;
	/** The array of local output basenames. */
	private final String[] localBasename;
	/** The input basename. */
	private final String inputBasename;
	/** The properties of the input index. */
	private final Properties inputProperties;
	/** The size of I/O buffers. */
	private final int bufferSize;
	/** The filename of the strategy used to partition the index. */
	private final String strategyFilename;
	/** The strategy used to perform the partitioning. */
	private final DocumentalPartitioningStrategy strategy;
	/** The additional local properties of each local index. */
	private final Properties[] strategyProperties;
	/** The logging interval. */
	private final long logInterval;
	/** The global index to be partitioned. */
	private final Index globalIndex;
	/** A reader on {@link #globalIndex}. */
	private final IndexReader indexReader;
	/** A reader for the terms of the global index. */
	private final FastBufferedReader terms;
	/** An index writer for each local index. */
	private final IndexWriter[] indexWriter;
	/** A copy of {@link #indexWriter} which is non-<code>null</code> if {@link #indexWriter} is an instance of {@link QuasiSuccinctIndexWriter}[]. */ 
	private QuasiSuccinctIndexWriter[] quasiSuccinctIndexWriter;
	/** Whether each {@link #indexWriter} has counts. */
	private final boolean haveCounts;
	/** Whether each {@link #indexWriter} has positions. */
	private final boolean havePositions;
	/** Whether each {@link #indexWriter} has payloads. */
	private final boolean havePayloads;
	/** A print writer for the terms of each local index. */
	private final PrintWriter[] localTerms;
	/** The maximum size of a document in each local index. */
	private final int[] maxDocSize;
	/** The maximum number of positions in each local index. */
	private final int[] maxDocPos;
	/** The number of terms in each local index. */
	private final long[] numTerms;
	/** The number of postings in each local index. */
	private final long[] numPostings;
	/** The number of occurrences in each local index. */
	private final long[] numOccurrences;
	/** The global count for each local index. */
	private final long[] occurrencies;
	/** The required precision for Bloom filters (0 means no filter). */
	private final int bloomFilterPrecision;

	
	
	
	public PartitionDocumentally( final String inputBasename, 
			final String outputBasename,
			final DocumentalPartitioningStrategy strategy,
			final String strategyFilename,
			final int BloomFilterPrecision,
			final int bufferSize,
			final Map<Component,Coding> writerFlags,
			IndexType indexType,
			boolean skips,
			final int quantum,
			final int height,
			final int skipBufferOrCacheSize,
			final long logInterval ) throws ConfigurationException, IOException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, URISyntaxException, InvocationTargetException, NoSuchMethodException {

		this.inputBasename = inputBasename;
		this.outputBasename = outputBasename;
		this.strategy = strategy;
		this.strategyFilename = strategyFilename;
		this.strategyProperties = strategy.properties();
		this.bufferSize = bufferSize;
		this.logInterval = logInterval;
		this.bloomFilterPrecision = BloomFilterPrecision;

		numIndices = strategy.numberOfLocalIndices();

		final Coding positionCoding = writerFlags.get( Component.POSITIONS );

		inputProperties = new Properties( inputBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
		globalIndex = Index.getInstance( inputBasename, false, positionCoding == Coding.GOLOMB || positionCoding == Coding.INTERPOLATIVE, false );
		indexReader = globalIndex.getReader();

		localBasename = new String[ numIndices ];
		for( int i = 0; i < numIndices; i++ ) localBasename[ i ] = outputBasename + "-" + i;

		localTerms = new PrintWriter[ numIndices ];
		maxDocSize = new int[ numIndices ];
		maxDocPos = new int[ numIndices ];
		numTerms = new long[ numIndices ];
		occurrencies = new long[ numIndices ];
		numOccurrences = new long[ numIndices ];
		numPostings = new long[ numIndices ];
		indexWriter = new IndexWriter[ numIndices ];
		quasiSuccinctIndexWriter = new QuasiSuccinctIndexWriter[ numIndices ];
		
		if ( ( havePayloads = writerFlags.containsKey( Component.PAYLOADS ) ) && ! globalIndex.hasPayloads ) 
			throw new IllegalArgumentException( "You requested payloads, but the global index does not contain them." );
		if ( ( haveCounts = writerFlags.containsKey( Component.COUNTS ) ) && ! globalIndex.hasCounts ) 
			throw new IllegalArgumentException( "You requested counts, but the global index does not contain them." );
		if ( ( havePositions = writerFlags.containsKey( Component.POSITIONS ) ) && !  globalIndex.hasPositions ) 
			throw new IllegalArgumentException( "You requested positions, but the global index does not contain them." );

		if ( indexType == IndexType.HIGH_PERFORMANCE && ! havePositions ) throw new IllegalArgumentException( "You cannot disable positions for high-performance indices." );
		if ( indexType != IndexType.INTERLEAVED && havePayloads ) throw new IllegalArgumentException( "Payloads are available in interleaved indices only." );
		skips |= indexType == IndexType.HIGH_PERFORMANCE;
		if ( skips && ( quantum <= 0 || height < 0 ) ) throw new IllegalArgumentException( "You must specify a positive quantum and a nonnegative height (variable quanta are not available when partitioning documentally)." );
		
		for ( int i = 0; i < numIndices; i++ ) {
			switch( indexType ) {
			case INTERLEAVED:
				if ( ! skips ) indexWriter[ i ] = new BitStreamIndexWriter( IOFactory.FILESYSTEM_FACTORY, localBasename[ i ], strategy.numberOfDocuments( i ), true, writerFlags );
				else indexWriter[ i ] = new SkipBitStreamIndexWriter( IOFactory.FILESYSTEM_FACTORY, localBasename[ i ], strategy.numberOfDocuments( i ), true, skipBufferOrCacheSize, writerFlags, quantum, height );
				break;
			case HIGH_PERFORMANCE:
				indexWriter[ i ] = new BitStreamHPIndexWriter( localBasename[ i ], strategy.numberOfDocuments( i ), true, skipBufferOrCacheSize, writerFlags, quantum, height );
				break;
			case QUASI_SUCCINCT:
				quasiSuccinctIndexWriter[ i ] = (QuasiSuccinctIndexWriter)( indexWriter[ i ] = new QuasiSuccinctIndexWriter( IOFactory.FILESYSTEM_FACTORY, localBasename[ i ], strategy.numberOfDocuments( i ), Fast.mostSignificantBit( quantum < 0 ? QuasiSuccinctIndex.DEFAULT_QUANTUM : quantum ), skipBufferOrCacheSize, writerFlags, ByteOrder.nativeOrder() ) );
			}
			localTerms[ i ] = new PrintWriter( new BufferedWriter( new OutputStreamWriter( new FileOutputStream( localBasename[ i ] + DiskBasedIndex.TERMS_EXTENSION ), "UTF-8" ) ) );			
		}
		
		terms = new FastBufferedReader( new InputStreamReader( new FileInputStream( inputBasename + DiskBasedIndex.TERMS_EXTENSION ), "UTF-8" ) );
	}
	
	private void partitionSizes() throws IOException {			
		final File sizesFile = new File( inputBasename + DiskBasedIndex.SIZES_EXTENSION );
		if ( sizesFile.exists() ) {
			LOGGER.info( "Partitioning sizes..." );
			final InputBitStream sizes = new InputBitStream ( sizesFile );
			final OutputBitStream localSizes[] = new OutputBitStream[ numIndices ];
			for ( int i = 0; i < numIndices; i++ ) localSizes[ i ] = new OutputBitStream( localBasename[ i ] + DiskBasedIndex.SIZES_EXTENSION );

			// ALERT: for the time being, we decide whether to "fill the gaps" in sizes using as sole indicator the equality between global and local number of documents.
			int size, localIndex;
			if ( globalIndex.numberOfDocuments == strategy.numberOfDocuments( 0 ) ) {
				for( int i = 0; i < globalIndex.numberOfDocuments; i++ ) {
					localSizes[ localIndex = strategy.localIndex( i ) ].writeGamma( size = sizes.readGamma() );
					if ( maxDocSize[ localIndex ] < size ) maxDocSize[ localIndex ] = size;
					for( int l = numIndices; l-- != 0; ) if ( l != localIndex ) localSizes[ l ].writeGamma( 0 ); 
				}
			}
			else { 
				for( int i = 0; i < globalIndex.numberOfDocuments; i++ ) {
					localSizes[ localIndex = strategy.localIndex( i ) ].writeGamma( size = sizes.readGamma() );
					if ( maxDocSize[ localIndex ] < size ) maxDocSize[ localIndex ] = size;
				}
			}

			sizes.close();
			for ( int i = 0; i < numIndices; i++ ) localSizes[ i ].close();
		}
	}
	
	public void run() throws Exception {
		final ProgressLogger pl = new ProgressLogger( LOGGER, logInterval, TimeUnit.MILLISECONDS );
		final IntBigList sizeList = globalIndex.sizes;
		partitionSizes();
		
		final int[] position = new int[ Math.max( 0, globalIndex.maxCount ) ];  
		final long[] localFrequency = new long[ numIndices ];  
		final long[] sumMaxPos = new long[ numIndices ];  
		final int[] usedIndex = new int[ numIndices ];
		final InputBitStream[] direct = new InputBitStream[ numIndices ];
		final InputBitStream[] indirect = new InputBitStream[ numIndices ];
		@SuppressWarnings("unchecked")
		final BloomFilter<Void>[] bloomFilter = bloomFilterPrecision != 0 ? new BloomFilter[ numIndices ] : null;
		final File[] tempFile = new File[ numIndices ];
		final CachingOutputBitStream[] temp = new CachingOutputBitStream[ numIndices ];
		IndexIterator indexIterator;
		
		for ( int i = 0; i < numIndices; i++ ) {
			tempFile[ i ] = new File( localBasename[ i ] + ".temp" );
			temp[ i ] = new CachingOutputBitStream( tempFile[ i ], bufferSize );
			direct[ i ] = new InputBitStream( temp[ i ].buffer() );
			indirect[ i ] = new InputBitStream( tempFile[ i ] );
			if ( bloomFilterPrecision != 0 ) bloomFilter[ i ] = BloomFilter.create( globalIndex.numberOfTerms, bloomFilterPrecision );
		}
		int usedIndices;
		MutableString currentTerm = new MutableString();
		Payload payload = null;
		long frequency, globalPointer, localPointer;
		int localIndex, count = -1;

		pl.expectedUpdates = globalIndex.numberOfPostings;
		pl.itemsName = "postings";
		pl.logInterval = logInterval;
		pl.start( "Partitioning index..." );

		for ( long t = 0; t < globalIndex.numberOfTerms; t++ ) {
			terms.readLine( currentTerm );
			indexIterator = indexReader.nextIterator();
			usedIndices = 0;
			frequency = indexIterator.frequency();
			
			for ( long j = 0; j < frequency; j++ ) {
				globalPointer = indexIterator.nextDocument();								
				localIndex = strategy.localIndex( globalPointer );	

				if ( localFrequency[ localIndex ] == 0 ) {
					// First time we see a document for this index.
					currentTerm.println( localTerms[ localIndex ] );
					numTerms[ localIndex ]++;
					usedIndex[ usedIndices++ ] = localIndex;
					if ( bloomFilterPrecision != 0 ) bloomFilter[ localIndex ].add( currentTerm );
				}
				
				/* Store temporarily posting data; note that we save the global pointer as we
				 * will have to access the size list. */
				
				localFrequency[ localIndex ]++;
				numPostings[ localIndex ]++;
				temp[ localIndex ].writeLongGamma( globalPointer );

				if ( globalIndex.hasPayloads ) payload = indexIterator.payload();
				if ( havePayloads ) payload.write( temp[ localIndex ] );
				
				if ( haveCounts ) {
					count = indexIterator.count();
					temp[ localIndex ].writeGamma( count );
					occurrencies[ localIndex ] += count;				
					if ( maxDocPos[ localIndex ] < count ) maxDocPos[ localIndex ] = count; 				
					if ( havePositions ) {
						int pos = indexIterator.nextPosition(), prevPos = pos;
						temp[ localIndex ].writeDelta( pos );
						for( int p = 1; p < count; p++ ) {
							temp[ localIndex ].writeDelta( ( pos = indexIterator.nextPosition() ) - prevPos - 1 );
							prevPos = pos;
						}
						sumMaxPos[ localIndex ] += pos;
					}
				}
			}
			
			// We now run through the indices used by this term and copy from the temporary buffer.

			OutputBitStream obs;
			
			for( int k = 0; k < usedIndices; k++ ) {
				final int i = usedIndex[ k ];

				if ( haveCounts ) numOccurrences[ i ] += occurrencies[ i ];
				InputBitStream ibs;
				if ( quasiSuccinctIndexWriter[ i ] != null ) quasiSuccinctIndexWriter[ i ].newInvertedList( localFrequency[ i ], occurrencies[ i ], sumMaxPos[ i ] ); 
				else indexWriter[ i ].newInvertedList();
				occurrencies[ i ] = 0;

				temp[ i ].align();
				if ( temp[ i ].buffer() != null ) ibs = direct[ i ];
				else {
					// We cannot read directly from the internal buffer.
					ibs = indirect[ i ];
					ibs.flush();
					temp[ i ].flush();
				}

				ibs.position( 0 );
					
				indexWriter[ i ].writeFrequency( localFrequency[ i ] );
				for( long j = 0; j < localFrequency[ i ]; j++ ) {
					obs = indexWriter[ i ].newDocumentRecord();
					globalPointer = ibs.readLongGamma();
					localPointer = strategy.localPointer( globalPointer );	
					indexWriter[ i ].writeDocumentPointer( obs, localPointer );
					if ( havePayloads ) {
						payload.read( ibs );
						indexWriter[ i ].writePayload( obs, payload );
					}
					if ( haveCounts ) indexWriter[ i ].writePositionCount( obs, count = ibs.readGamma() );
					if ( havePositions ) {
						ibs.readDeltas( position, count );
						for( int p = 1; p < count; p++ ) position[ p ] += position[ p - 1 ] + 1;
						indexWriter[ i ].writeDocumentPositions( obs, position, 0, count, sizeList != null ? sizeList.getInt( globalPointer ) : -1 );
					}
					
				}
				temp[ i ].position( 0 );
				temp[ i ].writtenBits( 0 );
				localFrequency[ i ] = 0;
				sumMaxPos[ i ] = 0;
			}
			
			usedIndices = 0;
			pl.count += frequency - 1;
			pl.update();
		}

		pl.done();

		Properties globalProperties = new Properties();
		globalProperties.setProperty( Index.PropertyKeys.FIELD, inputProperties.getProperty( Index.PropertyKeys.FIELD ) );
		globalProperties.setProperty( Index.PropertyKeys.TERMPROCESSOR, inputProperties.getProperty( Index.PropertyKeys.TERMPROCESSOR ) );
		
		for ( int i = 0; i < numIndices; i++ ) {
			localTerms[ i ].close(); 
			indexWriter[ i ].close();
			if ( bloomFilterPrecision != 0 ) BinIO.storeObject( bloomFilter[ i ], localBasename[ i ] + DocumentalCluster.BLOOM_EXTENSION );
			temp[ i ].close();
			tempFile[ i ].delete();
			
			Properties localProperties = indexWriter[ i ].properties();
			localProperties.addAll( globalProperties );
			localProperties.setProperty( Index.PropertyKeys.MAXCOUNT, String.valueOf( maxDocPos[ i ] ) );
			localProperties.setProperty( Index.PropertyKeys.MAXDOCSIZE, maxDocSize[ i ] );
			localProperties.setProperty( Index.PropertyKeys.FIELD, globalProperties.getProperty( Index.PropertyKeys.FIELD ) );
			localProperties.setProperty( Index.PropertyKeys.OCCURRENCES, haveCounts ? numOccurrences[ i ] : -1 );
			localProperties.setProperty( Index.PropertyKeys.POSTINGS, numPostings[ i ] );
			localProperties.setProperty( Index.PropertyKeys.TERMS, numTerms[ i ] );
			if ( havePayloads ) localProperties.setProperty( Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName() );
			if ( strategyProperties[ i ] != null ) localProperties.addAll( strategyProperties[ i ] );
			localProperties.save( localBasename[ i ] + DiskBasedIndex.PROPERTIES_EXTENSION );
		}

		if ( strategyFilename != null ) globalProperties.setProperty( IndexCluster.PropertyKeys.STRATEGY, strategyFilename );
		for( int i = 0; i < numIndices; i++ ) globalProperties.addProperty( IndexCluster.PropertyKeys.LOCALINDEX, localBasename[ i ] );
		globalProperties.setProperty( DocumentalCluster.PropertyKeys.BLOOM, bloomFilterPrecision != 0 );
		// If we partition an index with a single term, by definition we have a flat cluster
		globalProperties.setProperty( DocumentalCluster.PropertyKeys.FLAT, inputProperties.getLong( Index.PropertyKeys.TERMS ) <= 1 );
		globalProperties.setProperty( Index.PropertyKeys.MAXCOUNT, inputProperties.getProperty( Index.PropertyKeys.MAXCOUNT ) );
		globalProperties.setProperty( Index.PropertyKeys.MAXDOCSIZE, inputProperties.getProperty( Index.PropertyKeys.MAXDOCSIZE ) );
		globalProperties.setProperty( Index.PropertyKeys.POSTINGS, inputProperties.getProperty( Index.PropertyKeys.POSTINGS ) );
		globalProperties.setProperty( Index.PropertyKeys.OCCURRENCES, inputProperties.getProperty( Index.PropertyKeys.OCCURRENCES ) );
		globalProperties.setProperty( Index.PropertyKeys.DOCUMENTS, inputProperties.getProperty( Index.PropertyKeys.DOCUMENTS ) );
		globalProperties.setProperty( Index.PropertyKeys.TERMS, inputProperties.getProperty( Index.PropertyKeys.TERMS ) );
		if ( havePayloads ) globalProperties.setProperty( Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName() );

		/* For the general case, we must rely on a merged cluster. However, if we detect a contiguous
		 * strategy we can optimise a bit. */
		
		globalProperties.setProperty( Index.PropertyKeys.INDEXCLASS, 
				strategy instanceof ContiguousDocumentalStrategy ?
						DocumentalConcatenatedCluster.class.getName() :
						DocumentalMergedCluster.class.getName() );
		
		globalProperties.save(  outputBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
		LOGGER.debug( "Properties for clustered index " + outputBasename + ": " + new ConfigurationMap( globalProperties ) );
		
	}

	
	public static void main( final String arg[] ) throws ConfigurationException, IOException, URISyntaxException, ClassNotFoundException, Exception {		
		
		SimpleJSAP jsap = new SimpleJSAP( PartitionDocumentally.class.getName(), "Partitions an index documentally.",
				new Parameter[] {
			new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer." ),
			new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
			new FlaggedOption( "strategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "strategy", "A serialised documental partitioning strategy." ),
			new FlaggedOption( "uniformStrategy", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'u', "uniform", "Requires a uniform partitioning in the given number of parts." ),
			new FlaggedOption( "bloom", JSAP.INTEGER_PARSER, "0", JSAP.NOT_REQUIRED, 'B', "bloom", "Generates Bloom filters with given precision." ),
			new FlaggedOption( "comp", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag for the index (may be specified several times)." ).setAllowMultipleDeclarations( true ),
			new Switch( "noSkips", JSAP.NO_SHORTFLAG, "no-skips", "Disables skips." ),
			new Switch( "interleaved", JSAP.NO_SHORTFLAG, "interleaved", "Forces an interleaved index." ),
			new Switch( "highPerformance", 'h', "high-performance", "Forces a high-performance index." ),
			new FlaggedOption( "cacheSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( QuasiSuccinctIndexWriter.DEFAULT_CACHE_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "cache-size", "The size of the bit cache used while creating a quasi-succinct index." ),
			new FlaggedOption( "quantum", JSAP.INTSIZE_PARSER, "32", JSAP.NOT_REQUIRED, 'Q', "quantum", "The skip quantum." ),
			new FlaggedOption( "height", JSAP.INTSIZE_PARSER, Integer.toString( BitStreamIndex.DEFAULT_HEIGHT ), JSAP.NOT_REQUIRED, 'H', "height", "The skip height." ),
			new FlaggedOption( "skipBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "skip-buffer-size", "The size of the internal temporary buffer used while creating an index with skips." ),
			new UnflaggedOption( "inputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the global index." ),
			new UnflaggedOption( "outputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the local indices." )
		});
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;
		String inputBasename = jsapResult.getString( "inputBasename" );
		String outputBasename = jsapResult.getString( "outputBasename" );
		String strategyFilename = jsapResult.getString( "strategy" );
		DocumentalPartitioningStrategy strategy = null;

		if ( jsapResult.userSpecified( "uniformStrategy" ) ) {
			strategy = DocumentalStrategies.uniform( jsapResult.getInt( "uniformStrategy" ), Index.getInstance( inputBasename ).numberOfDocuments );
			BinIO.storeObject( strategy, strategyFilename = outputBasename + IndexCluster.STRATEGY_DEFAULT_EXTENSION );
		}
		else if ( strategyFilename != null ) strategy = (DocumentalPartitioningStrategy)BinIO.loadObject( strategyFilename );
		else throw new IllegalArgumentException( "You must specify a partitioning strategy" );
		
		final boolean skips = ! jsapResult.getBoolean( "noSkips" );
		final boolean interleaved = jsapResult.getBoolean( "interleaved" );
		final boolean highPerformance = jsapResult.getBoolean( "highPerformance" );
		if ( ! skips && ! interleaved ) throw new IllegalArgumentException( "You can disable skips only for interleaved indices" );
		if ( interleaved && highPerformance ) throw new IllegalArgumentException( "You must specify either --interleaved or --high-performance." );
		if ( ! skips && ( jsapResult.userSpecified( "quantum" ) || jsapResult.userSpecified( "height" ) ) ) throw new IllegalArgumentException( "You specified quantum or height, but you also disabled skips." );

		final IndexType indexType = interleaved ? IndexType.INTERLEAVED : highPerformance ? IndexType.HIGH_PERFORMANCE : IndexType.QUASI_SUCCINCT;
		final Map<Component, Coding> compressionFlags = indexType == IndexType.QUASI_SUCCINCT ?
				CompressionFlags.valueOf( jsapResult.getStringArray( "comp" ), CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX ) :
				CompressionFlags.valueOf( jsapResult.getStringArray( "comp" ), CompressionFlags.DEFAULT_STANDARD_INDEX );

		new PartitionDocumentally( inputBasename,
				outputBasename, 
				strategy, 
				strategyFilename,
				jsapResult.getInt( "bloom" ),
				jsapResult.getInt( "bufferSize" ),
				compressionFlags,
				indexType,
				skips,
				jsapResult.getInt( "quantum" ),
				jsapResult.getInt( "height" ),
				indexType == IndexType.QUASI_SUCCINCT ? jsapResult.getInt( "cacheSize" ) : jsapResult.getInt( "skipBufferSize" ),
				jsapResult.getLong( "logInterval" ) ).run();
	}			
}
