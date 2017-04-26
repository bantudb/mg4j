package it.unimi.di.big.mg4j.tool;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.index.BitStreamHPIndexWriter;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.BitStreamIndexWriter;
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
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.VariableQuantumIndexWriter;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.dsi.Util;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationMap;
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
import com.martiansoftware.jsap.stringparsers.FileStringParser;

/** Combines several indices.
 * 
 * <p>Indices may be combined in several different ways. This abstract class
 * contains code that is common to classes such as {@link it.unimi.di.big.mg4j.tool.Merge}
 * or {@link it.unimi.di.big.mg4j.tool.Concatenate}: essentially, command line parsing,
 * index opening, and term list fusion is taken care of. Then, the template method
 * {@link #combine(int, long)} must write into {@link #indexWriter} the combined inverted
 * list. If, however, {@link #metadataOnly} is true,
 * {@link #indexWriter} is <code>null</code> and {@link #combine(int, long)} must just 
 *  compute the total frequency, occurrency, and sum of maximum positions.
 * 
 * <p>Note that by combining a single index into a new one you can recompress an index
 * with different compression parameters (which includes the possibility of eliminating
 * positions or counts). It is also possible to build just the metadata associated with an index (term list,
 * frequencies, occurrencies).
 * 
 * <p>The subclasses of this class must implement {@link #combine(int, long)} so that indices
 * with different sets of features are combined keeping the largest set of features requested
 * by the user. For instance, combining an index with positions and an index with counts, but
 * no positions, should generate an index with counts but no positions. 
 *
 * <p><strong>Warning</strong>: a combination requires opening <em>three</em> files per input index,
 * plus a few more files for the output index. If the combination process is interrupted by
 * an exception claiming that there are too many open files, check how to increase the
 * number of files you can open (usually, for instance on UN*X, there is a global and a per-process limit,
 * so be sure to set both).
 * 
 * <h2>Read-once indices, readers, and distributed index combination</h2>
 * 
 * <p>If the {@linkplain it.unimi.di.big.mg4j.index.Index indices} and 
 * {@linkplain it.unimi.di.big.mg4j.index.BitStreamIndexReader bitstream index readers} involved in the
 * combination are <em>read-once</em> (i.e., opening an index and reading once its contents sequentially
 * causes each file composing the index to be read exactly once) 
 * <em>then also {@link it.unimi.di.big.mg4j.tool.Combine} implementations should be read-once</em> ({@link it.unimi.di.big.mg4j.tool.Concatenate},
 * {@link it.unimi.di.big.mg4j.tool.Merge} and {@link it.unimi.di.big.mg4j.tool.Paste} are).
 * 
 * <p>This means, in particular, that index combination can be performed from <em>pipes</em>, which in
 * turn can be filled, for instance, with data coming from the network. In other words, albeit this
 * class is theoretically based on a number of indices existing on a local disk, those indices can be
 * substituted with suitable pipes filled with remote data without affecting the combination process.
 * For instance, the following <samp>bash</samp> code creates three sets of pipes for an interleaved index:
 * <pre style="margin: 1em 0">
 * for i in 0 1 2; do
 *   for e in frequencies occurrencies index offsets posnumbits sumsmaxpos properties sizes terms; do 
 *     mkfifo pipe$i.$e
 *   done
 * done
 * </pre> 
 * 
 * <p>Each pipe should be then filled with suitable data, for instance obtained from the net (assuming
 * you have indices <samp>index0</samp>, <samp>index1</samp> and <samp>index2</samp> on <samp>example.com</samp>):
 * <pre style="margin: 1em 0">
 * for i in 0 1 2; do 
 *   for e in frequencies occurrencies index offsets posnumbits sumsmaxpos properties sizes terms; do 
 *     (ssh -x example.com cat index$i.$e >pipe$i.$e &)
 *   done
 * done
 * </pre> 
 * <p>Now all pipes will be filled with data from the corresponding remote files, and
 * combining the indices <samp>pipe0</samp>, <samp>pipe1</samp> and <samp>pipe2</samp>
 * will give the same result as combining <samp>index0</samp>, <samp>index1</samp> and <samp>index2</samp>
 * on the remote system.
 * 
 * @author Sebastiano Vigna
 * @since 1.0
 */

public abstract class Combine {
	private static final Logger LOGGER = LoggerFactory.getLogger( Combine.class );
	private final static boolean DEBUG = false;

	public enum IndexType {
		/** An old-style, interleaved index. */
		INTERLEAVED,
		/** A high-performance index which stores position separately. */
		HIGH_PERFORMANCE,
		/** A quasi-succinct index. */
		QUASI_SUCCINCT
	}
	
	/** The default buffer size. */
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	
	/** The I/O factory that will be used to create files. */
	protected final IOFactory ioFactory;
	/** The number of indices to be merged. */
	final protected int numIndices;
	/** The array of indices to be merged. */
	final protected Index[] index;
	/** An array of index readers parallel to {@link #index}. */
	final protected IndexReader[] indexReader;
	/** An array of index iterators parallel to {@link #index} (filled by concrete implementations). */
	final protected IndexIterator[] indexIterator;
	/** Compute only index metadata (sizes, terms and occurrencies). */
	protected final boolean metadataOnly; 
	/** An array of input bit streams, returning the occurrencies for each index. */
	private final InputBitStream[] occurrencies;
	/** An array of input bit streams, returning the offsets for each index (used for variable-quantum computation). */
	private final InputBitStream[] offsets;
	/** An array of input bit streams, returning the number of bits used for positions for each index (used for variable-quantum computation). */
	private final InputBitStream[] posNumBits;
	/** An array of input bit streams, returning sum of maximum positions for each index (used for variable-quantum computation). */
	protected final InputBitStream[] sumsMaxPos;
	/** Whether to have occurrencies for all indices. */
	private boolean haveOccurrencies; 
	/** Whether we have the sum of maximum positions for all indices. */
	protected boolean haveSumsMaxPos;
	/** Whether to output sizes. */
	private boolean writeSizes; 
	/** An array of mutable strings, containing the last term read for a given index. */
	private MutableString[] term;
	/** An array of fast buffered readers, used to read the terms of each index. */
	private FastBufferedReader[] termReader;
	/** The queue containing terms. */
	protected ObjectHeapSemiIndirectPriorityQueue<MutableString> termQueue;
	/** The overall number of documents. */
	protected final long numberOfDocuments;
	/** The overall number of occurrences. */
	protected long numberOfOccurrences;
	/** The maximum count in the merged index. */
	protected int maxCount;
	/** The array of input basenames. */
	protected final String[] inputBasename;
	/** The output basename. */
	protected final String outputBasename;
	/** The size of I/O buffers. */
	protected final int bufferSize;
	/** If nonzero, the fraction of space to be used by variable-quantum skip towers. */
	protected final double p;
	/** The logging interval. */
	private final long logInterval;
	/** The index writer for the merged index. */ 
	protected IndexWriter indexWriter;
	/** A copy of {@link #indexWriter} which is non-<code>null</code> if {@link #indexWriter} is an instance of {@link VariableQuantumIndexWriter}. */ 
	protected VariableQuantumIndexWriter variableQuantumIndexWriter;
	/** A copy of {@link #indexWriter} which is non-<code>null</code> if {@link #indexWriter} is an instance of {@link QuasiSuccinctIndexWriter}. */ 
	protected QuasiSuccinctIndexWriter quasiSuccinctIndexWriter;
	/** Whether {@link #indexWriter} has counts. */
	protected final boolean hasCounts;
	/** Whether {@link #indexWriter} has positions. */
	protected final boolean hasPositions;
	/** Whether {@link #indexWriter} has payloads. */
	protected final boolean hasPayloads;
	/** Additional properties for the merged index. */
	protected final Properties additionalProperties;
	/** An array partially filled with the indices (as offsets in {@link #index}) participating to the merge process for the current term. */
	protected final int[] usedIndex;
	/** For each index, the frequency of the current term (given that it is present). */
	protected final long[] frequency;
	/** A temporary place to write positions. */
	protected int[] positionArray;
	/** True if the index writer needs sizes (usually, because it uses {@linkplain Coding#GOLOMB Golomb} or {@linkplain Coding#INTERPOLATIVE interpolative} coding for its positions). */
	protected final boolean needsSizes;
	/** The big array of sizes of the combined index. This is set up by {@link #combineSizes(OutputBitStream)} by the combiners who need it. */
	protected int[][] size;
	/** The predicted size of the non-positional part of next inverted list to be combined. It will be -1, unless {@link #p} is not zero. */
	protected long predictedSize;
	/** The predicted number of bits for the positions the next inverted list to be combined. It will be -1, unless {@link #p} is not zero. */
	protected long predictedLengthNumBits;
	
	/** Combines several indices into one.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param outputBasename the basename of the combined index.
	 * @param inputBasename the basenames of the input indices.
	 * @param metadataOnly if true, we save only metadata (term list, frequencies, occurrencies).
	 * @param requireSizes if true, the sizes of input indices will be forced to be loaded.
	 * @param bufferSize the buffer size for index readers.
	 * @param writerFlags the flags for the index writer.
	 * @param indexType the type of the index to build.
	 * @param skips whether to insert skips in case <code>interleaved</code> is true.
	 * @param quantum the quantum of skipping structures; if negative, a percentage of space for variable-quantum indices (irrelevant if <code>skips</code> is false).
	 * @param height the height of skipping towers (irrelevant if <code>skips</code> is false).
	 * @param skipBufferOrCacheSize the size of the buffer used to hold temporarily inverted lists during the skipping structure construction, or the size of the bit cache used when
	 * building a {@linkplain QuasiSuccinctIndex quasi-succinct index}.
	 * @param logInterval how often we log.
	 */
	public Combine( 
			final IOFactory ioFactory,
			final String outputBasename,
			final String[] inputBasename,
			final boolean metadataOnly,
			final boolean requireSizes,
			final int bufferSize,
			final Map<Component,Coding> writerFlags,
			IndexType indexType,
			boolean skips,
			final int quantum,
			final int height,
			final int skipBufferOrCacheSize,
			final long logInterval ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this( ioFactory, outputBasename, inputBasename, null, metadataOnly, requireSizes, bufferSize, writerFlags, indexType, skips, quantum, height, skipBufferOrCacheSize, logInterval );
	}

	/** Combines several indices into one.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param outputBasename the basename of the combined index.
	 * @param inputBasename the basenames of the input indices.
	 * @param delete a monotonically increasing list of integers representing documents that will be deleted from the output index, or <code>null</code>.
	 * @param metadataOnly if true, we save only metadata (term list, frequencies, occurrencies).
	 * @param requireSizes if true, the sizes of input indices will be forced to be loaded.
	 * @param bufferSize the buffer size for index readers.
	 * @param writerFlags the flags for the index writer.
	 * @param indexType the type of the index to build.
	 * @param skips whether to insert skips in case <code>interleaved</code> is true.
	 * @param quantum the quantum of skipping structures; if negative, a percentage of space for variable-quantum indices (irrelevant if <code>skips</code> is false).
	 * @param height the height of skipping towers (irrelevant if <code>skips</code> is false).
	 * @param skipBufferOrCacheSize the size of the buffer used to hold temporarily inverted lists during the skipping structure construction, or the size of the bit cache used when
	 * building a {@linkplain QuasiSuccinctIndex quasi-succinct index}.
	 * @param logInterval how often we log.
	 */
	public Combine( 
			final IOFactory ioFactory,
			final String outputBasename,
			final String[] inputBasename,
			final IntList delete,
			final boolean metadataOnly,
			final boolean requireSizes,
			final int bufferSize,
			final Map<Component,Coding> writerFlags,
			IndexType indexType,
			boolean skips,
			final int quantum,
			final int height,
			final int skipBufferOrCacheSize,
			final long logInterval ) throws IOException, ConfigurationException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		this.logInterval = logInterval;
		this.ioFactory = ioFactory;

		LOGGER.debug( "Combining indices " + Arrays.toString( inputBasename ) + " into " + outputBasename );
		
		// We filter query parameters. A bit dirty--must be kept in sync with Index.getInstance().
		this.inputBasename = new String[ inputBasename.length ]; 
		for( int i = 0; i < inputBasename.length; i++ ) {
			final int questionMarkPos = inputBasename[ i ].indexOf( '?' ); 
			this.inputBasename[ i ] = questionMarkPos == -1 ? inputBasename[ i ] : inputBasename[ i ].substring( 0, questionMarkPos );
		}
		this.outputBasename = outputBasename;
		this.metadataOnly = metadataOnly;
		this.bufferSize = bufferSize;
		needsSizes = writerFlags.get( Component.POSITIONS ) == Coding.GOLOMB || writerFlags.get( Component.POSITIONS ) == Coding.INTERPOLATIVE;
		
		numIndices = inputBasename.length;
		index = new Index[ numIndices ];
		indexReader = new IndexReader[ numIndices ];
		indexIterator = new IndexIterator[ numIndices ];
		occurrencies = new InputBitStream[ numIndices ];
		offsets = new InputBitStream[ numIndices ];
		posNumBits = new InputBitStream[ numIndices ];
		sumsMaxPos = new InputBitStream[ numIndices ];
		term = new MutableString[ numIndices ];
		termReader = new FastBufferedReader[ numIndices ];
		termQueue = new ObjectHeapSemiIndirectPriorityQueue<MutableString>( term, numIndices );
		
		// This will remain set if *all* indices to be merged agree. haveSumsMaxPos starts from true only for quasi-succinct indices.
		boolean haveCounts = writerFlags.containsKey( Component.COUNTS ), havePositions = writerFlags.containsKey( Component.POSITIONS );
		haveSumsMaxPos = haveOccurrencies = true;
		writeSizes = true;
		/* This will be set if *all* indices to be merged agree. Moreover, if some
		 * indices disagree we will emit a warning. */
		TermProcessor termProcessor = null;
		/* This will be set if *all* indices to be merged agree. Moreover, if some
		 * indices disagree we will emit a warning. */
		Payload payload = null;
		String field = null;
		boolean someOccurrencies = false, someSizes = false, allDataForSizeComputation = true;
		
		for( int i = 0; i < numIndices; i++ ) {
			index[ i ] = Index.getInstance( ioFactory, inputBasename[ i ], false, requireSizes, false );
			if ( i == 0 ) {
				termProcessor = index[ 0 ].termProcessor.copy();
				payload = index[ 0 ].payload == null ? null : index[ 0 ].payload.copy();
			}
			else {
				if ( ! termProcessor.equals( index[ i ].termProcessor ) ) throw new IllegalStateException( "The term processor of the first index (" + termProcessor + ") is different from the term processor of index " + i + " (" + index[ i ].termProcessor + ")" );
				if ( ( payload == null ) != ( index[ i ].payload == null ) || payload != null && ! payload.compatibleWith( index[ i ].payload ) ) throw new IllegalStateException( "The payload specification of index " + index[ 0 ] + " is not compatible with that of index " + index[ i ] );
			}

			if ( index[ i ].field != null ) {
				if ( field == null ) {
					if ( i != 0 ) LOGGER.warn( "Not all indices specify the field property" );
					field = index[ i ].field;
				}
				else if ( ! field.equals( index[ i ].field ) ) LOGGER.warn( "Index fields disagree: \"" + field + "\", \"" + index[ i ].field + "\"" );
			}

			haveCounts &= index[ i ].hasCounts;
			havePositions &= index[ i ].hasPositions;
			maxCount = Math.max( maxCount, index[ i ].maxCount );
			indexReader[ i ] = index[ i ].getReader( bufferSize );
			if ( index[ i ].properties.getLong( Index.PropertyKeys.OCCURRENCES, -1 ) == -1 ) numberOfOccurrences = -1;
			if ( numberOfOccurrences != -1 ) numberOfOccurrences += index[ i ].properties.getLong( Index.PropertyKeys.OCCURRENCES );

			final String occurrenciesFile = this.inputBasename[ i ] + DiskBasedIndex.OCCURRENCIES_EXTENSION;
			haveOccurrencies &= ioFactory.exists( occurrenciesFile );
			someOccurrencies |= ioFactory.exists( occurrenciesFile );
			if ( haveOccurrencies ) occurrencies[ i ] = new InputBitStream( ioFactory.getInputStream( occurrenciesFile ), false );

			final String sumsMaxPosFile = this.inputBasename[ i ] + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION;
			haveSumsMaxPos &= ioFactory.exists( sumsMaxPosFile );
			if ( haveSumsMaxPos ) sumsMaxPos[ i ] = new InputBitStream( ioFactory.getInputStream( sumsMaxPosFile ), false );

			if ( ! metadataOnly ) {
				final String offsetsFile = this.inputBasename[ i ] + DiskBasedIndex.OFFSETS_EXTENSION;
				allDataForSizeComputation &= ioFactory.exists( offsetsFile );
				if ( quantum < 0 && allDataForSizeComputation ) offsets[ i ] = new InputBitStream( ioFactory.getInputStream( offsetsFile ), false );

				if ( index[ i ].hasPositions && indexType != IndexType.QUASI_SUCCINCT ) {
					final String positionsLengthsFile = this.inputBasename[ i ] + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION;
					allDataForSizeComputation &= ioFactory.exists( positionsLengthsFile );
					if ( quantum < 0 && allDataForSizeComputation ) posNumBits[ i ] = new InputBitStream( ioFactory.getInputStream( positionsLengthsFile ), false );
				}
			}
			
			final String sizesFile = this.inputBasename[ i ] + DiskBasedIndex.SIZES_EXTENSION;
			writeSizes &= ioFactory.exists( sizesFile );
			someSizes |= ioFactory.exists( sizesFile );

			term[ i ] = new MutableString();
			termReader[ i ] = new FastBufferedReader( new InputStreamReader( ioFactory.getInputStream( this.inputBasename[ i ] + DiskBasedIndex.TERMS_EXTENSION ), "UTF-8" ) );
			if ( termReader[ i ].readLine( term[ i ] ) != null ) termQueue.enqueue( i ); // If the term list is nonempty, we enqueue it
		}

		if ( haveOccurrencies != someOccurrencies ) LOGGER.warn(  "Some (but not all) occurencies file missing" );
		if ( writeSizes != someSizes ) LOGGER.warn(  "Some (but not all) sizes file missing" );
		
		additionalProperties = new Properties();
		additionalProperties.setProperty( Index.PropertyKeys.TERMPROCESSOR, ObjectParser.toSpec( termProcessor ) );
		if ( payload != null ) {
			if ( indexType != IndexType.INTERLEAVED ) throw new IllegalArgumentException( "Payloads are available in interleaved indices only." );
			additionalProperties.setProperty( Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName() );
			//writerFlags.put( Component.PAYLOADS, null );
		}
		additionalProperties.setProperty( Index.PropertyKeys.BATCHES, inputBasename.length );
		if ( field != null ) additionalProperties.setProperty( Index.PropertyKeys.FIELD, field );

		usedIndex = new int[ numIndices ];
		frequency = new long[ numIndices ];
		positionArray = new int[ Math.max( 0, maxCount ) ];

		numberOfDocuments = combineNumberOfDocuments();
		
		if ( ( hasCounts = writerFlags.containsKey( Component.COUNTS ) ) && ! haveCounts ) throw new IllegalArgumentException( "Some of the indices to be combined do not have counts." );
		if ( ( hasPositions = writerFlags.containsKey( Component.POSITIONS ) ) && ! havePositions ) throw new IllegalArgumentException( "Some of the indices to be combined do not have positions." );
		if ( ( hasPayloads = writerFlags.containsKey( Component.PAYLOADS ) ) && payload == null ) throw new IllegalArgumentException( "Indices to be combined do not have payloads." );
		if ( indexType == IndexType.QUASI_SUCCINCT && havePositions && ( ! haveSumsMaxPos || ! haveOccurrencies ) ) throw new IllegalArgumentException( "Quasi-succinct indices require occurrencies and sum of maximum positions to write an index with positions." );
		if ( indexType == IndexType.QUASI_SUCCINCT && haveCounts && ! haveOccurrencies ) throw new IllegalArgumentException( "Quasi-succinct indices require occurencies to write an index with counts." );
		if ( ! allDataForSizeComputation && indexType != IndexType.QUASI_SUCCINCT && hasPositions && skips && quantum < 0 ) throw new IllegalArgumentException( "Some of the indices to be combined do not have offsets or number of bits for positions (and you required variable quanta)." );
		
		// If we have payloads or not all of the index, we are forced to use an interleaved index.
		if ( hasPayloads ) indexType = IndexType.INTERLEAVED;
		if ( indexType == IndexType.HIGH_PERFORMANCE && ! havePositions ) throw new IllegalArgumentException( "You cannot disable positions or counts for high-performance indices." );
		// High-performance indices always have skips.
		skips |= indexType == IndexType.HIGH_PERFORMANCE;
		if ( skips && ( quantum == 0 || height < 0 ) ) throw new IllegalArgumentException( "You must specify a nonzero quantum and a nonnegative height" );
		// We set up variable quanta only if we have skips, we are not computing just metadata, and the quantum is negative.
		p = indexType != IndexType.QUASI_SUCCINCT && skips && ! metadataOnly && quantum < 0 ? -quantum / 100.0 : 0;

		if ( p != 0 ) LOGGER.debug( "Imposing dynamic " + Util.format( p * 100.0 ) + "% occupancy of variable-quantum skip lists" );
		
		if ( ! metadataOnly ) {
			switch( indexType ) {
			case INTERLEAVED:
				if ( ! skips ) indexWriter = new BitStreamIndexWriter( ioFactory, outputBasename, numberOfDocuments, true, writerFlags );
				else indexWriter = new SkipBitStreamIndexWriter( ioFactory, outputBasename, numberOfDocuments, true, skipBufferOrCacheSize, writerFlags, skips ? ( quantum < 0 ? 0 : quantum ) : -1, skips ? height : -1 );
				if ( skips && quantum < 0 ) variableQuantumIndexWriter = (VariableQuantumIndexWriter)indexWriter;
				break;
			case HIGH_PERFORMANCE:
				if ( ioFactory != IOFactory.FILESYSTEM_FACTORY ) throw new IllegalArgumentException( "High-performance indices currently do not support I/O factories" );
				indexWriter = new BitStreamHPIndexWriter( outputBasename, numberOfDocuments, true, skipBufferOrCacheSize, writerFlags, quantum < 0 ? 0 : quantum, height );
				variableQuantumIndexWriter = (VariableQuantumIndexWriter)indexWriter;
				break;
			case QUASI_SUCCINCT:
				indexWriter = quasiSuccinctIndexWriter = new QuasiSuccinctIndexWriter( ioFactory, outputBasename, numberOfDocuments, Fast.mostSignificantBit( quantum < 0 ? QuasiSuccinctIndex.DEFAULT_QUANTUM : quantum ), skipBufferOrCacheSize, writerFlags, ByteOrder.nativeOrder() );
			}
		}
	}
	
	
	/** Combines the number of documents.
	 * 
	 * @return the number of documents of the combined index.
	 */
	protected abstract long combineNumberOfDocuments();
	
	/** A partial {@link IntIterator} implementation based on &gamma;-coded integers.
	 * 
	 * <p>Instances of this class adapt an {@link InputBitStream} to an {@link IntIterator}
	 * by reading &gamma;-coded integers. The implementation is partial because {@link #hasNext()}
	 * always returns true&mdash;the user must know in advance how many times {@link #nextInt()}
	 * may be safely called. 
	 * 
	 * @see #sizes(int)
	 */
	protected static final class GammaCodedIntIterator extends AbstractIntIterator implements Closeable {
		final private InputBitStream inputBitStream;

		public GammaCodedIntIterator( final InputBitStream inputBitStream ) {
			this.inputBitStream = inputBitStream;
		}

		/** Returns true.
		 * @return true
		 */
		public boolean hasNext() { return true; }
		
		/** Returns the next &gamma;-coded integer in the underlying {@link InputBitStream}. 
		 * @return the result of {@link InputBitStream#readGamma()}.
		 */
		public int nextInt() { 
			try {
				return inputBitStream.readGamma();
			}
			catch ( IOException e ) {
				throw new RuntimeException( e );
			} 
		}
		
		/** Delegates to the underlying {@link InputBitStream}. */
		public void close() throws IOException {
			inputBitStream.close();
		}
	}
	
	/** Returns an iterator on sizes.
	 * 
	 * <p>The purpose of this method is to provide {@link #combineSizes(OutputBitStream)} implementations with
	 * a way to access the size list from a disk file or from {@link BitStreamIndex#sizes} transparently.
	 * This mechanism is essential to ensure that size files are read exactly once.
	 * 
	 * <p>The caller should check whether the returned object implements {@link Closeable},
	 * and, in this case, invoke {@link Closeable#close()} after usage.
	 *
	 * @param numIndex the number of an index.
	 * @return an iterator on the sizes of the index.
	 */
	
	protected IntIterator sizes( int numIndex ) throws IOException {
		if ( index[ numIndex ].sizes != null ) return index[ numIndex ].sizes.listIterator();
		LOGGER.debug( "Reading sizes from " + inputBasename[ numIndex ] + DiskBasedIndex.SIZES_EXTENSION );
		return new GammaCodedIntIterator( new InputBitStream( ioFactory.getInputStream( inputBasename[ numIndex ] + DiskBasedIndex.SIZES_EXTENSION ), false ) );
	}

	
	/** Combines size lists.
	 * 
	 * @return the maximum size of a document in the combined index.
	 * @throws IOException
	 */
	protected abstract int combineSizes( final OutputBitStream sizeOutputBitStream ) throws IOException;
	
	/** Combines several indices.
	 * 
	 * <p>When this method is called, exactly <code>numUsedIndices</code> entries
	 * of {@link #usedIndex} contain, in increasing order, the indices containing
	 * inverted lists for the current term. Implementations of this method must
	 * combine the inverted list and return the total frequency.
	 * 
	 * @param numUsedIndices the number of valid entries in {@link #usedIndex}.
	 * @param occurrency the occurrency of the term (used only when building {@link IndexType#QUASI_SUCCINCT} indices).
	 * @return the total frequency.
	 */

	protected abstract long combine( int numUsedIndices, long occurrency ) throws IOException;
	
	
	public void run() throws ConfigurationException, IOException {
		final ProgressLogger pl = new ProgressLogger( LOGGER, logInterval, TimeUnit.MILLISECONDS );
		pl.displayFreeMemory = true;

		final int maxDocSize;

		if ( writeSizes ) {
			LOGGER.info( "Combining sizes..." );
			final OutputBitStream sizesOutputBitStream = new OutputBitStream( ioFactory.getOutputStream( outputBasename + DiskBasedIndex.SIZES_EXTENSION ), bufferSize, false );
			maxDocSize = combineSizes( sizesOutputBitStream );
			sizesOutputBitStream.close();
			LOGGER.info( "Sizes combined." );
		}
		else maxDocSize = -1;
		
		// To write the new term list
		final PrintWriter termFile = new PrintWriter( new BufferedWriter( new OutputStreamWriter( ioFactory.getOutputStream( outputBasename + DiskBasedIndex.TERMS_EXTENSION ), "UTF-8" ), bufferSize ) );
		
		// The current term
		MutableString currTerm;
		
		long totalOccurrency = 0;
		pl.expectedUpdates = haveOccurrencies ? numberOfOccurrences : -1;
		pl.itemsName = haveOccurrencies ? "occurrences" : "terms";
		pl.logInterval = logInterval;
		pl.start( "Combining lists..." );

		int numUsedIndices, k;
		predictedSize = -1;
		predictedLengthNumBits = -1;
		
		// Discard first zero from offsets
		if ( p != 0 ) for( InputBitStream ibs: offsets ) ibs.readGamma();
		
		// TODO: use the front of the queue?
		while( ! termQueue.isEmpty() ) {
			numUsedIndices = 0;
			// We read a new word from the queue, copy it and write it to the term file
			currTerm = term[ k = usedIndex[ numUsedIndices++ ] = termQueue.first() ].copy();
			
			if ( DEBUG ) System.err.println( "Merging term " + currTerm );
			
			currTerm.println( termFile );
			if ( termReader[ k ].readLine( term[ k ] ) == null ) termQueue.dequeue();
			else termQueue.changed();
			
			// Then, we extract all equal words from the queue, accumulating the set of indices in inIndex and currIndex
			while( ! termQueue.isEmpty() && term[ termQueue.first() ].equals( currTerm ) ) {
				k = usedIndex[ numUsedIndices++ ] = termQueue.first();
				if ( termReader[ k ].readLine( term[ k ] ) == null ) termQueue.dequeue();
				else termQueue.changed();
			}
			
			if ( numUsedIndices > 1 ) Arrays.sort( usedIndex, 0, numUsedIndices );

			// Load index iterators
			for( int i = numUsedIndices; i-- != 0; ) indexIterator[ usedIndex[ i ] ] = indexReader[ usedIndex[ i ] ].nextIterator();

			if ( haveOccurrencies ) {
				// Compute and write the total occurrency. This works for any type of combination.
				totalOccurrency = 0;
				for( int i = numUsedIndices; i-- != 0; ) totalOccurrency += occurrencies[ usedIndex[ i ] ].readLongGamma();
			}

			if ( p != 0 ) {
				predictedSize = 0;
				predictedLengthNumBits = 0;

				for( int i = numUsedIndices; i-- != 0; ) {
					if ( index[ usedIndex[ i ] ] instanceof BitStreamHPIndex ) {
						predictedSize += offsets[ usedIndex[ i ] ].readLongGamma();
						if ( hasPositions ) predictedLengthNumBits += posNumBits[ usedIndex[ i ] ].readLongGamma();
					}
					else {
						// Interleaved index: we must subtract the number of bits used for positions from the length of the overall inverted list
						final long t = hasPositions ? posNumBits[ usedIndex[ i ] ].readLongGamma() : 0;
						predictedSize += offsets[ usedIndex[ i ] ].readLongGamma() - t;
						predictedLengthNumBits += t;
					}
				}
			}
						
			combine( numUsedIndices, totalOccurrency );
			/* A trick to get a correct prediction. */
			if ( haveOccurrencies ) pl.count += totalOccurrency - 1;
			pl.update();
		}
		pl.done();
		
		termFile.close();

		if ( ! metadataOnly ) {
			for( int i = numIndices; i-- != 0; ) {
				indexReader[ i ].close();
				if ( haveOccurrencies ) occurrencies[ i ].close();
				if ( sumsMaxPos[ i ] != null ) sumsMaxPos[ i ].close();
				if ( p != 0 ) {
					offsets[ i ].close();
					if ( posNumBits[ i ] != null ) posNumBits[ i ].close();
				}
				termReader[ i ].close();
			}
			final long indexSize = indexWriter.writtenBits();
			indexWriter.close();
			final Properties properties = indexWriter.properties();
			additionalProperties.setProperty( Index.PropertyKeys.SIZE, indexSize );
			additionalProperties.setProperty( Index.PropertyKeys.MAXDOCSIZE, maxDocSize );
			additionalProperties.setProperty( Index.PropertyKeys.MAXCOUNT, maxCount );
			additionalProperties.setProperty( Index.PropertyKeys.OCCURRENCES, numberOfOccurrences );
			properties.addAll( additionalProperties );
			LOGGER.debug( "Post-merge properties: " + new ConfigurationMap( properties ) );
			Scan.saveProperties( ioFactory, properties, outputBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
		}
				
		final PrintStream stats = new PrintStream( ioFactory.getOutputStream( outputBasename + DiskBasedIndex.STATS_EXTENSION ) );
		if ( ! metadataOnly ) indexWriter.printStats( stats );
		stats.close();
	}

	public static void main( final String[] arg ) throws JSAPException, ConfigurationException, IOException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		main( arg, null );
	}
	
	public static void main( final String[] arg, final Class<? extends Combine> combineClass ) throws JSAPException, ConfigurationException, IOException, URISyntaxException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		
		SimpleJSAP jsap = new SimpleJSAP( Combine.class.getName(), "Combines several indices. By default, documents are concatenated, but you can also merge or paste them by choosing the suitable options, or invoking the corresponding subclass instead of " + Combine.class.getName() + ". Note that by combining a single input index you can recompress an index with new parameters.",
				new Parameter[] {
				new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer." ),
				new FlaggedOption( "ioFactory", JSAP.STRING_PARSER, "FILESYSTEM_FACTORY", JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "io-factory", "An I/O factory that will be used to create files (either a static field of IOFactory or an object specification)." ),
				new FlaggedOption( "comp", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag for the index (may be specified several times)." ).setAllowMultipleDeclarations( true ),
				new Switch( "noSkips", JSAP.NO_SHORTFLAG, "no-skips", "Disables skips." ),
				new Switch( "interleaved", JSAP.NO_SHORTFLAG, "interleaved", "Forces an interleaved index." ),
				new Switch( "highPerformance", 'h', "high-performance", "Forces a high-performance index." ),
				new FlaggedOption( "quantum", JSAP.INTEGER_PARSER, Integer.toString( BitStreamIndex.DEFAULT_QUANTUM ), JSAP.NOT_REQUIRED, 'Q', "quantum", "For quasi-succinct indices, the size of the quantum (-1 implies the default quantum). For other indices, enable skips with given quantum, if positive; fix space occupancy of variable-quantum skip towers in percentage if negative." ),
				new FlaggedOption( "height", JSAP.INTSIZE_PARSER, Integer.toString( BitStreamIndex.DEFAULT_HEIGHT ), JSAP.NOT_REQUIRED, 'H', "height", "The skip height." ),
				new Switch( "metadataOnly", 'o', "metadata-only", "Combines only metadata (sizes, terms, frequencies and occurencies)." ),
				new Switch( "merge", 'm', "merge", "Merges indices (duplicates cause an error)." ),
				new Switch( "duplicates", 'd', "duplicates", "Pastes indices, concatenating the document positions for duplicates." ),
				new Switch( "incremental", 'i', "incremental", "Pastes indices incrementally: positions in each index are incremented by the sum of the document sizes in previous indices." ),
				new Switch( "properties", 'p', "properties", "The only specified inputBasename will be used to load a property file written by the scanning process." ),
//				new FlaggedOption( "delete", FileStringParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'D', "delete", "A monotonically increasing array of integers in Java binary format representing documents that will be deleted from the output index." ),
				new FlaggedOption( "tempFileDir", FileStringParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "temp-file-dir", "The directory for the temporary file used during pasting." ),
				new FlaggedOption( "tempFileBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( Paste.DEFAULT_MEMORY_BUFFER_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "temp-file-buffer-size", "The size of the buffer for the temporary file during pasting." ),
				new FlaggedOption( "cacheSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( QuasiSuccinctIndexWriter.DEFAULT_CACHE_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "cache-size", "The size of the bit cache used while creating a quasi-succinct index." ),
				new FlaggedOption( "skipBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "skip-buffer-size", "The size of the internal temporary buffer used while creating an index with skips." ),
				new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
				new UnflaggedOption( "outputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the resulting index." ),
				new UnflaggedOption( "inputBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The basenames of the indices to be merged." )
		});
		
		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		final IOFactory ioFactory = Scan.parseIOFactory( jsapResult.getString( "ioFactory" ) );

		final boolean skips = ! jsapResult.getBoolean( "noSkips" );
		final boolean interleaved = jsapResult.getBoolean( "interleaved" );
		final boolean highPerformance = jsapResult.getBoolean( "highPerformance" );
		if ( ! skips && ! interleaved ) throw new IllegalArgumentException( "You can disable skips only for interleaved indices" );
		if ( interleaved && highPerformance ) throw new IllegalArgumentException( "You must specify either --interleaved or --high-performance." );
		if ( ! skips && ( jsapResult.userSpecified( "quantum" ) || jsapResult.userSpecified( "height" ) ) ) throw new IllegalArgumentException( "You specified quantum or height, but you also disabled skips." );
		
		if ( combineClass != null && jsapResult.userSpecified( "duplicates" ) || jsapResult.userSpecified( "merge") )
			throw new IllegalArgumentException( "When invoking " + Combine.class.getName() + " from " + combineClass.getName() + " you cannot choose the combination process" );
		
		final String[] inputBasename;
		if ( jsapResult.getBoolean( "properties" ) ) {
			if ( jsapResult.getStringArray( "inputBasename" ).length > 1 ) throw new IllegalArgumentException( "When using --properties, you must specify exactly one inputBasename" );
			inputBasename = new Properties( jsapResult.getStringArray( "inputBasename" )[ 0 ] + Scan.CLUSTER_PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
		}
		else inputBasename = jsapResult.getStringArray( "inputBasename" );
		
		
		// TODO: resolve problem of passing default flag values without knowing type of index
		final IndexType indexType = interleaved ? IndexType.INTERLEAVED : highPerformance ? IndexType.HIGH_PERFORMANCE : IndexType.QUASI_SUCCINCT;
		final Map<Component, Coding> compressionFlags = indexType == IndexType.QUASI_SUCCINCT ?
				CompressionFlags.valueOf( jsapResult.getStringArray( "comp" ), CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX ) :
				CompressionFlags.valueOf( jsapResult.getStringArray( "comp" ), CompressionFlags.DEFAULT_STANDARD_INDEX );

		( combineClass == Paste.class || jsapResult.getBoolean( "duplicates" ) ?
		(Combine)new Paste( ioFactory, jsapResult.getString( "outputBasename" ), 
				inputBasename,
				jsapResult.getBoolean( "metadataOnly" ),
				jsapResult.getBoolean( "incremental" ),
				jsapResult.getInt( "bufferSize" ),
				jsapResult.getFile( "tempFileDir" ),
				jsapResult.getInt( "tempFileBufferSize" ),
				compressionFlags,
				indexType,
				skips,
				jsapResult.getInt( "quantum" ),
				jsapResult.getInt( "height" ),
				indexType == IndexType.QUASI_SUCCINCT ? jsapResult.getInt( "cacheSize" ) : jsapResult.getInt( "skipBufferSize" ),
				jsapResult.getLong( "logInterval" ) ) :
		combineClass == Merge.class || jsapResult.getBoolean( "merge" ) ?
				(Combine)new Merge( ioFactory, jsapResult.getString( "outputBasename" ), 
						inputBasename,
						jsapResult.getBoolean( "metadataOnly" ),
						jsapResult.getInt( "bufferSize" ),
						compressionFlags,
						indexType,
						skips,
						jsapResult.getInt( "quantum" ),
						jsapResult.getInt( "height" ),
						indexType == IndexType.QUASI_SUCCINCT ? jsapResult.getInt( "cacheSize" ) : jsapResult.getInt( "skipBufferSize" ),
						jsapResult.getLong( "logInterval" ) ) :
							(Combine)new Concatenate( ioFactory, jsapResult.getString( "outputBasename" ), 
									inputBasename,
									jsapResult.getBoolean( "metadataOnly" ),
									jsapResult.getInt( "bufferSize" ),
									compressionFlags,
									indexType,
									skips,
									jsapResult.getInt( "quantum" ),
									jsapResult.getInt( "height" ),
									indexType == IndexType.QUASI_SUCCINCT ? jsapResult.getInt( "cacheSize" ) : jsapResult.getInt( "skipBufferSize" ),
									jsapResult.getLong( "logInterval" ) )
									
		).run(); 
	}
}
