package it.unimi.di.big.mg4j.index;

/*
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2003-2016 Paolo Boldi and Sebastiano Vigna
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

import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.dsi.Util;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.Int2IntRBTreeMap;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.NullOutputStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;

/** Writes a bitstream-based interleaved index with skips.
 * 
 * <p>These indices are managed by MG4J mainly for historical reasons, as {@linkplain QuasiSuccinctIndex quasi-succinct indices}
 * are just better under every respect.
 * 
 * <p>An interleaved inverted index with skips makes it possible
 * to skip ahead quickly while reading inverted lists. More specifically,
 * when reading the inverted list relative to a certain term, one may want to
 * decide to skip all document records that concern documents with pointer
 * less than a given integer. In a normal inverted index this is impossible:
 * one would have to read all document records sequentially.
 *  
 * <p>The skipping structure used by this class is new, and has been described by 
 * Paolo Boldi and Sebastiano Vigna in &ldquo;<a href="http://vigna.dsi.unimi.it/papers.php#BoVCPESLQIIL">Compressed 
 * perfect embedded skip lists for quick inverted-index lookups</a>&rdquo;, <i>Proc. SPIRE 2005</i>,
 * volume 3772 of Lecture Notes in Computer Science, pages 25&minus;28. Springer, 2005.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 * @since 0.6
 */
public class SkipBitStreamIndexWriter extends BitStreamIndexWriter implements VariableQuantumIndexWriter {
	/** The size of the buffer for the temporary file used to build an inverted list. Inverted lists
	 * shorter than this number of bytes will be directly rebuilt from the buffer, and never flushed to disk. */ 
	public final static int DEFAULT_TEMP_BUFFER_SIZE = 32 * 1024 * 1024;

	private static final boolean ASSERTS = false;
	private static final boolean DEBUG = false;
	private final static boolean STATS = false;

	/** Maximum number of trials when optimising the entry bit length. */
	private final static int MAX_TRY = 32;

	/** Whether the index will use variable quanta. */
	private boolean variableQuanta;

	/** The parameter <code>h</code> (the maximum height of a skip tower). */
	private final int height;

	/** The parameter <code>q</code> (2<var><sup>h</sup>q</var> documents record are kept in the cache); necessarily a power of two. */
	private long quantum;

	/** The bit mask giving the remainder of the division by {@link #quantum}. */	
	private long quantumModuloMask;
	
	/** The shift giving result of the division by {@link #quantum}. */
	private int quantumDivisionShift;
	
	/** We have <var>w</var>=2<sup><var>h</var></sup><var>q</var>. */
	private long w;

	/** The number of document records written in the cache containing the current block. */
	private long cache;

	/** The <var>k</var>-th entry of this array contains the document pointer of the <var>k</var>-th
	 *  skip document record within the current block. For sake of simplicity, <code>pointer[cache]</code>
	 *  contains the first document pointer within the next block. */
	private final long[] skipPointer;

	/** The {@link OutputBitStream}s where cached document pointers are written. */
	private final OutputBitStream[] cachePointer;

	/** The {@link FastByteArrayOutputStream}s underlying <code>cachePointer</code> . */
	private final FastByteArrayOutputStream[] cachePointerByte;

	/** The {@link OutputBitStream}s where cached skip towers are written. Indices are skip
	 *  indices. */
	private final OutputBitStream[] cacheSkip;

	/** An array whose entries (as many as those of {@link #cacheSkip}) are all {@link #bitCount}. */
	private final OutputBitStream[] cacheSkipBitCount;

	/** The {@link FastByteArrayOutputStream}s underlying <code>cacheSkip</code> . Indices are skip
	*  indices. */
	private final FastByteArrayOutputStream[] cacheSkipByte;

	/** The {@link OutputBitStream} where cached document data are written. */
	private final CachingOutputBitStream cacheDataOut;

	/** The {@link FastBufferedInputStream} from which cached document data are read. */
	private final FastBufferedInputStream cacheDataIn;

	/** The length of the data segment for each quantum. */
	private final long[] cacheDataLength;

	/** An {@link OutputBitStream} wrapping a {@link NullOutputStream} for code-length preview. */
	private final OutputBitStream bitCount;

	/** The sum of all tower data computed so far. */
	public final TowerData towerData;

	/** The number of bits written for variable quanta. */
	public long bitsForVariableQuanta;

	/** The number of bits written for quantum lengths. */
	public long bitsForQuantumBitLengths;

	/** The number of bits written for entry lenghts. */
	public long bitsForEntryBitLengths;

	/** Bits used in lists which actually have towers. */
	private long bitsForListsWithTowers;

	/** The number of written blocks. */
	public long numberOfBlocks;

	/** An estimate on the number of bits occupied per tower entry in the last written cache, or -1 if no cache has been
	 * written for the current inverted list. */
	public int prevEntryBitLength;

	/** An estimate on the number of bits occupied per quantum in the last written cache, or -1 if no cache has been
	 * written for the current inverted list. */
	public int prevQuantumBitLength;

	/** The Golomb modulus for a top pointer skip, for each level. */
	private final int[] towerTopB;
	
	/** The most significant bit of the Golomb modulus for a top pointer skip, for each level. */
	private final int[] towerTopLog2B;
	
	/** The Golomb modulus for a lower pointer skip, for each level. */
	private final int[] towerLowerB;
	
	/** The most significant bit of the Golomb modulus for a lower pointer skip, for each level. */
	private final int[] towerLowerLog2B;
	
	/** The prediction for a pointer skip, for each level. */
	private final long[] pointerPrediction;
	
	private java.io.PrintWriter pointerSkipStats;
	private java.io.PrintWriter pointerTopSkipStats;
	private java.io.PrintWriter bitSkipStats;
	private java.io.PrintWriter bitTopSkipStats;
	private String pointerSkipLine, bitSkipLine;

	/** Creates a new skip index writer with the specified basename. The index will be written on a file (stemmed with <samp>.index</samp>).
	 *  If <code>writeOffsets</code>, also an offset file will be produced (stemmed with <samp>.offsets</samp>). 
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the basename.
	 * @param numberOfDocuments the number of documents in the collection to be indexed.
	 * @param writeOffsets if <code>true</code>, the offset file will also be produced.
	 * @param tempBufferSize the size in bytes of the internal temporary buffer (inverted lists shorter than this size will never be flushed to disk). 
	 * @param flags a flag map setting the coding techniques to be used (see {@link CompressionFlags}).
	 * @param quantum the quantum; it must be zero, or a power of two; if it is zero, a variable-quantum index is assumed.
	 * @param height the maximum height of a skip tower; the cache will contain at most 2<sup><var>h</var></sup> document records.
	 */
	public SkipBitStreamIndexWriter( final IOFactory ioFactory, final CharSequence basename, final long numberOfDocuments, final boolean writeOffsets, int tempBufferSize, final Map<Component,Coding> flags, final int quantum, final int height ) throws IOException {
		super( ioFactory, basename, numberOfDocuments, writeOffsets, flags );
		
		if ( height < 0 ) throw new IllegalArgumentException( "Illegal height " + height );
		if ( quantum < 0 || ( quantum & -quantum ) != quantum ) throw new IllegalArgumentException( "Illegal quantum " + quantum );
		this.height = height;
		if ( ! ( variableQuanta = quantum == 0 ) ) this.quantum = quantum;
		quantumDivisionShift = Fast.mostSignificantBit( quantum );
		quantumModuloMask = quantum - 1;

		int two2h = 1 << height;

		if ( DEBUG ) {
			System.err.println( "Cache will contain at most " + two2h * quantum + " records (q=" + quantum + ",h=" + height + ")" );
			/*System.err.print( "Skip records will be " );
			for ( int i = 0; i < two2h; i++ )
				System.err.print( ( i * quantum ) + " " );
			System.err.println();*/
		}

		towerData = new TowerData();
		tempFile = File.createTempFile( "MG4J", ".data" );
		cacheDataIn = new FastBufferedInputStream( new FileInputStream( tempFile ) );
		cacheDataOut = new CachingOutputBitStream( tempFile, tempBufferSize );
		cacheDataLength = new long[ two2h ];
		cachePointer = new OutputBitStream[ two2h ];
		cachePointerByte = new FastByteArrayOutputStream[ two2h ];

		for ( int i = 0; i < two2h; i++ )
			cachePointer[ i ] = new OutputBitStream( cachePointerByte[ i ] = new FastByteArrayOutputStream(), 0 );

		cacheSkip = new OutputBitStream[ two2h ];
		cacheSkipBitCount = new OutputBitStream[ two2h ];
		cacheSkipByte = new FastByteArrayOutputStream[ two2h ];

		for ( int i = 0; i < two2h; i++ ) {
			cacheSkip[ i ] = new OutputBitStream( cacheSkipByte[ i ] = new FastByteArrayOutputStream(), 0 );
			cacheSkipBitCount[ i ] = new OutputBitStream( NullOutputStream.getInstance(), 0 );
		}

		skipPointer = new long[ two2h + 1 ];
		distance = new long[ two2h + 1 ];

		bitCount = new OutputBitStream( NullOutputStream.getInstance(), 0 );

		towerTopB = new int[ height + 1 ];
		towerTopLog2B = new int[ height + 1 ];
		towerLowerB = new int[ height + 1 ];
		towerLowerLog2B = new int[ height + 1 ];
		pointerPrediction = new long[ height + 1 ];

		if ( STATS ) {
			try {
				pointerSkipStats = new PrintWriter( new java.io.FileWriter( "pointerSkip.stats" ) );
				pointerTopSkipStats = new PrintWriter( new java.io.FileWriter( "pointerTopSkip.stats" ) );
				bitSkipStats = new PrintWriter( new java.io.FileWriter( "bitSkip.stats" ) );
				bitTopSkipStats = new PrintWriter( new java.io.FileWriter( "bitTopSkip.stats" ) );

				String comment = "# " + System.getProperty( "freq" ) + "\u00b1" + Integer.getInteger( "error" ) + "%";
				pointerSkipStats.println( comment );
				pointerTopSkipStats.println( comment );
				bitSkipStats.println( comment );
				bitTopSkipStats.println( comment );
			}
			catch ( IOException e ) {
				System.err.println( "Can't open stat files: " + e );
			}
		}
	}

	/** Suggests a quantum size using frequency and bit size data.
	 * 
	 * @param predictedFrequency a prediction of the frequency of the inverted list.
	 * @param numberOfDocuments the number of documents in the collection.
	 * @param fraction the fraction of space to be used for skip lists.
	 * @param predictedSize a prediction of the size of the inverted list for terms and counts.
	 * @param predictedPositionsSize a prediction of the size of the inverted list for positions (might be zero).
	 * @return -1, if this list should not have towers because the suggested quantum size is larger than or equal to <code>predictedFrequency</code>;
	 * the logarithm of the suggested quantum size, otherwise.
	 */
	public static int log2Quantum( long predictedFrequency, long numberOfDocuments, double fraction, long predictedSize, long predictedPositionsSize ) {
		// WARNING: this is almost identical to the BitStreamHPIndexWriter version.
		if ( predictedFrequency < 2 ) return -1;
		
		if ( DEBUG ) System.err.println( "Computing quantum; freq: " + predictedFrequency + " fraction: " + fraction + " size: " + predictedSize + " pos size: " + predictedPositionsSize );

		int quantum, bestLog2q = -1;
		double numberOfEntries, cost, costFraction, p, entropy;
		final double log2PredictedPositionsSize = Fast.log2( predictedPositionsSize );
		final double log2PredictedFrequency = Fast.log2( predictedFrequency );

		p = (double)predictedFrequency / numberOfDocuments;
		// Entropy of the geometric distribution with parameter p.
		entropy = ( -p * Math.log( p ) - ( 1 - p ) * Math.log( 1 - p ) ) / ( p * 0.6931471805599453 );

		for( int log2q = 11; log2q-- != 0; ) {
			quantum = 1 << log2q;
			if ( DEBUG ) System.err.println( "Trying quantum " + quantum + "..." );

			numberOfEntries = (double)predictedFrequency / quantum; // This is an upper bound based on infinite height.
			
			if ( DEBUG ) System.err.println( "Entries: " + numberOfEntries );
			
			if ( numberOfEntries > 1 ) {
				cost = numberOfEntries * ( ( p >= 1 ? 0 : entropy + log2q / 2. + 1 ) + 
						( predictedPositionsSize == 0 ? 0 : 2 * ( log2PredictedPositionsSize + log2q / 2. + Fast.log2( log2q + 1 ) - log2PredictedFrequency ) ) 
						+ 12 );  // This is actually growing very slowly with collection size
				costFraction = cost / ( predictedSize + predictedPositionsSize );
				if ( DEBUG ) System.err.println( "Cost: " + cost + " fraction: " + costFraction );
				
				// We find the smallest quantum that does not break the fraction constraint, and suggest a quantum sized half.
				if ( costFraction < fraction ) bestLog2q = log2q;
			}
		}
		
		if ( DEBUG ) System.err.println( "Best log2(q): " + bestLog2q + "; returning " + ( bestLog2q <= 0 || ( 1 << ( bestLog2q - 1 ) ) > predictedFrequency ? bestLog2q : bestLog2q - 1 ) );
		
		return bestLog2q <= 0 || ( 1 << ( bestLog2q - 1 ) ) > predictedFrequency ? bestLog2q : bestLog2q - 1;
	}

	public long newInvertedList( long predictedFrequency, double fraction, long predictedSize, long predictedPositionsSize ) throws IOException {
		return newInvertedList( SkipBitStreamIndexWriter.log2Quantum( predictedFrequency, numberOfDocuments, fraction, predictedSize, predictedPositionsSize ) );
	}

	public long newInvertedList() throws IOException {
		if ( variableQuanta ) throw new IllegalStateException( "This index writer needs a specific quantum for each inverted list" );
		return newInvertedList( Fast.mostSignificantBit( this.quantum ) );
	}

	private long newInvertedList( final int log2q ) throws IOException {
		if ( cache != 0 ) writeOutCache( -1 );

		// We record the number of bits spent for the last list in case it contained towers
		if ( currentTerm >= 0 && frequency >= quantum ) bitsForListsWithTowers += obs.writtenBits() - lastInvertedListPos;
		
		quantum = log2q < 0 ? 0 : 1 << log2q;

		final long position = super.newInvertedList();
		return position;
	}

	public void writeFrequency( final long frequency ) throws IOException {
		super.writeFrequency( frequency );

		// We write the variable quantum just after the frequency; note that q might be zero for no quantum.
		if ( variableQuanta && frequency > 1 ) bitsForVariableQuanta += obs.writeGamma( Fast.mostSignificantBit( quantum ) + 1 );
		if ( quantum == 0 ) quantum = 1L << Fast.ceilLog2( frequency ) + 1; // So large that we have no skip tower
		w = ( 1L << height ) * quantum;
		quantumDivisionShift = Fast.mostSignificantBit( quantum );
		quantumModuloMask = quantum - 1;

		prevQuantumBitLength = prevEntryBitLength = -1;	

		if ( DEBUG ) System.err.println( "----------- " + currentTerm + " (" + frequency + ")" );

		final long pointerQuantumSigma = BitStreamIndex.quantumSigma( frequency, numberOfDocuments, quantum );
		for( int i = Math.min( height, Fast.mostSignificantBit( frequency >>> quantumDivisionShift ) ); i >= 0; i-- ) {
			towerTopB[ i ] = BitStreamIndex.gaussianGolombModulus( pointerQuantumSigma, i + 1 );
			towerTopLog2B[ i ] = Fast.mostSignificantBit( towerTopB[ i ] );
			towerLowerB[ i ] = BitStreamIndex.gaussianGolombModulus( pointerQuantumSigma, i );
			towerLowerLog2B[ i ] = Fast.mostSignificantBit( towerLowerB[ i ] );
			pointerPrediction[ i ] = ( quantum * ( 1L << i ) * numberOfDocuments + frequency / 2 ) / frequency;
		}

	}

	public OutputBitStream newDocumentRecord() throws IOException {
		super.newDocumentRecord();
		return cacheDataOut;
	}

	public void writeDocumentPointer( final OutputBitStream out, final long pointer ) throws IOException {
		// If the previous block is over, write it out!
		if ( cache == w ) writeOutCache( pointer );

		// Record data pointer if we are on a skip; otherwise, write it to the cache.
		if ( ( cache & quantumModuloMask ) == 0 ) {
			if ( cache >>> quantumDivisionShift > 0 ) cacheDataLength[ (int)( ( cache >>> quantumDivisionShift ) - 1 ) ] = cacheDataOut.writtenBits();
			cacheDataOut.align();
			cacheDataOut.writtenBits( 0 );
			skipPointer[ (int)( cache >>> quantumDivisionShift ) ] = pointer;
			super.writeDocumentPointer( cachePointer[ (int)( cache++ >>> quantumDivisionShift ) ], pointer );
		} 
		else {
			cache++;
			super.writeDocumentPointer( cacheDataOut, pointer );
		}
	}

	private long writeOutPointer( final OutputBitStream out, final long pointer ) throws IOException {
		if ( frequency == numberOfDocuments ) return 0; // We do not write pointers for everywhere occurring terms.

		switch ( pointerCoding ) {
			case GAMMA:
				return out.writeLongGamma( pointer - lastDocument - 1 );
			case DELTA:
				return out.writeLongDelta( pointer - lastDocument - 1 );
			case GOLOMB:
				return out.writeLongGolomb( pointer - lastDocument - 1, b, log2b );
			default:
				throw new IllegalStateException( "The required pointer coding (" + pointerCoding + ") is not supported." );
		}
	}

	public void close() throws IOException {

		if ( cache != 0 ) writeOutCache( -1 );

		bitsForListsWithTowers += obs.writtenBits() - lastInvertedListPos;
		
		super.close();
		
		cacheDataIn.close();
		cacheDataOut.close();
		tempFile.delete();
		
		if ( STATS ) {
			pointerSkipStats.close();
			pointerTopSkipStats.close();
			bitSkipStats.close();
			bitTopSkipStats.close();
		}
	}

	/** A structure maintaining statistical data about tower construction. */

	public static class TowerData {
		/** The number of bits written for bit skips at the top of a tower. */
		public long bitsForTopBitSkips;

		/** The number of bits written for skip pointers at the top of a tower. */
		public long bitsForTopSkipPointers;

		/** The number of bits written for bit skips in the lower part of a tower. */
		public long bitsForLowerBitSkips;

		/** The number of bits written for skip pointers in the lower part of a tower. */
		public long bitsForLowerSkipPointers;

		/** The number of bits written for tower lengths. */
		public long bitsForTowerLengths;

		/** The number of written skip towers. */
		public long numberOfSkipTowers;

		/** The number of written top skip entries. */
		public long numberOfTopEntries;

		/** The number of written lower skip entries. */
		public long numberOfLowerEntries;

		/** Clear all fields of this tower data. */

		void clear() {
			bitsForTopBitSkips = 0;
			bitsForTopSkipPointers = 0;
			bitsForLowerBitSkips = 0;
			bitsForLowerSkipPointers = 0;
			bitsForTowerLengths = 0;
			numberOfSkipTowers = 0;
			numberOfTopEntries = 0;
			numberOfLowerEntries = 0;
		}


		/** Returns the overall number of bits used for skip pointers.
		 * @return the overall number of bits used for skip pointers.
		 */
		public long bitsForSkipPointers() { return bitsForTopSkipPointers + bitsForLowerSkipPointers; }

		/** Returns the overall number of bits used for bit skips. 
		 * @return the overall number of bits used for bit skips.
		 */
		public long bitsForBitSkips() { return bitsForTopBitSkips + bitsForLowerBitSkips; }

		/** Returns the overall number of bits used for tower entries (bits for tower lengths are not included).
		 * @return the overall number of bits used for tower entries.
		 */
		public long bitsForEntries() { return bitsForSkipPointers() + bitsForBitSkips(); }

		/** Returns the overall number of bits used for towers.
		 * @return the overall number of bits used for towers.
		 */
		public long bitsForTowers() { return bitsForTowerLengths + bitsForEntries(); }

		/** Returns the overall number of entries.
		 * @return the overall number of entries.
		 */
		public long numberOfEntries() { return numberOfTopEntries + numberOfLowerEntries; }
	}
	


	/** The <var>k</var>-th entry of this array contains the number of bits from the start of
	 * the <var>k</var>-th skip tower up to the end of the current block (more precisely,
	 * to the point that should be reached via skipping, which is just after the document pointer).
	 * Indices are skip indices. It is used just by {@link #tryTower(int, int, long, OutputBitStream[], TowerData, boolean)}, 
	 * but it is declared here for efficiency.
	 */
	final private long[] distance;

	/** The temporary file dumping the index data contained in a block. */
	final private File tempFile;

	/** Whether we should write stats. */
	private boolean writeStats;
	
	/** Computes the towers.
	 * 
	 * @param quantumBitLength the length in bits of a quantum.
	 * @param entryBitLength the estimated length in bits of a tower entry.
	 * @param toTheEnd the number of bits that must be skipped to reach the next tower (usually,
	 * the length of the first pointer of the next block or 0 if this is to be the last block).
	 * @param skip an array of output bit stream where the data related to each tower will be written.
	 * @param towerData will be filled with statistical date about the towers.
	 * @param doinIt if true, we are actually writing a tower, not just trying.
	 */
	private void tryTower( final int quantumBitLength, final int entryBitLength, long toTheEnd, final OutputBitStream[] skip, final TowerData towerData, final boolean doinIt ) throws IOException {
		int i, k, s;
		long d;
		long basePointer;
		// truncated is true only for those towers (in defective blocks) whose height is strictly smaller than the height they should have
		boolean truncated = false;

		if ( DEBUG ) {
			if ( doinIt ) System.err.println( "Writing out tower for term " + currentTerm + "; quantumBitLength=" + quantumBitLength + " entryBitLength=" + entryBitLength );
		}
		
		for ( k = (int)( ( cache - 1 ) >>> quantumDivisionShift ); k >= 0; k-- ) {
			// Where are we? At the end of the k-th quantum. So toTheEnd must be increased by
			// the length of the data contained in the same quantum, moving us...
			toTheEnd += cacheDataLength[ k ];

			// ...just after the k-th skip tower.
			// We compute the maximum valid index of the skip tower (*MUST* be kept in sync with the subsequent loop).
			s = ( k == 0 ) ? height : Integer.numberOfTrailingZeros( k );

			// This test handles defective blocks. In particular, for defective quanta s=-1,
			// yielding no skipping data at all for such quanta. truncated is true if the
			// current tower is truncated w.r.t. the infinite skip list.
			if ( cache < w ) {
				final int upperBound = Fast.mostSignificantBit( ( cache >>> quantumDivisionShift ) - k );
				if ( s > upperBound ) {
					s = upperBound;
					truncated = true;
				} else truncated = false;
			}
			else truncated = k == 0;
			
			skip[ k ].writtenBits( 0 );

			if ( s >= 0 ) {
				if ( DEBUG ) if ( doinIt ) System.err.print( "% (" + k + ") [" + skipPointer[ k ] + "] " );

				basePointer = skipPointer[ k ];

				/* If the current tower is truncated, we must actually write the top of the tower.
				 * The top must be forecast in a Bernoullian way: we write it as a difference from the average pointer skip, 
				 * which is q 2^s / relativeFrequency. */
				if ( truncated ) {
					towerData.numberOfTopEntries++;
					towerData.bitsForTopSkipPointers += skip[ k ].writeLongGolomb( Fast.int2nat( skipPointer[ k + ( 1 << s ) ] - basePointer - pointerPrediction[ s ] ), towerTopB[ s ], towerTopLog2B[ s ] );
					towerData.bitsForTopBitSkips += skip[ k ].writeLongDelta( Fast.int2nat( 
						( toTheEnd - distance[ k + ( 1 << s ) ] ) - ( quantumBitLength * ( 1L << s ) + entryBitLength * ( ( 1L << s + 1 ) - s - 2 ) ) )
					);
				}
				
				if ( DEBUG ) {
					if ( doinIt ) System.err.print( ( truncated ? "" : "(" ) + ( skipPointer[ k + ( 1 << s ) ] - basePointer ) + ":" + ( toTheEnd - distance[ k + ( 1 << s ) ] ) + ( truncated ? " " : ") " ) );
				}

				if ( STATS ) {
					if ( doinIt && writeStats ) {
						// These *MUST* be kept in sync with the writes above.
						if ( truncated ) {
							pointerTopSkipStats.println( s + "\t" + ( skipPointer[ k + ( 1 << s ) ] - basePointer - pointerPrediction[ s ] ) );
							bitTopSkipStats.println( s + "\t" + ( ( toTheEnd - distance[ k + ( 1 << s ) ] ) - ( quantumBitLength * ( 1L << s ) + entryBitLength * ( ( 1L << s + 1 ) - s - 2 ) ) ) );
						}
						pointerSkipLine = "";
						bitSkipLine = "";
					}
				}

				// Produce a (single) tower of height s
				for ( i = s - 1; i >= 0; i-- ) {
					towerData.bitsForLowerSkipPointers += skip[k].writeLongGolomb( 
						Fast.int2nat( ( skipPointer[ k + ( 1 << i ) ] - basePointer ) - ( ( skipPointer[ k + ( 1 << i + 1 ) ] - basePointer ) / 2 ) ),
						towerLowerB[ i ], towerLowerLog2B[ i ] 
					);

					if ( STATS ) {
						if ( doinIt && writeStats ) pointerSkipLine = ( ( skipPointer[ k + ( 1 << i ) ] - basePointer ) - ( ( skipPointer[ k + ( 1 << i + 1 ) ] - basePointer ) / 2 ) ) + "\t" + pointerSkipLine;
					}					

					towerData.bitsForLowerBitSkips += skip[k].writeLongDelta( 
						Fast.int2nat( ( ( toTheEnd - distance[ k + ( 1 << ( i + 1 ) ) ] - entryBitLength * ( i + 1L ) ) / 2 ) - ( toTheEnd - distance[ k + ( 1 << i ) ] ) ) 
					);

					if ( STATS ) {
						if ( doinIt && writeStats ) bitSkipLine =  ( ( ( toTheEnd - distance[ k + ( 1 << ( i + 1 ) ) ] - entryBitLength * ( i + 1L ) ) / 2 ) -
								( toTheEnd - distance[ k + ( 1 << i ) ] ) ) + "\t" + bitSkipLine;
					}					

					if ( DEBUG ) {
						if ( doinIt ) System.err.print( ( skipPointer[ k + ( 1 << i ) ] - basePointer ) + ":" + ( toTheEnd - distance[ k + ( 1 << i ) ] ) + " " );
					}
				}

				if ( s > 0 ) { // No length for single-entry towers.
					d = bitCount.writeDelta( Fast.int2nat( (int) skip[k].writtenBits() - ( s + 1 ) * entryBitLength ) );
					towerData.bitsForTowerLengths += d;
					toTheEnd += d;
				}

				if ( STATS ) {
					if ( doinIt && writeStats ) {
						if ( pointerSkipLine.length() > 0 ) pointerSkipStats.println( pointerSkipLine );
						if ( bitSkipLine.length() > 0 ) bitSkipStats.println( bitSkipLine );
					}
				}

				toTheEnd += skip[k].writtenBits();

				if ( DEBUG ) {
					if ( doinIt ) System.err.print( " (" + (int) skip[k].writtenBits() + " bits)" );
				}

				towerData.numberOfLowerEntries += s;
				towerData.numberOfSkipTowers++;

				if ( DEBUG ) {
					if ( doinIt ) System.err.println();
				}
			}

			distance[ k ] = toTheEnd;

			// Where are we? Just before the beginning of the k-th skip tower
			toTheEnd += cachePointer[ k ].writtenBits();

			// Where are we? Just before the beginning of the k-th document record
		}
	}

	/** Write out the cache content.
	 * 
	 * @param pointer the first pointer of the next block, or -1 if this is the last block.
	 */
	private void writeOutCache( final long pointer ) throws IOException {
		if ( DEBUG ) System.err.println( "Entered writeOutCache() with cache=" + cache + " (H is " + ( 1L << height ) + ", B is " + w + ")" );

		cacheDataLength[ (int)( ( ( cache + quantum - 1 ) >>> quantumDivisionShift ) - 1 ) ] = cacheDataOut.writtenBits();

		/* Number of bits to go after the first pointer of the first record of the next block (or, if there
		   is no other block in the current list, to go to the end of the list). */
		long toTheEnd;

		// Record the new document pointer for the highest tower
		int nextAfter = (int)( ( ( cache + quantum ) - 1 ) >>> quantumDivisionShift ); // This is ceil( cache / q )

		if ( pointer >= 0 ) {
			skipPointer[nextAfter] = pointer;
			toTheEnd = writeOutPointer( bitCount, pointer );
		} else {
			skipPointer[nextAfter] = currentDocument + 1; // Fake: just for the last block
			toTheEnd = 0;
		}

		distance[nextAfter] = 0;

		int k, s;
		long d;

		// Compute quantum length in bits (without towers)
		int quantumBitLength = 0, entryBitLength = 0;

		for ( d = k = 0; k <= ( ( cache - 1 ) >>> quantumDivisionShift ); k++ ) d += ( cachePointer[k].writtenBits() + cacheDataLength[ k ] );
		quantumBitLength = (int)( ( ( d * quantum ) + ( cache - 1 ) ) / cache );

		final TowerData td = new TowerData();
		final Int2IntRBTreeMap candidates = new Int2IntRBTreeMap(); 

		/* As a first try, we compute the tower costs using 0 as average entry bit length. */
		tryTower( quantumBitLength, 0, toTheEnd, cacheSkipBitCount, td, false );
		
		if ( td.numberOfSkipTowers > 0 ) { // There actually is at least a tower.
			/* Now we repeat this operation, trying to obtain the best value for the
			 * average entry bit length. 
			 */

			while( candidates.size() < MAX_TRY && ! candidates.containsValue( entryBitLength = (int)( td.bitsForTowers() / td.numberOfEntries() ) ) ) {
				td.clear();
				tryTower( quantumBitLength, entryBitLength, toTheEnd, cacheSkipBitCount, td, false );
				candidates.put( (int)( td.bitsForTowers() / td.numberOfEntries() ), entryBitLength );
			}

			if ( ASSERTS ) assert candidates.size() < MAX_TRY;

			entryBitLength = candidates.get( candidates.firstIntKey() );

			if ( STATS ) if ( System.getProperty( "freq" ) != null ) {
				final double freq = Double.parseDouble( System.getProperty( "freq" ) );
				final double error = Integer.getInteger( "error" ).intValue() / 100.0;
				final double relativeFrequency = (double)frequency / numberOfDocuments;
				if ( ( writeStats = ( relativeFrequency >= freq * ( 1 - error ) && relativeFrequency <= freq * ( 1 + error ) ) ) ) {
					pointerSkipStats.println( "# " + currentTerm );
					pointerTopSkipStats.println( "# " + currentTerm );
					bitSkipStats.println( "# " + currentTerm + " " + quantumBitLength + " " + entryBitLength );
					bitTopSkipStats.println( "# " + currentTerm + " " + quantumBitLength + " " + entryBitLength );
				}
			}

			if ( DEBUG ) System.err.println( "Going to write tower at position " + obs.writtenBits() );
			tryTower( quantumBitLength, entryBitLength, toTheEnd, cacheSkip, towerData, true );
		}

		// Ready to write out cache
		long maxCacheDataLength = 0;
		for ( k = 0; k <= ( ( cache - 1 ) >>> quantumDivisionShift ); k++ ) if ( cacheDataLength[ k ] > maxCacheDataLength ) maxCacheDataLength = cacheDataLength[ k ];  
		
		/* We have two ways of writing out cached data. If all the data is still in the output bit
		 * stream buffer, we just read it directly. Otherwise, we have to pour it into a temporary buffer. */
		
		final byte[] buffer;
		final boolean direct;
		int pos = 0;
		
		cacheDataOut.align();

		if ( cacheDataOut.buffer() != null ) {
			buffer = cacheDataOut.buffer();
			direct = true;
		}
		else {
			cacheDataOut.flush();
			assert ( maxCacheDataLength + 7 ) / 8 <= Integer.MAX_VALUE : ( maxCacheDataLength + 7 ) / 8 + " (" + maxCacheDataLength + ")"; 
			buffer = new byte[ (int)( ( maxCacheDataLength + 7 ) / 8 ) ];
			direct = false;
			cacheDataIn.flush();
			cacheDataIn.position( 0 );
		}
		
		for ( k = 0; k <= ( ( cache - 1 ) >>> quantumDivisionShift ); k++ ) {

			/* See comments above. */
			s = ( k == 0 ) ? height : Integer.numberOfTrailingZeros( k );

			if ( cache < w ) s = Math.min( s, Fast.mostSignificantBit( ( cache >>> quantumDivisionShift ) - k ) );

			d = cachePointer[k].writtenBits();
			cachePointer[k].flush();
			obs.write( cachePointerByte[k].array, d );

			d = cacheSkip[k].writtenBits();
			cacheSkip[k].flush();

			if ( s >= 0 ) {
				if ( k == 0 ) {
					if ( prevQuantumBitLength < 0 ) {
						bitsForQuantumBitLengths += obs.writeLongDelta( quantumBitLength );
						bitsForEntryBitLengths += obs.writeLongDelta( entryBitLength );
					}
					else {
						bitsForQuantumBitLengths += obs.writeLongDelta( Fast.int2nat( quantumBitLength - prevQuantumBitLength ) );
						bitsForEntryBitLengths += obs.writeLongDelta( Fast.int2nat( entryBitLength - prevEntryBitLength ) );
					}

					prevQuantumBitLength = quantumBitLength;
					prevEntryBitLength = entryBitLength;

					numberOfBlocks++;
				}

				if ( s > 0 ) obs.writeDelta( Fast.int2nat( (int)d - entryBitLength * ( s + 1 ) ) ); // No length for single-entry towers.
			} else if ( ASSERTS ) assert d == 0;

			obs.write( cacheSkipByte[k].array, d );
			
			if ( direct ) {
				obs.write( buffer, pos * 8, cacheDataLength[ k ] );
				pos += ( cacheDataLength[ k ] + 7 ) / 8;
			}
			else {
				assert ( cacheDataLength[ k ] + 7 ) / 8 <= Integer.MAX_VALUE : ( cacheDataLength[ k ] + 7 ) / 8 + " (" + cacheDataLength[ k ] + ")";
				cacheDataIn.read( buffer, 0, (int)( ( cacheDataLength[ k ] + 7 ) / 8 ) );
				obs.write( buffer, cacheDataLength[ k ] );
			}
		}

		// Clean used caches
		for ( k = 0; k <= ( ( cache - 1 ) >>> quantumDivisionShift ); k++ ) {
			cachePointerByte[k].reset();
			cachePointer[k].writtenBits( 0 );

			cacheSkipByte[k].reset();
			cacheSkip[k].writtenBits( 0 );

			cacheDataOut.position( 0 );
			cacheDataOut.writtenBits( 0 );
		}

		cache = 0;

		if ( ASSERTS ) assert obs.writtenBits() == writtenBits();
	}

	public long writtenBits() {
		return super.writtenBits() + bitsForVariableQuanta + towerData.bitsForTopSkipPointers +
		towerData.bitsForTopBitSkips + towerData.bitsForLowerSkipPointers +
		towerData.bitsForLowerBitSkips + towerData.bitsForTowerLengths +
		bitsForQuantumBitLengths + bitsForEntryBitLengths;
	}
	
	public Properties properties() {
		Properties result = super.properties();
		result.setProperty( Index.PropertyKeys.INDEXCLASS, FileIndex.class.getName() );
		result.setProperty( BitStreamIndex.PropertyKeys.SKIPQUANTUM, variableQuanta ? 0 : quantum );
		result.setProperty( BitStreamIndex.PropertyKeys.SKIPHEIGHT, height );
		return result;	
	}

	public void printStats( final PrintStream stats ) {
		super.printStats( stats );
		stats.println( "Skip towers: " + Util.format( towerData.numberOfSkipTowers ) + " (" + 
				Util.format( towerData.bitsForTowers() + bitsForVariableQuanta ) + " bits [" + 
				Util.format( ( towerData.bitsForTowers() + bitsForVariableQuanta ) *100.0 / bitsForListsWithTowers ) + "%, overall " +
				Util.format( ( towerData.bitsForTowers() + bitsForVariableQuanta ) *100.0 / ( obs.writtenBits() ) ) + "%], " +
				Util.format( ( towerData.bitsForTowers() + bitsForVariableQuanta ) / (double)towerData.numberOfSkipTowers ) + " bits/tower)" );
		stats.println( "Skip entries: " + Util.format( towerData.numberOfEntries() ) + " (" + 
				Util.format( towerData.bitsForEntries() / (double)towerData.numberOfEntries()) + " bits/entry)" );
		// Note that lengths are written approximately every other tower.
		stats.println( "Skip tower lengths: " + Util.format( towerData.bitsForTowerLengths ) + " bits (" + Util.format( 2.0 * towerData.bitsForTowerLengths/ towerData.numberOfSkipTowers ) + " bits/tower)" );
		stats.println( "Quantum bit lengths: " + Util.format( bitsForQuantumBitLengths ) + " bits (" + Util.format( bitsForQuantumBitLengths/ (double)numberOfBlocks ) + " bits/block)" );
		stats.println( "Entry bit lengths: " + Util.format(bitsForEntryBitLengths ) + " bits (" + Util.format( bitsForEntryBitLengths/ (double)numberOfBlocks ) + " bits/block)" );
		if ( variableQuanta ) stats.println( "Variable quanta: " + Util.format( bitsForVariableQuanta ) + " bits (" + Util.format( bitsForVariableQuanta / ( currentTerm + 1.0 ) ) + " bits/list)" );
		
		stats.println( "Top bit skips: " + Util.format(towerData.bitsForTopBitSkips ) + " bits (" + Util.format( towerData.bitsForTopBitSkips / (double)towerData.numberOfTopEntries ) + " bits/skip)" );
		stats.println( "Top pointer skips: " + Util.format( towerData.bitsForTopSkipPointers ) + " bits (" + Util.format( towerData.bitsForTopSkipPointers/ (double)towerData.numberOfTopEntries ) + " bits/skip)" );
		stats.println( "Lower bit skips: " + Util.format( towerData.bitsForLowerBitSkips ) + " bits (" + Util.format( towerData.bitsForLowerBitSkips/ (double)towerData.numberOfLowerEntries ) + " bits/skip)" );
		stats.println( "Lower pointer skips: " + Util.format( towerData.bitsForLowerSkipPointers ) + " bits (" + Util.format( towerData.bitsForLowerSkipPointers/ (double)towerData.numberOfLowerEntries ) + " bits/skip)" );
		stats.println( "Bit skips: " + Util.format( towerData.bitsForBitSkips() ) + " bits (" + Util.format( towerData.bitsForBitSkips()/ (double)towerData.numberOfEntries() ) + " bits/skip)" );
		stats.println( "Pointer skips: " + Util.format( towerData.bitsForSkipPointers() ) + " bits (" + Util.format( towerData.bitsForSkipPointers()/ (double)towerData.numberOfEntries() ) + " bits/skip)" );
	}

}
