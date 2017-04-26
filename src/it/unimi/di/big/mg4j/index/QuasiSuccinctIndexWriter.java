package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2012-2016 Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.dsi.Util;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

/** An index writer for {@linkplain QuasiSuccinctIndex quasi-succinct indices}. 
 * 
 * @author Sebastiano Vigna
 */

public class QuasiSuccinctIndexWriter implements IndexWriter {

	/** The default size of the bit cache. */
	public final static int DEFAULT_CACHE_SIZE = 16 * 1024 * 1024;
	
	
	/** Returns the number of lower bits for the Elias&ndash;Fano encoding of a list of given
	 * length, upper bound and strictness.
	 * 
	 * @param length the number of elements of the list.
	 * @param upperBound an upper bound for the elements of the list.
	 * @param strict if true, the elements of the list are strictly increasing, and the
	 * returned number of bits is for the strict representation (e.g., storing the
	 * <var>k</var>-th element decreased by <var>k</var>).
	 * @return the number of bits for the Elias&ndash;Fano encoding of a list with the
	 * specified parameters.
	 */
	public static int lowerBits( final long length, final long upperBound, final boolean strict ) {
		return length == 0 ? 0 : Math.max( 0, Fast.mostSignificantBit( ( upperBound - ( strict ? length : 0 ) ) / length ) );
	}
	
	/** Returns the size in bits of forward or skip pointers to the Elias&ndash;Fano encoding of a list of given
	 * length, upper bound and strictness.
	 * 
	 * @param length the number of elements of the list.
	 * @param upperBound an upper bound for the elements of the list.
	 * @param strict if true, the elements of the list are strictly increasing, and the
	 * returned number of bits is for the strict representation (e.g., storing the
	 * <var>k</var>-th element decreased by <var>k</var>).
	 * @param indexZeroes if true, the number of bits for skip pointers is returned; otherwise,
	 * the number of bits for forward pointers is returned.
	 * @return the size of bits of forward or skip pointers the Elias&ndash;Fano encoding of a list with the
	 * specified parameters.
	 */
	public static int pointerSize( final long length, final long upperBound, final boolean strict, final boolean indexZeroes ) {
		// Note that if we index ones it might happen that a pointer points just after the end of the bit stream.
		return Math.max(  0, Fast.ceilLog2( length + ( ( upperBound - ( strict ? length : 0 ) ) >>> lowerBits( length, upperBound, strict ) ) + ( indexZeroes ? 0 : 1 ) ) );
	}

	/** Returns the number of forward or skip pointers to the Elias&ndash;Fano encoding of a list of given
	 * length, upper bound and strictness.
	 * 
	 * @param length the number of elements of the list.
	 * @param upperBound an upper bound for the elements of the list.
	 * @param log2Quantum the logarithm of the quantum size.
	 * @param strict if true, the elements of the list are strictly increasing, and the
	 * returned number of bits is for the strict representation (e.g., storing the
	 * <var>k</var>-th element decreased by <var>k</var>).
	 * @param indexZeroes if true, an upper bound on the number of skip pointers is returned; otherwise,
	 * the (exact) number of forward pointers is returned.
	 * @return an upper bound on the number of skip pointers or
	 * the (exact) number of forward pointers.
	 */
	public static long numberOfPointers( final long length, final long upperBound, final int log2Quantum, final boolean strict, final boolean indexZeroes ) {
		if ( length == 0 ) return 0;
		if ( indexZeroes ) return ( ( upperBound - ( strict ? length : 0 ) ) >>> lowerBits( length, upperBound, strict ) ) >>> log2Quantum;
		return length >>> log2Quantum;
	}

	protected final static class LongWordCache implements Closeable {
		private static final boolean ASSERTS = true;
		/** The spill file. */
		private final File spillFile;
		/** A channel opened on {@link #spillFile}. */
		private final FileChannel spillChannel;
		/** A cache for longwords. Will be spilled to {@link #spillChannel} in case more than {@link #cacheLength} bits are added. */
		private final ByteBuffer cache;
		/** The current bit buffer. */
		private long buffer;
		/** The current number of free bits in {@link #buffer}. */
		private int free;
		/** The length of the cache, in bits. */
		private long cacheLength;
		/** The number of bits currently stored. */ 
		private long length;
		/** Whether {@link #spillChannel} should be repositioned at 0 before usage. */
		private boolean spillMustBeRewind;
		
		@SuppressWarnings("resource")
		public LongWordCache( final int cacheSize, final String suffix ) throws IOException {
			spillFile = File.createTempFile( QuasiSuccinctIndexWriter.class.getName(), suffix );
			spillFile.deleteOnExit();
			spillChannel = new RandomAccessFile( spillFile, "rw" ).getChannel();
			cache = ByteBuffer.allocateDirect( cacheSize ).order( ByteOrder.nativeOrder() );
			cacheLength = cacheSize * 8L;
			free = Long.SIZE;
		}

		private void flushBuffer() throws IOException {
			cache.putLong( buffer );
			if ( ! cache.hasRemaining() ) {
				if ( spillMustBeRewind ) {
					spillMustBeRewind = false;
					spillChannel.position( 0 );
				}
				cache.flip();
				spillChannel.write( cache );
				cache.clear();
			}
		}

		public int append( final long value, final int width ) throws IOException {
			if ( ASSERTS ) assert width == Long.SIZE || ( -1L << width & value ) == 0; 
			buffer |= value << ( Long.SIZE - free );
			length += width;
			
			if ( width < free ) free -= width;
			else {
				flushBuffer();
				
				if ( width == free ) {
					buffer = 0;
					free = Long.SIZE;
				}
				else {
					// free < Long.SIZE
					buffer = value >>> free;
					free = Long.SIZE - width + free; // width > free
				}
			}
			return width;
		}
		
		public void clear() {
			length = buffer = 0;
			free = Long.SIZE;
			cache.clear();
			spillMustBeRewind = true;
		}
		
		@Override
		public void close() throws IOException {
			spillChannel.close();
			spillFile.delete();
		}
		
		public long length() {
			return length;
		}
		
		public void writeUnary( int l ) throws IOException {
			if ( l >= free ) {
				// Phase 1: align
				l -= free;
				length += free;
				flushBuffer();

				// Phase 2: jump over longwords
				buffer = 0;
				free = Long.SIZE;
				while( l >= Long.SIZE ) {
					flushBuffer();
					l -= Long.SIZE;
					length += Long.SIZE;
				}
			}
			
			append( 1L << l, l + 1 );
		}
		
		public long readLong() throws IOException {
			if ( ! cache.hasRemaining() ) {
				cache.clear();
				spillChannel.read( cache );
				cache.flip();
			}
			return cache.getLong();
		}
		
		public void rewind() throws IOException {
			if ( free != Long.SIZE ) cache.putLong( buffer );

			if ( length > cacheLength ) {
				cache.flip();
				spillChannel.write( cache );
				spillChannel.position( 0 );
				cache.clear();
				spillChannel.read( cache );
				cache.flip();
			}
			else cache.rewind();
		}	
	}
	
	public final static class LongWordOutputBitStream {
		private static final int BUFFER_SIZE = 64 * 1024;
		private static final boolean ASSERTS = true;

		/** The 64-bit buffer, whose upper {@link #free} bits do not contain data. */
		private long buffer;
		/** The Java nio buffer used to write with prescribed endianness. */
		private ByteBuffer byteBuffer;
		/** The number of upper free bits in {@link #buffer} (strictly positive). */
		private int free;
		/** The output channel. */
		private WritableByteChannel writableByteChannel;
		
		public LongWordOutputBitStream( final WritableByteChannel writableByteChannel, final ByteOrder byteOrder ) {
			this.writableByteChannel = writableByteChannel;
			byteBuffer = ByteBuffer.allocateDirect( BUFFER_SIZE ).order( byteOrder );
			free = Long.SIZE;
		}

		public int append( final long value, final int width ) throws IOException {
			if ( ASSERTS ) assert width == Long.SIZE || ( -1L << width & value ) == 0; 
			buffer |= value << ( Long.SIZE - free );
			
			if ( width < free ) free -= width;
			else {
				byteBuffer.putLong( buffer ); // filled
				if ( ! byteBuffer.hasRemaining() ) {
					byteBuffer.flip();
					writableByteChannel.write( byteBuffer );
					byteBuffer.clear();
				}

				if ( width == free ) {
					buffer = 0;
					free = Long.SIZE;
				}
				else {
					// free < Long.SIZE
					buffer = value >>> free;
					free = Long.SIZE - width + free; // width > free
				}
			}
			return width;
		}
		
		public long append( final long[] value, final long length ) throws IOException {
			long l = length;
			for( int i = 0; l > 0; i++ ) {
				final int width = (int)Math.min( l, Long.SIZE );
				append( value[ i ], width );
				l -= width;
			}
			
			return length;
		}

		public long append( final LongArrayBitVector bv ) throws IOException {
			return append( bv.bits(), bv.length() );
		}

		public long append( final LongWordCache cache ) throws IOException {
			long l = cache.length();
			cache.rewind();
			while( l > 0 ) {
				final int width = (int)Math.min( l, Long.SIZE );
				append( cache.readLong(), width );
				l -= width;
			}
			
			return cache.length();
		}
		
		public int align() throws IOException {
			if ( free != Long.SIZE ) {
				byteBuffer.putLong( buffer ); // partially filled
				if ( ! byteBuffer.hasRemaining() ) {
					byteBuffer.flip();
					writableByteChannel.write( byteBuffer );
					byteBuffer.clear();
				}

				final int result = free;
				buffer = 0;
				free = Long.SIZE;
				return result;
			}
			
			return 0;
		}

		public int writeNonZeroGamma( long value ) throws IOException {
			if ( value <= 0 ) throw new IllegalArgumentException( "The argument " + value + " is not strictly positive." );
			final int msb = Fast.mostSignificantBit( value );
			final long unary = 1L << msb;
			append( unary, msb + 1 );
			append( value ^ unary, msb );
			return 2 * msb + 1;
		}
		
		public int writeGamma( long value ) throws IOException {
			if ( value < 0 ) throw new IllegalArgumentException( "The argument " + value + " is negative." );
			return writeNonZeroGamma( value + 1 );
		}
		
		public void close() throws IOException {
			byteBuffer.putLong( buffer );
			byteBuffer.flip();
			writableByteChannel.write( byteBuffer );
			writableByteChannel.close();
		}
	}

	protected final static class Accumulator implements Closeable {
		private static final boolean ASSERTS = true;
		/** The minimum size in bytes of a {@link LongWordCache}. */
		private static final int MIN_CACHE_SIZE = 16;
		/** The accumulator for pointers (to zeros or ones). */
		private final LongWordCache pointers;
		/** The accumulator for high bits. */
		private final LongWordCache upperBits;
		/** The accumulator for low bits. */
		private final LongWordCache lowerBits;
		/** If true, {@link #add(long)} does not accept zeroes. */
		private boolean strict;
		/** The number of lower bits. */
		private int l;
		/** A mask extracting the {@link #l} lower bits. */
		private long lowerBitsMask;
		/** The number of elements that will be added to this list. */
		private long length;
		/** The current length of the list. */
		private long currentLength;
		/** The current prefix sum (decremented by {@link #currentLength} if {@link #strict} is true). */
		private long currentPrefixSum;
		/** An upper bound to the sum of all values that will be added to the list (decremented by {@link #currentLength} if {@link #strict} is true). */
		private long correctedUpperBound;
		/** The logarithm of the indexing quantum. */
		private int log2Quantum;
		/** The indexing quantum. */
		private long quantum;
		/** The size of a pointer (the ceiling of the logarithm of {@link #maxUpperBits}). */
		private int pointerSize;
		/** The mask to decide whether to quantize. */
		private long quantumMask;
		/** Whether we should index ones or zeroes. */
		private boolean indexZeroes;
		/** If true, we are writing a ranked characteristic function. */
		private boolean ranked;
		/** The last position where a one was set. */
		private long lastOnePosition;
		/** The expected number of points. */
		private long expectedNumberOfPointers;
		/** The number of bits used for the upper-bits array. */
		public long bitsForUpperBits;
		/** The number of bits used for the lower-bits array. */
		public long bitsForLowerBits;
		/** The number of bits used for forward/skip pointers. */
		public long bitsForPointers;

		public Accumulator( int bufferSize, int log2Quantum ) throws IOException {
			// A reasonable logic to allocate space.
			bufferSize = bufferSize & -bufferSize; // Ensure power of 2.
			/* Very approximately, half of the cache for lower, half for upper, and a small fraction (8/quantum) for pointers.
			 * This will generate a much larger cache than expected if quantum is very small. */
			pointers = new LongWordCache( Math.max( MIN_CACHE_SIZE, bufferSize >>> Math.max( 3, log2Quantum - 3 ) ), "pointers" );
			lowerBits = new LongWordCache( Math.max( MIN_CACHE_SIZE, bufferSize / 2 ), "lower" );
			upperBits = new LongWordCache( Math.max( MIN_CACHE_SIZE, bufferSize / 2 ), "upper" );
		}

		public int lowerBits() {
			return l;
		}
		
		public int pointerSize() {
			return pointerSize;
		}
		
		public long numberOfPointers() {
			return expectedNumberOfPointers;
		}
		
		public void init( final long length, final long upperBound, final boolean strict, final boolean indexZeroes, final int log2Quantum ) {
			this.indexZeroes = indexZeroes;
			this.log2Quantum = log2Quantum;
			this.length = length;
			this.strict = strict;
			quantum = 1L << log2Quantum;
			quantumMask = quantum - 1;
			pointers.clear();
			lowerBits.clear();
			upperBits.clear();
			correctedUpperBound = upperBound - ( strict ? length : 0 );
			final long correctedLength = length + ( ! strict && indexZeroes ? 1 : 0 ); // The length including the final terminator
			if ( correctedUpperBound < 0 ) throw new IllegalArgumentException();
			
			currentPrefixSum = 0;
			currentLength = 0;
			lastOnePosition = -1;
			
			l = QuasiSuccinctIndexWriter.lowerBits( correctedLength, upperBound, strict );
			
			ranked = correctedLength + ( upperBound >>> l ) + correctedLength * l > upperBound && ! strict && indexZeroes;
			if ( ranked ) l = 0;

			lowerBitsMask = ( 1L << l ) - 1;

			pointerSize = ranked ? Fast.length( correctedLength ) : QuasiSuccinctIndexWriter.pointerSize( correctedLength, upperBound, strict, indexZeroes );
			expectedNumberOfPointers = ranked ? Math.max( 0, upperBound >>> log2Quantum ) : QuasiSuccinctIndexWriter.numberOfPointers( correctedLength, upperBound, log2Quantum, strict, indexZeroes );
		}
		
		public void add( final long x ) throws IOException {
			//System.err.println( "add(" + x + "), l = " + l + ", length = " + length );
			if ( strict && x == 0 ) throw new IllegalArgumentException( "Zeroes are not allowed." );
			currentPrefixSum += x - ( strict ? 1 : 0 );
			if ( currentPrefixSum > correctedUpperBound ) throw new IllegalArgumentException( "Too large prefix sum: " + currentPrefixSum + " >= " + correctedUpperBound );
			if ( l != 0 ) lowerBits.append( currentPrefixSum & lowerBitsMask, l );
			final long onePosition = ranked ? currentPrefixSum : ( currentPrefixSum >>> l ) + currentLength; 
			
			upperBits.writeUnary( (int)( onePosition - lastOnePosition - 1 ) );
			
			if ( ranked ) {
				for( long position = lastOnePosition + quantum & -1L << log2Quantum; position <= onePosition; position += quantum )
					if ( position != 0 ) pointers.append( currentLength, pointerSize );
			}
			else if ( indexZeroes ) {
				long zeroesBefore = lastOnePosition - currentLength + 1;
				for( long position = lastOnePosition + ( zeroesBefore & -1L << log2Quantum ) + quantum - zeroesBefore; position < onePosition; position += quantum, zeroesBefore += quantum )
					pointers.append( position + 1, pointerSize );
			}
			else if ( ( currentLength + 1 & quantumMask ) == 0 ) pointers.append( onePosition + 1, pointerSize );

			lastOnePosition = onePosition;
			currentLength++;
		}
		
		public long dump( final LongWordOutputBitStream lwobs ) throws IOException {
			if ( currentLength != length ) throw new IllegalStateException();
			if ( ! strict && indexZeroes ) {
				// Add last fictional document pointer equal to the number of documents.
				if ( ranked ) {
					if ( lastOnePosition >= correctedUpperBound ) throw new IllegalStateException( "The last written pointer is " + lastOnePosition + " >= " + correctedUpperBound );
					add( correctedUpperBound - lastOnePosition );
				}
				else add( correctedUpperBound - currentPrefixSum );
			}
			if ( ASSERTS ) assert ! ranked || pointers.length() / pointerSize == expectedNumberOfPointers : "Expected " + expectedNumberOfPointers + " pointers for ranked index, found " + pointers.length() / pointerSize;
			if ( indexZeroes && pointerSize != 0 ) for( long actualPointers = pointers.length() / pointerSize; actualPointers++ < expectedNumberOfPointers; ) pointers.append( 0, pointerSize );
			if ( ASSERTS ) assert pointerSize == 0 || pointers.length() / pointerSize == expectedNumberOfPointers : "Expected " + expectedNumberOfPointers + " pointers, found " + pointers.length() / pointerSize;
			//System.err.println("pointerSize :" + pointerSize );
			bitsForPointers = lwobs.append( pointers );
			//System.err.println("lower: " + result );
			bitsForLowerBits = lwobs.append( lowerBits );
			//System.err.println("upper: " + result );
			bitsForUpperBits = lwobs.append( upperBits );
			//System.err.println("end: " + result );
			return bitsForLowerBits + bitsForUpperBits + bitsForPointers;
		}

		@Override
		public void close() throws IOException {
			pointers.close();
			upperBits.close();
			lowerBits.close();
		}
	}

	/** The number of documents in the collection. */
	private final long numberOfDocuments;
	/** The logarithm of the quantum. */
	private final int log2Quantum;
	/** The accumulator storing pointers of the current posting list. */
	private Accumulator pointersAccumulator;
	/** The accumulator storing counts of the current posting list. */
	private Accumulator countsAccumulator;
	/** The accumulator storing positions of the current posting list, if {@link #positions} is not <code>null</code>. */
	private Accumulator positionsAccumulator;
	/** The longword output bit stream storing pointers. */
	private LongWordOutputBitStream pointers;
	/** The longword output bit stream storing counts. */
	private LongWordOutputBitStream counts;
	/** The longword output bit stream storing positions, or <code>null</code> if we're not storing positions. */
	private LongWordOutputBitStream positions;
	/** The output bit stream storing offsets into {@link #pointers}. */
	private OutputBitStream pointersOffsets;
	/** The output bit stream storing offsets into {@link #counts}. */
	private OutputBitStream countsOffsets;
	/** The output bit stream storing offsets into {@link #positions} if the latter is not <code>null</code>. */
	private OutputBitStream positionsOffsets;
	/** The sum of maximum positions of the current term. */
	private OutputBitStream sumsMaxPos;
	/** Stats for pointers. */
	private Stats pointersStats;
	/** Stats for counts, in {@link #counts} is not <code>null</code>. */
	private Stats countsStats;
	/** Stats for positions, if {@link #positions} is not <code>null</code>. */
	private Stats positionsStats;
	/** The last stored pointer. */
	private long lastPointer;
	/** The current term. */
	private long currentTerm;
	/** The occurrency of the current term (as passed to {@link #newInvertedList(int, long, long)}). */
	private long occurrency;
	/** The frequency of the current term (as passed to {@link #newInvertedList(int, long, long)}). */
	private long frequency;
	/** The overall number of occurrences in the index so far. */
	private long numberOfOccurrences;
	/** The overall number of postings in the index so far. */
	private long numberOfPostings;
	/** The maximum count in the index so far. */
	private int maxCount;
	/** The output bit stream for frequencies. */
	private OutputBitStream frequencies;
	/** The output bit stream for occurrency. */
	private OutputBitStream occurrencies;
	/** The endianness of this index */
	private final ByteOrder byteOrder;

	/** Creates a new index writer, with the specified basename.
	 *  
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the basename.
	 * @param numberOfDocuments the number of documents in the collection to be indexed.
	 * @param log2Quantum the logarithm of the quantum.
	 * @param cacheSize the size in byte of the bit caches.
	 * @param byteOrder the byte order of the index (if <code>null</code>, {@link ByteOrder#nativeOrder()}).
	 */
	public QuasiSuccinctIndexWriter( final IOFactory ioFactory, final CharSequence basename, final long numberOfDocuments, final int log2Quantum, int cacheSize, final Map<Component,Coding> flags, ByteOrder byteOrder ) throws IOException {
		if ( log2Quantum < 0 ) throw new IllegalArgumentException( Integer.toString( log2Quantum ) );
		this.numberOfDocuments = numberOfDocuments;
		this.log2Quantum = log2Quantum;
		this.byteOrder = byteOrder == null ? ByteOrder.nativeOrder() : byteOrder;
		
		maxCount = -1;
		
		/* We divide the cache size as follows: pointers, 18.75%; counts, 6.25%; positions, 75%.
		 * If positions are not present, we artificially enlarge the cache size so that
		 * we have the right space usage, with the same ratio between pointers and counts. */

		if ( ! flags.containsKey( Component.POSITIONS ) ) cacheSize *= 4;

		pointersAccumulator = new Accumulator( 3 * cacheSize / 16, log2Quantum );
		pointers = new LongWordOutputBitStream( ioFactory.getWritableByteChannel( basename + DiskBasedIndex.POINTERS_EXTENSIONS  ), byteOrder );
		pointersOffsets = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.POINTERS_EXTENSIONS + DiskBasedIndex.OFFSETS_POSTFIX  ), false);

		if ( flags.containsKey( Component.COUNTS ) ) {
			occurrencies = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.OCCURRENCIES_EXTENSION ), false );
			countsAccumulator = new Accumulator( cacheSize / 16, log2Quantum );
			counts = new LongWordOutputBitStream( ioFactory.getWritableByteChannel( basename + DiskBasedIndex.COUNTS_EXTENSION  ), byteOrder );
			countsOffsets = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.COUNTS_EXTENSION + DiskBasedIndex.OFFSETS_POSTFIX ), false );
		}
			
		if ( flags.containsKey( Component.POSITIONS ) ) {
			positionsAccumulator = new Accumulator( 3 * cacheSize / 4, log2Quantum );
			positions = new LongWordOutputBitStream( ioFactory.getWritableByteChannel( basename + DiskBasedIndex.POSITIONS_EXTENSION ), byteOrder );
			positionsOffsets = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.POSITIONS_EXTENSION + DiskBasedIndex.OFFSETS_POSTFIX ), false );
			sumsMaxPos = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION ), false );
		}

		frequencies = new OutputBitStream( ioFactory.getOutputStream( basename + DiskBasedIndex.FREQUENCIES_EXTENSION ), false );
		
		pointersStats = new Stats();
		if ( counts != null ) countsStats = new Stats();
		if ( positions != null ) positionsStats = new Stats();
		pointersOffsets.writeGamma( 0 );
		if ( counts != null ) countsOffsets.writeGamma( 0 );
		if ( positions != null ) positionsOffsets.writeGamma( 0 );
		currentTerm = -1;
	}

	private final static class Stats {
		public long bitsForLowerBits;
		public long bitsForUpperBits;
		public long bitsForSkipPointers;
		public long bitsForAdditional;
		
		public void update( Accumulator a ) {
			bitsForLowerBits += a.bitsForLowerBits;
			bitsForUpperBits += a.bitsForUpperBits;
			bitsForSkipPointers += a.bitsForPointers;
		}
		
		private static String format( long v, long total ) {
			return v + " (" + Util.formatSize( v / 8 ) + "B, " + Util.format( 100. * v / total ) + "%)\n";
		}
		
		public long total() {
			return bitsForLowerBits + bitsForUpperBits + bitsForSkipPointers + bitsForAdditional;
		}
		
		public long totalNonSkip() {
			return bitsForLowerBits + bitsForUpperBits + bitsForAdditional;
		}
		
		public String toString() {
			final long total = total();
			return "Upper bits: " + format( bitsForUpperBits, total ) +
			"Lower bits: " + format( bitsForLowerBits, total ) +
			"Skip pointers bits: " + format( bitsForSkipPointers, total ) +
			"Additional bits: " + format( bitsForAdditional, total ) +
			"Overall bits: " + total + " (" + Util.formatSize( total / 8 ) + "B)\n";
		}
	}

	/** Starts a new inverted list. The previous inverted list, if any, is actually written
	 * to the underlying bit stream.
	 *  
	 * <p>This method provides additional information which is necessary to build the posting list.
	 * The information can be omitted if only part of the index is being written (e.g., no positions
	 * or even no counts and positions).
	 * 
	 * @param frequency the frequency of the inverted list.
	 * @param occurrency the occurrency of the inverted list (use -1 if you are not writing counts).
	 * @param sumMaxPos the sum of the maximum position in each document (unused if positions are not indexed).
	 * @throws IllegalStateException if too few records were written for the previous inverted list.
	 * @see IndexWriter#newInvertedList()
	 */
	public void newInvertedList( final long frequency, final long occurrency, final long sumMaxPos ) throws IOException {
		if ( currentTerm++ != -1 ) flushAccumulators();

		this.frequency = frequency;
		this.occurrency = occurrency;

		numberOfPostings += frequency;
		numberOfOccurrences += occurrency;
		lastPointer = 0;

		frequencies.writeLongGamma( frequency );
		if ( occurrencies != null ) occurrencies.writeLongGamma( occurrency );
		if ( sumsMaxPos != null ) sumsMaxPos.writeLongDelta( sumMaxPos );
				
		pointersAccumulator.init( frequency, numberOfDocuments, false, true, log2Quantum );
		if ( counts != null ) countsAccumulator.init( frequency, occurrency, true, false, log2Quantum );
		if ( positions != null ) positionsAccumulator.init( occurrency, sumMaxPos + frequency, true, false, log2Quantum );
	}

	@Override
	public long newInvertedList() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeFrequency( final long frequency ) {}

	@Override
	public OutputBitStream newDocumentRecord() throws IOException {
		return null;
	}

	@Override
	public void writeDocumentPointer( final OutputBitStream out, final long pointer ) throws IOException {
		pointersAccumulator.add( pointer - lastPointer );
		lastPointer = pointer;
	}

	@Override
	public void writePayload( final OutputBitStream unused, final Payload payload ) throws IOException {
		throw new IllegalStateException( "Quasi-succinct indices do not support payloads" );
	}

	@Override
	public void writePositionCount( final OutputBitStream unused, final int count ) throws IOException {
		countsAccumulator.add( count );
	}

	@Override
	public void writeDocumentPositions( final OutputBitStream unused, final int[] position, final int offset, final int count, final int docSize ) throws IOException {
		positionsAccumulator.add( position[ offset ] + 1 );
		for( int j = 1; j < count; j++ ) positionsAccumulator.add( position[ offset + j ] - position[ offset + j - 1 ] );
		if ( count > maxCount ) maxCount = count;
	}

	@Override
	public long writtenBits() {
		return pointersStats.total() + ( counts != null ? countsStats.total() : 0 ) + ( positions != null ? positionsStats.total() : 0 );
	}

	@Override
	public Properties properties() {
		Properties properties = new Properties();
		properties.setProperty( Index.PropertyKeys.DOCUMENTS, numberOfDocuments );
		properties.setProperty( Index.PropertyKeys.TERMS, currentTerm + 1 );
		properties.setProperty( Index.PropertyKeys.POSTINGS, numberOfPostings );
		properties.setProperty( Index.PropertyKeys.MAXCOUNT, maxCount );
		properties.setProperty( Index.PropertyKeys.INDEXCLASS, QuasiSuccinctIndex.class.getName() );
		properties.setProperty( BitStreamIndex.PropertyKeys.SKIPQUANTUM, 1L << log2Quantum );
		properties.setProperty( QuasiSuccinctIndex.PropertyKeys.BYTEORDER, byteOrder.toString() );
		
		if ( counts == null ) properties.addProperty( Index.PropertyKeys.CODING, CompressionFlags.Component.COUNTS + ":" + CompressionFlags.NONE );
		if ( positions == null ) properties.addProperty( Index.PropertyKeys.CODING, CompressionFlags.Component.POSITIONS + ":" + CompressionFlags.NONE );
		return properties;
	}

	@Override
	public void close() throws IOException {
		if ( currentTerm != -1 ) flushAccumulators();
		
		pointersAccumulator.close();
		pointersOffsets.close();
		pointers.close();
		
		if ( counts != null ) {
			countsAccumulator.close();
			countsOffsets.close();
			counts.close();
			if ( positions != null ) {
				positionsAccumulator.close();
				positionsOffsets.close();
				positions.close(); 
			}
		}

		frequencies.close();
		if ( sumsMaxPos != null ) sumsMaxPos.close();		
		if ( occurrencies != null ) occurrencies.close();
	}

	@Override
	public void printStats( PrintStream stats ) {
		stats.println( "Pointers" );
		stats.println( "=========" );
		stats.println( pointersStats );
		stats.println( "Bits per pointer: " + Util.format( (double)pointersStats.totalNonSkip() / numberOfPostings ) + " (" + Util.format( (double)pointersStats.total() / numberOfPostings )  + " with skips)"  );
		long size = pointersStats.total();
		if ( counts != null ) {
			stats.println();
			stats.println( "Counts" );
			stats.println( "=========" );
			stats.println( countsStats );
			stats.println( "Bits per count: " + Util.format( (double)countsStats.totalNonSkip() / numberOfPostings ) + " (" + Util.format( (double)countsStats.total() / numberOfPostings )  + " with skips)"  );
			size += countsStats.total();
			if ( positions != null ) {
				stats.println();
				stats.println( "Positions" );
				stats.println( "=========" );
				stats.println( positionsStats );
				stats.println( "Bits per position: " + Util.format( (double)positionsStats.totalNonSkip() / numberOfOccurrences ) + " (" + Util.format( (double)positionsStats.total() / numberOfOccurrences )  + " with skips)" );
				size += positionsStats.total();
			}
		}
		
		stats.println();
		stats.println( "Size: " + size + " bits (" + Util.formatSize( size / 8 ) + "B)" );
	}

	private void flushAccumulators() throws IOException {
		// Kluge: we must have an occurrency even when not writing counts.
		if ( counts == null && occurrency <= 0 ) occurrency = frequency;
		// Write occurrency
		int occurrencyBits = pointers.writeNonZeroGamma( occurrency );
		// If non-hapax, write gap'd occurrency.
		int frequencyBits = occurrency == 1 ? 0 : pointers.writeGamma( occurrency - frequency );
		pointersStats.bitsForAdditional += frequencyBits;

		long pointersBits = pointersAccumulator.dump( pointers );
		pointersStats.update( pointersAccumulator );
		pointersOffsets.writeLongGamma( frequencyBits + occurrencyBits + pointersBits );

		if ( counts != null ) {
			long countsBits = countsAccumulator.dump( counts );
			countsStats.update( countsAccumulator );
			countsOffsets.writeLongGamma( countsBits );
			if ( positions != null ) {
				int positionsLBits = positions.writeGamma( positionsAccumulator.lowerBits() );
				positionsStats.bitsForAdditional += occurrencyBits;
				positionsStats.bitsForAdditional += positionsLBits;
				int positionsSkipPointersSizeBits = positionsAccumulator.numberOfPointers() == 0 ? 0 : positions.writeNonZeroGamma( positionsAccumulator.pointerSize() );
				positionsStats.bitsForAdditional += positionsSkipPointersSizeBits;
				long positionsBits = positionsAccumulator.dump( positions ) ;
				positionsStats.update( positionsAccumulator );
				positionsOffsets.writeLongGamma( positionsLBits + positionsSkipPointersSizeBits + positionsBits );
			}
		}
	}
}
