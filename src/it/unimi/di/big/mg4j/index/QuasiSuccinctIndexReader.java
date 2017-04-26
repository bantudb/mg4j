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


import static it.unimi.dsi.bits.Fast.MSBS_STEP_8;
import static it.unimi.dsi.bits.Fast.ONES_STEP_4;
import static it.unimi.dsi.bits.Fast.ONES_STEP_8;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;

/** An {@linkplain IndexReader index reader} for {@linkplain QuasiSuccinctIndex quasi-succinct indices}. */

public class QuasiSuccinctIndexReader extends AbstractIndexReader implements IndexReader {
	/** An index iterator that can read Elias-Fano pointer lists. */
	protected final EliasFanoIndexIterator eliasFanoIndexIterator;
	/** An index iterator that can read ranked pointer lists. */
	protected final RankedIndexIterator rankedIndexIterator;
	/** The index, cached. */
	protected final QuasiSuccinctIndex index;
	/** A reference to the data for pointers. */
	protected final LongBigList pointersList;
	/** A reference to the data for counts, or <code>null</code> of {@link #index} has counts. */
	protected final LongBigList countsList;
	/** A reference to the data for positions, or <code>null</code> of {@link #index} has positions. */
	protected final LongBigList positionsList;
	/** A longword bit reader used to check whether a posting list is ranked or not. */
	protected final LongWordBitReader pointersLongWordBitReader;
	/** A longword bit reader used to retrieve values stored in the list of positions. */
	protected final LongWordBitReader positionsLongWordBitReader;
	/** The global current term ({@link #eliasFanoIndexIterator} and {@link #rankedIndexIterator} have both a similar local variable). */
	private long currentTerm;

	public QuasiSuccinctIndexReader( final QuasiSuccinctIndex index ) {
		this.index = index;
		pointersList = index.getPointersList();
		countsList = index.hasCounts ? index.getCountsList() : null;
		positionsList = index.hasPositions ? index.getPositionsList() : null;
		pointersLongWordBitReader = new LongWordBitReader( pointersList, 0 );
		positionsLongWordBitReader = new LongWordBitReader( positionsList, 0 );
		eliasFanoIndexIterator = new EliasFanoIndexIterator( this );
		rankedIndexIterator = new RankedIndexIterator( this );
		currentTerm = -1;
	}
	
	protected final static class LongWordBitReader {

		private static final boolean DEBUG = false;

		/** The underlying list. */
		private final LongBigList list;
		/** The extraction width for {@link #extract()} and {@link #extract(long)}. */
		private final int l;
		/** {@link Long#SIZE} minus {@link #l}, cached. */
		private final int longSizeMinusl;
		/** The extraction mask for {@link #l} bits. */
		private final long mask;

		/** The 64-bit buffer, whose lower {@link #filled} bits contain data. */
		private long buffer;
		/** The number of lower used bits {@link #buffer}. */
		private int filled;
		/** The current position in the list. */
		private long curr;

		public LongWordBitReader( final LongBigList list, final int l ) {
			assert l < Long.SIZE;
			this.list = list;
			this.l = l;
			this.longSizeMinusl = Long.SIZE - l;
			mask = ( 1L << l ) - 1;
			curr = -1;
		}

		public LongWordBitReader position( final long position ) {
			if ( DEBUG ) System.err.println( this + ".position(" + position + ") [buffer = " +  Long.toBinaryString( buffer ) + ", filled = " + filled + "]" );

			buffer = list.getLong( curr = position / Long.SIZE );
			final int bitPosition = (int)( position % Long.SIZE );
			buffer >>>= bitPosition;
			filled = Long.SIZE - bitPosition;
			
			if ( DEBUG ) System.err.println( this + ".position() filled: " + filled + " buffer: " + Long.toBinaryString( buffer ));
			return this;
		}

		public long position() {
			return curr * Long.SIZE + Long.SIZE - filled; 
		}

		private long extractInternal( final int width ) {
			if ( DEBUG ) System.err.println( this + ".extract(" + width + ") [buffer = " +  Long.toBinaryString( buffer ) + ", filled = " + filled + "]" );

			if ( width <= filled ) {
				long result = buffer & ( 1L << width ) - 1;
				filled -= width;
				buffer >>>= width;
				return result;
			}
			else {
				long result = buffer;
				buffer = list.getLong( ++curr );

				final int remainder = width - filled;
				// Note that this WON'T WORK if remainder == Long.SIZE, but that's not going to happen.
				result |= ( buffer & ( 1L << remainder ) - 1 ) << filled;
				buffer >>>= remainder;
				filled = Long.SIZE - remainder;
				return result;
			}
		}
		
		public long extract() {
			if ( DEBUG ) System.err.println( this + ".extract() " + l + " bits [buffer = " +  Long.toBinaryString( buffer ) + ", filled = " + filled + "]" );

			if ( l <= filled ) {
				final long result = buffer & mask;
				filled -= l;
				buffer >>>= l;
				return result;
			}
			else {
				long result = buffer;
				buffer = list.getLong( ++curr );
				result |= buffer << filled & mask;
				// Note that this WON'T WORK if remainder == Long.SIZE, but that's not going to happen.
				buffer >>>= l - filled;
				filled += longSizeMinusl;
				return result;
			}
		}
		
		public long extract( long position ) {
			if ( DEBUG ) System.err.println( this + ".extract(" + position + ") [l=" + l + "]" );

			final int bitPosition = (int)( position % Long.SIZE );
			final int totalOffset = bitPosition + l;
			final long result = list.getLong( curr = position / Long.SIZE ) >>> bitPosition;
			
			if ( totalOffset <= Long.SIZE ) {
				buffer = result >>> l;
				filled = Long.SIZE - totalOffset;
				return result & mask;
			}
			
			final long t = list.getLong( ++curr );

			buffer = t >>> totalOffset;
			filled = 2 * Long.SIZE - totalOffset;
			
			return result | t << -bitPosition & mask;
		}
		
		public int readUnary() {
			if ( DEBUG ) System.err.println( this + ".readUnary() [buffer = " +  Long.toBinaryString( buffer ) + ", filled = " + filled + "]" );

			int accumulated = 0;

			for(;;) {
				if ( buffer != 0 ) {
					final int msb = Long.numberOfTrailingZeros( buffer );
					filled -= msb + 1;
					/* msb + 1 can be Long.SIZE, so we must break down the shift. */
					buffer >>>= msb;
					buffer >>>= 1;
					if ( DEBUG ) System.err.println( this + ".readUnary() => " + ( msb + accumulated ) );
					return msb + accumulated;
				}
				accumulated += filled;
				buffer = list.getLong( ++curr );
				filled = Long.SIZE;
			}

		}

		public long readNonZeroGamma() {
			final int msb = readUnary();
			return extractInternal( msb ) | ( 1L << msb );
		}

		public long readGamma() {
			return readNonZeroGamma() - 1;
		}
	}

	protected static class PointerReader {
		/** The underlying list. */
		protected final LongBigList list;
		/** The longword bit reader for pointers. */
		protected final LongWordBitReader skipPointers;
		/** The starting position of the pointers. */
		protected final long skipPointersStart;
		/** The starting position of the upper bits. */
		protected final long upperBitsStart;
		/** The logarithm of the quantum, cached from the index. */
		protected final int log2Quantum;
		/** The quantum, cached from the index. */
		protected final int quantum;
		/** The size of a pointer. */
		protected final int pointerSize;
		/** The number of pointers. */
		protected final long numberOfPointers;
		/** The frequency of the term (i.e., the number of elements of the current list). */
		protected final long frequency;
		/** The 64-bit window. */
		protected long window;
		/** The current word position in the list of upper bits. */
		protected long curr;
		/** The index of the current prefix sum. */
		public long currentIndex;

		public PointerReader( final LongBigList list, final long upperBitsStart, final LongWordBitReader skipPointers, final long skipPointersStart, final long numberOfPointers, final int pointerSize, final long frequency, final int log2Quantum ) {
			this.list = list;
			this.upperBitsStart = upperBitsStart;
			this.skipPointers = skipPointers;
			this.skipPointersStart = skipPointersStart;
			this.pointerSize = pointerSize;
			this.numberOfPointers = numberOfPointers;
			this.log2Quantum = log2Quantum;
			this.quantum = 1 << log2Quantum;
			this.frequency = frequency;
		}
	}

	protected final static class RankedPointerReader extends PointerReader {
		private static final boolean DEBUG = false;
		private final static int SKIPPING_THRESHOLD = 1024;

		public RankedPointerReader( final LongBigList list, final long upperBitsStart, final LongWordBitReader skipPointers, final long skipPointersStart, final long numberOfPointers, final int pointerSize, final long frequency, final int log2Quantum ) {
			super( list, upperBitsStart, skipPointers, skipPointersStart, numberOfPointers, pointerSize, frequency, log2Quantum );
			position( upperBitsStart );
		}

		private void position( final long position ) {
			window = list.getLong( curr = position / Long.SIZE ) & -1L << (int)( position );
		}

		public long getNextPrefixSum() {
			// Elegant bit-cancellation reading of the upper bits, borrowed from Philip Pronin's code for Facebook's folly library.
			while( window == 0 ) window = list.getLong( ++curr );
			final int msb = Long.numberOfTrailingZeros( window );
			window &= window - 1;
			currentIndex++;
			return curr * Long.SIZE + msb - upperBitsStart;
		}

		public long skipTo( long lowerBound ) {
			if ( DEBUG ) System.err.println( this + ".skipTo(" + lowerBound + ") [currentIndex = " + currentIndex + ", frequency = " + frequency + "]" );
			long toSkip = lowerBound - curr * Long.SIZE + upperBitsStart;

			if ( toSkip > SKIPPING_THRESHOLD ) {
				final long pointerIndex = lowerBound >>> log2Quantum;
				currentIndex = pointerIndex == 0 ? 0 : skipPointers.extract( skipPointersStart + ( pointerIndex - 1 ) * pointerSize );
				position( upperBitsStart + ( pointerIndex << log2Quantum ) );
				
				toSkip = lowerBound - curr * Long.SIZE + upperBitsStart;
				assert toSkip < Long.SIZE + quantum : toSkip;
			}
			
			long ones = 0;
			final long wordsToSkip = toSkip / Long.SIZE; 
			for( long i = wordsToSkip; i-- != 0; ) {
				ones += Long.bitCount( window );
				window = list.getLong( ++curr );
			}

			toSkip -= wordsToSkip * Long.SIZE;
			currentIndex += ones;

			assert toSkip >= 0 : toSkip;
			assert toSkip < Long.SIZE : toSkip;

			final long mask = ( 1L << toSkip ) - 1;
			currentIndex += Long.bitCount( window & mask );
			window &= ~mask;

			return getNextPrefixSum();
		}	
	}
	
	
	protected final static class EliasFanoPointerReader extends PointerReader {
		private final static int SKIPPING_THRESHOLD = 8;
		/** The number of lower bits. */
		private final int l;
		/** The longword bit reader for the lower bits. */
		private final LongWordBitReader lowerBits;
		/** The starting position of the power bits. */
		private final long lowerBitsStart;
		/** The last value returned by {@link #getNextUpperBits()}. */ 
		private long lastUpperBits;
		
		public EliasFanoPointerReader( final LongBigList list, final LongWordBitReader lowerBits, 
				final long lowerBitsStart, final int l, final LongWordBitReader skipPointers, final long skipPointersStart, final long numberOfPointers, final int pointerSize, final long frequency, final int log2Quantum ) {
			super( list, lowerBitsStart + l * ( frequency + 1L ), skipPointers, skipPointersStart, numberOfPointers, pointerSize, frequency, log2Quantum );
			this.lowerBits = lowerBits;
			this.lowerBitsStart = lowerBitsStart;
			this.l = l;
			position( upperBitsStart );
		}

		private void position( final long position ) {
			window = list.getLong( curr = position / Long.SIZE ) & -1L << (int)( position );
		}

		private long getNextUpperBits() {
			while( window == 0 ) window = list.getLong( ++curr );
			lastUpperBits = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - currentIndex++ - upperBitsStart;
			window &= window - 1;
			return lastUpperBits;
		}

		public long getNextPrefixSum() {
			return getNextUpperBits() << l | lowerBits.extract();
		}

		public long skipTo( final long lowerBound ) {
			final long zeroesToSkip = lowerBound >>> l;

			if ( zeroesToSkip - lastUpperBits < SKIPPING_THRESHOLD ) {
				long prefixSum;
				while( ( prefixSum = getNextPrefixSum() ) < lowerBound );
				return prefixSum;
			}
			
			if ( zeroesToSkip - lastUpperBits > quantum ) {
				final long block = zeroesToSkip >>> log2Quantum;
				assert block > 0;
				assert block <= numberOfPointers;
				final long blockZeroes = block << log2Quantum;
				final long skip = skipPointers.extract( skipPointersStart + ( block - 1 ) * pointerSize );
				assert skip != 0;
				position( upperBitsStart + skip );
				currentIndex = skip - blockZeroes;
			}

			long delta = zeroesToSkip - curr * Long.SIZE + currentIndex + upperBitsStart;			
			assert delta >= 0 : delta;

			for( int bitCount; ( bitCount = Long.bitCount( ~window ) ) < delta; ) {
				window = list.getLong( ++curr );
				delta -= bitCount;
				currentIndex += Long.SIZE - bitCount;
			}
			
			/* Note that for delta == 1 the following code is a NOP, but the test for zero is so faster that
	           it is not worth replacing with a > 1. Predecrementing won't work as delta might be zero. */
			if ( delta-- != 0 ) { 
				// Phase 1: sums by byte
				final long word = ~window;
				assert delta < Long.bitCount( word ) : delta + " >= " + Long.bitCount( word );
				long byteSums = word - ( ( word & 0xa * ONES_STEP_4 ) >>> 1 );
				byteSums = ( byteSums & 3 * ONES_STEP_4 ) + ( ( byteSums >>> 2 ) & 3 * ONES_STEP_4 );
				byteSums = ( byteSums + ( byteSums >>> 4 ) ) & 0x0f * ONES_STEP_8;
				byteSums *= ONES_STEP_8;

				// Phase 2: compare each byte sum with delta to obtain the relevant byte
				final long rankStep8 = delta * ONES_STEP_8;
				final long byteOffset = ( ( ( ( ( rankStep8 | MSBS_STEP_8 ) - byteSums ) & MSBS_STEP_8 ) >>> 7 ) * ONES_STEP_8 >>> 53 ) & ~0x7;

				final int byteRank = (int)( delta - ( ( ( byteSums << 8 ) >>> byteOffset ) & 0xFF ) );

				final int select = (int)( byteOffset + Fast.selectInByte[ (int)( word >>> byteOffset & 0xFF ) | byteRank << 8 ] );

				// We cancel up to, but not including, the target one.
				window &= -1L << select;
				currentIndex += select - delta;
			}

			final long lower = lowerBits.extract( lowerBitsStart + l * currentIndex );
			long prefixSum = getNextUpperBits() << l | lower; 
			
			for(;;) {
				if ( prefixSum >= lowerBound ) return prefixSum;
				prefixSum = getNextPrefixSum();
			}
		}
		
		public String toString() {
			return this.getClass().getSimpleName() + '@' + Integer.toHexString( System.identityHashCode( this ) );
		}
	}

	protected final static class CountReader {
		private static final boolean DEBUG = false;
		
		/** The longword bit reader for pointers. */
		private final LongWordBitReader skipPointers;
		/** The longword bit reader for the lower bits. */
		private final LongWordBitReader lowerBits;

		/** The underlying list. */
		private final LongBigList list;
		/** The 64-bit window. */
		private long window;
		/** The current word position in the list of upper bits. */
		private long curr;
		
		/** The starting position of the pointers. */
		private final long skipPointersStart;
		/** The starting position of the power bits. */
		private final long lowerBitsStart;
		/** The starting position of the upper bits. */
		private final long upperBitsStart;
		
		/** The number of lower bits. */
		private final int l;
		/** The size of a pointer. */
		private final int pointerSize;
		/** The number of pointers. */
		private final long numberOfPointers;
		/** The logarithm of the quantum, cached from the index. */
		private final int log2Quantum;
		/** The quantum. */
		private final int quantum;

		/** The current prefix sum (the sum of the first {@link #currentIndex} elements). */
		private long prefixSum;
		/** The previous prefix sum. */
		protected long prevPrefixSum;
		/** The index of the current prefix sum. */
		protected long currentIndex;

		public CountReader( final LongBigList list, final long position, final long frequency, final long occurrency, final int log2Quantum ) {
			this.l = QuasiSuccinctIndexWriter.lowerBits( frequency, occurrency, true );
			this.pointerSize = QuasiSuccinctIndexWriter.pointerSize( frequency, occurrency, true, false );
			numberOfPointers = QuasiSuccinctIndexWriter.numberOfPointers( frequency, -1, log2Quantum, true, false );

			skipPointers = new LongWordBitReader( list, pointerSize );
			lowerBits = new LongWordBitReader( list, l );
			this.list = list;

			skipPointersStart = position;
			lowerBitsStart = skipPointersStart + pointerSize * numberOfPointers;
			lowerBits.position( lowerBitsStart );
			upperBitsStart = lowerBitsStart + l * frequency;
			currentIndex = prevPrefixSum = prefixSum = 0;

			this.log2Quantum = log2Quantum;
			quantum = 1 << log2Quantum;

			position( upperBitsStart );
		}

		private void position( final long position ) {
			window = list.getLong( curr = position / Long.SIZE ) & -1L << (int)( position );
		}

		public long getLong( final long index ) {
			if ( DEBUG ) System.err.println( this + ".getLong(" + index + ") [currentIndex = " + currentIndex + "]" );

			long delta = index - currentIndex;

			if ( delta == 0 ) {	// shortcut
				prevPrefixSum = prefixSum;
				while( window == 0 ) window = list.getLong( ++curr );
				prefixSum = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - currentIndex++ - upperBitsStart << l | lowerBits.extract();
				window &= window - 1;
				return prefixSum - prevPrefixSum + 1;
			}

			if ( delta >= quantum ) {
				final long block = index >>> log2Quantum;
				assert block > 0;
				assert block <= numberOfPointers;
				final long skip = skipPointers.extract( skipPointersStart + ( block - 1 ) * pointerSize );
				position( upperBitsStart + skip - 1 );
				final long blockOnes = block << log2Quantum;
				delta = index - blockOnes + 1;
			}

			for( int bitCount; ( bitCount = Long.bitCount( window ) ) < delta; delta -= bitCount )
				window = list.getLong( ++curr );
			
			//System.err.println( "index: " + index + " delta: " + delta + " curr: " + curr + " window: " + Long.toBinaryString( window ) );

			/* This appears to be faster than != 0 (WTF?!). Note that for delta == 1 the following code is a NOP. */
			if ( --delta > 0 ) {
				// Phase 1: sums by byte
				final long word = window;
				assert delta < Long.bitCount( word ) : delta + " >= " + Long.bitCount( word );
				long byteSums = word - ( ( word & 0xa * ONES_STEP_4 ) >>> 1 );
				byteSums = ( byteSums & 3 * ONES_STEP_4 ) + ( ( byteSums >>> 2 ) & 3 * ONES_STEP_4 );
				byteSums = ( byteSums + ( byteSums >>> 4 ) ) & 0x0f * ONES_STEP_8;
				byteSums *= ONES_STEP_8;

				// Phase 2: compare each byte sum with delta to obtain the relevant byte
				final long rankStep8 = delta * ONES_STEP_8;
				final long byteOffset = ( ( ( ( ( rankStep8 | MSBS_STEP_8 ) - byteSums ) & MSBS_STEP_8 ) >>> 7 ) * ONES_STEP_8 >>> 53 ) & ~0x7;

				final int byteRank = (int)( delta - ( ( ( byteSums << 8 ) >>> byteOffset ) & 0xFF ) );

				final int select = (int)( byteOffset + Fast.selectInByte[ (int)( word >>> byteOffset & 0xFF ) | byteRank << 8 ] );

				// We cancel up to, but not including, the target one.
				window &= -1L << select;
			}

			assert window != 0;
			currentIndex = index + 1;
			prevPrefixSum = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - ( index - 1 ) - upperBitsStart << l | lowerBits.extract( lowerBitsStart + l * ( index - 1 ) );
			window &= window - 1;
			while( window == 0 ) window = list.getLong( ++curr );
			prefixSum = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - index - upperBitsStart << l | lowerBits.extract();
			window &= window - 1;
			return prefixSum - prevPrefixSum + 1;
		}

		public String toString() {
			return this.getClass().getSimpleName() + '@' + Integer.toHexString( System.identityHashCode( this ) );
		}
	}

	protected final static class PositionReader {
		private static final boolean DEBUG = false;
	
		/** The longword bit reader for pointers. */
		private final LongWordBitReader skipPointers;
		/** The longword bit reader for the lower bits. */
		private final LongWordBitReader lowerBits;

		/** The underlying list. */
		private final LongBigList list;
		/** The 64-bit window. */
		private long window;
		/** The current word position in the list of upper bits. */
		private long curr;

		/** The starting position of the pointers. */
		private final long skipPointersStart;
		/** The starting position of the power bits. */
		private final long lowerBitsStart;
		/** The starting position of the upper bits. */
		private final long upperBitsStart;

		/** The number of lower bits. */
		private final int l;
		/** The size of a pointer. */
		private final int pointerSize;
		/** The number of pointers. */
		private final long numberOfPointers;
		/** The logarithm of the quantum, cached from the index. */
		private final int log2Quantum;
		/** The quantum. */
		private final int quantum;

		/** The current prefix sum (the sum of the first {@link #currentIndex} elements). */
		private long prefixSum;
		/** The index of the current prefix sum. */
		private long currentIndex;
		/** The base of the sequence of positions currently returned. */
		private long base;

		public PositionReader( final LongBigList list, final int l, final long skipPointersStart, final long numberOfPointers, final int pointerSize, final long occurrency, final int log2Quantum ) {
			this.list = list; 
			this.l = l;
			this.skipPointersStart = skipPointersStart;
			this.numberOfPointers = numberOfPointers;
			this.pointerSize = pointerSize;

			skipPointers = new LongWordBitReader( list, pointerSize );
			lowerBits = new LongWordBitReader( list, l );;
			lowerBitsStart = skipPointersStart + pointerSize * numberOfPointers;
			lowerBits.position( lowerBitsStart );
			upperBitsStart = lowerBitsStart + l * occurrency;
			currentIndex = prefixSum = 0;

			position( upperBitsStart );

			this.log2Quantum = log2Quantum;
			quantum = 1 << log2Quantum;
		}

		private void position( final long position ) {
			window = list.getLong( curr = position / Long.SIZE ) & -1L << (int)( position );
		}

		public int getFirstPosition( long index ) {
			if ( DEBUG ) System.err.println( this + ".getFirstPosition(" + index + ")" );

			long delta = index - currentIndex;

			if ( delta == 0 ) {	// shortcut
				/*while( delta-- != 0 ) { // Alternative code. Intended for small deltas.
					while( window == 0 ) window = list.getLong( ( curr += Long.SIZE ) / Long.SIZE );
					prefixSum = curr + Long.numberOfTrailingZeros( window ) - currentIndex++ - upperBitsStart << l | lowerBits.extract();
					window &= window - 1;
				}*/
				base = prefixSum;
				while( window == 0 ) window = list.getLong( ++curr );
				prefixSum = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - currentIndex++ - upperBitsStart << l | lowerBits.extract();
				window &= window - 1;
				return (int)( prefixSum - base );
			}

			if ( delta >= quantum ) {
				final long block = index >>> log2Quantum;
				assert block > 0;
				assert block <= numberOfPointers;
				final long skip = skipPointers.extract( skipPointersStart + ( block - 1 ) * pointerSize );
				position( upperBitsStart + skip - 1 );
				final long blockOnes = block << log2Quantum;
				delta = index - blockOnes + 1;
			}

			for( int bitCount; ( bitCount = Long.bitCount( window ) ) < delta; delta -= bitCount )
				window = list.getLong( ++curr );

			/* This appears to be faster than != 0 (WTF?!). Note that for delta == 1 the following code is a NOP. */
			if ( --delta > 0 ) {
				// Phase 1: sums by byte
				final long word = window;
				assert delta < Long.bitCount( word ) : delta + " >= " + Long.bitCount( word );
				long byteSums = word - ( ( word & 0xa * ONES_STEP_4 ) >>> 1 );
				byteSums = ( byteSums & 3 * ONES_STEP_4 ) + ( ( byteSums >>> 2 ) & 3 * ONES_STEP_4 );
				byteSums = ( byteSums + ( byteSums >>> 4 ) ) & 0x0f * ONES_STEP_8;
				byteSums *= ONES_STEP_8;

				// Phase 2: compare each byte sum with k + 1 to obtain the relevant byte
				final long residualPlusOneStep8 = ( delta + 1 ) * ONES_STEP_8;
				final long byteOffset = Long.numberOfTrailingZeros( ( ( ( byteSums | MSBS_STEP_8 ) - residualPlusOneStep8 ) & MSBS_STEP_8 ) >>> 7 );

				final int byteRank = (int)( delta - ( ( ( byteSums << 8 ) >>> byteOffset ) & 0xFF ) );

				final int select = (int)( byteOffset + Fast.selectInByte[ (int)( word >>> byteOffset & 0xFF ) | byteRank << 8 ] );

				// We cancel up to, but not including, the target one.
				window &= -1L << select;
			}

			assert window != 0;
			currentIndex = index + 1;
			base = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - index + 1 - upperBitsStart << l | lowerBits.extract( lowerBitsStart + l * ( index - 1 ) );
			window &= window - 1;
			while( window == 0 ) window = list.getLong( ++curr );
			prefixSum = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - index - upperBitsStart << l | lowerBits.extract();
			window &= window - 1;
			return (int)( prefixSum - base );
		}
		
		public int getNextPosition() {
			while( window == 0 ) window = list.getLong( ++curr );
			prefixSum = curr * Long.SIZE + Long.numberOfTrailingZeros( window ) - currentIndex++ - upperBitsStart << l | lowerBits.extract();
			window &= window - 1;
			return (int)( prefixSum - --base );
		}
		
		public String toString() {
			return this.getClass().getSimpleName() + '@' + Integer.toHexString( System.identityHashCode( this ) );
		}
	}

	protected abstract static class AbstractQuasiSuccinctIndexIterator extends AbstractIndexIterator {
		//private static final boolean DEBUG = false;
		/** The index reader associated to this index iterator. */
		protected final QuasiSuccinctIndexReader indexReader;
		/** The index of {@link #indexReader}, cached. */
		protected final QuasiSuccinctIndex index;
		/** A reference to the data for pointers. */
		protected final LongBigList pointersList;
		/** A reference to the data for counts, or <code>null</code> of {@link #hasCounts} is false. */
		protected final LongBigList countsList;
		/** A reference to the data for positions, or <code>null</code> of {@link #hasPositions} is false. */
		protected final LongBigList positionsList;
		/** The count reader for the current term. */
		protected CountReader counts;
		/** The position reader for the current term. */
		protected PositionReader positions;
		/** An index interval iterator. */
		private final IntervalIterator intervalIterator;
		/** A singleton set containing {@link #intervalIterator}. */
		private final Reference2ReferenceMap<Index,IntervalIterator> singletonIntervalIterator;
		/** The number of documents (cached from {@link #index}). */
		protected final long numberOfDocuments;
		/** The key index (cached from {@link #index}). */
		private final Index keyIndex;
		/** Cached from {@link #index}. */
		protected final boolean hasCounts;
		/** Cached from {@link #index}. */
		protected final boolean hasPositions;
		/** The current document. */
		protected long currentDocument;
		/** The frequency of the current term. */
		protected long frequency;
		/** The occurrency of the current term. */
		protected long occurrency;
		/** The index of the current term. */
		protected long currentTerm;
		/** The count of the current posting, or -1 if it is not known. */
		protected long count;
		/** The number of returned positions for the current document. */
		protected long nextPosition;

		protected AbstractQuasiSuccinctIndexIterator( final QuasiSuccinctIndexReader indexReader ) {
			this.indexReader = indexReader;
			index = indexReader.index;
			keyIndex = index.keyIndex;
			hasPositions = index.hasPositions;
			hasCounts = index.hasCounts;
			numberOfDocuments = index.numberOfDocuments;
			pointersList = indexReader.pointersList;
			countsList = indexReader.countsList;
			positionsList = indexReader.positionsList;
			intervalIterator = hasPositions ? new IndexIntervalIterator( this ) : IntervalIterators.FALSE; 
			singletonIntervalIterator = Reference2ReferenceMaps.singleton( keyIndex, hasPositions ? intervalIterator : IntervalIterators.FALSE );
			currentTerm = frequency = -1;
		}

		@Override
		public Index index() {
			return index;
		}

		@Override
		public long termNumber() {
			return currentTerm;
		}

		@Override
		public long frequency() throws IOException {
			return frequency;
		}

		@Override
		public Payload payload() throws IOException {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public int nextPosition() throws IOException {
			assert currentDocument != -1;
			assert currentDocument != END_OF_LIST;

			if ( nextPosition == 0 ) {
				nextPosition = 1;
				count();
				return positions.getFirstPosition( counts.prevPrefixSum + counts.currentIndex - 1 );  
			}
			
			if ( nextPosition == count ) return END_OF_POSITIONS;
			nextPosition++;
			return positions.getNextPosition();
		}

		@Override
		public long document() {
			return currentDocument;
		}

		private void ensureCurrentDocument() {
			if ( ( currentDocument | 0x80000000 ) == -1 ) throw new IllegalStateException( currentDocument == -1 ? "nextDocument() has never been called for (term=" + currentTerm + ")" : "This reader is positioned beyond the end of list of (term=" + currentTerm + ")" );
		}

		public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
			return singletonIntervalIterator;
		}
		
		public IntervalIterator intervalIterator() throws IOException {
			return intervalIterator;
		}
		
		public IntervalIterator intervalIterator( final Index index ) throws IOException {
			ensureCurrentDocument();
			return index == keyIndex ? intervalIterator() : IntervalIterators.FALSE;
		}
		
		public ReferenceSet<Index> indices() {
			return index.singletonSet;
		}

		public String toString() {
			return index + " [" + currentTerm + "]" + ( weight != 1 ? "{" + weight + "}" : "" );
		}

		@Override
		public void dispose() throws IOException {
			indexReader.close();
		}
	}
	
	protected final static class EliasFanoIndexIterator extends AbstractQuasiSuccinctIndexIterator {
		private static final boolean DEBUG = false;
		/** The pointer reader for the current term. */
		protected EliasFanoPointerReader pointers;

		public EliasFanoIndexIterator( QuasiSuccinctIndexReader indexReader ) {
			super( indexReader );
		}

		protected boolean position( final long termNumber, final long frequency, final long occurrency ) {
			this.frequency = frequency;
			this.occurrency = occurrency;

			final int log2Quantum = index.log2Quantum;
			int l = QuasiSuccinctIndexWriter.lowerBits( frequency + 1, numberOfDocuments, false );
			int pointerSize = QuasiSuccinctIndexWriter.pointerSize( frequency + 1, numberOfDocuments, false, true );
			long numberOfPointers = QuasiSuccinctIndexWriter.numberOfPointers( frequency + 1, numberOfDocuments, log2Quantum, false, true );

			final LongWordBitReader skipPointers = new LongWordBitReader( pointersList, pointerSize );
			
			final LongWordBitReader lowerBits = new LongWordBitReader( pointersList, l );

			final long skipPointersStart = indexReader.pointersLongWordBitReader.position();

			final long lowerBitsStart = skipPointersStart + pointerSize * numberOfPointers;
			lowerBits.position( lowerBitsStart ); 						
			pointers = new EliasFanoPointerReader( pointersList, lowerBits, lowerBitsStart, l, skipPointers, skipPointersStart, numberOfPointers, pointerSize, frequency, log2Quantum );

			if ( hasCounts ) {
				long position = termNumber == 0 ? 0 : index.countsOffsets.getLong( termNumber );
				counts = new CountReader( countsList, position, frequency, occurrency, log2Quantum );
				count = 0;
				
				if ( hasPositions ) {
					position = termNumber == 0 ? 0 : index.positionsOffsets.getLong( termNumber  );

					indexReader.positionsLongWordBitReader.position( position );
					l = (int)indexReader.positionsLongWordBitReader.readGamma();
					numberOfPointers = QuasiSuccinctIndexWriter.numberOfPointers( occurrency, -1, log2Quantum, true, false );
					pointerSize = numberOfPointers == 0 ? -1 : (int)indexReader.positionsLongWordBitReader.readNonZeroGamma();
					positions = new PositionReader( positionsList, l, indexReader.positionsLongWordBitReader.position(), numberOfPointers, pointerSize, occurrency, log2Quantum );  
				}
			}

			currentTerm = termNumber;
			currentDocument = -1;
			return true;
		}
			
		@Override
		public long nextDocument() throws IOException {
			assert currentDocument != END_OF_LIST;
			if ( DEBUG ) System.err.println( this + ".nextDocument() [currentDocument = " + currentDocument + ", currentIndex = " + pointers.currentIndex + ", frequency = " + frequency + "]" );
			count = nextPosition = 0;
			final long nextDocument = pointers.getNextPrefixSum();
			if ( DEBUG ) System.err.println( this + ".nextDocument() => " + currentDocument );
			return currentDocument = nextDocument == numberOfDocuments ? END_OF_LIST : nextDocument;
		}

		@Override
		public long skipTo( long n ) throws IOException {
			if ( n == END_OF_LIST ) return currentDocument = END_OF_LIST;
			assert n < numberOfDocuments : n + " >= " + numberOfDocuments;
			if ( currentDocument >= n ) return currentDocument;
			count = nextPosition = 0;
			final long nextDocument = pointers.skipTo( n );
			return currentDocument = nextDocument == numberOfDocuments ? END_OF_LIST : nextDocument;
		}

		@Override
		public boolean mayHaveNext() {
			// System.err.println( pointers.currentIndex+" < "+frequency);
			return currentDocument != END_OF_LIST && pointers.currentIndex < frequency;
		}

		@Override
		public int count() throws IOException {
			assert currentDocument != -1;
			assert currentDocument != END_OF_LIST;
			return (int)( count == 0 ? count = counts.getLong( pointers.currentIndex - 1 ) : count );
		}
	}
	
	protected final static class RankedIndexIterator extends AbstractQuasiSuccinctIndexIterator {
		private static final boolean DEBUG = false;
		/** The pointer reader for the current term. */
		protected RankedPointerReader pointers;

		public RankedIndexIterator( QuasiSuccinctIndexReader indexReader ) {
			super( indexReader );
		}

		protected boolean position( final long termNumber, final long frequency, final long occurrency ) {
			this.frequency = frequency;
			this.occurrency = occurrency;
			
			final int log2Quantum = index.log2Quantum;
			int pointerSize = Fast.length( frequency + 1 );
			long numberOfPointers = numberOfDocuments >>> log2Quantum;

			final LongWordBitReader skipPointers = new LongWordBitReader( pointersList, pointerSize );

			final long skipPointersStart = indexReader.pointersLongWordBitReader.position();
			final long upperBitsStart = skipPointersStart + pointerSize * numberOfPointers;
			indexReader.pointersLongWordBitReader.position( upperBitsStart );
						
			pointers = new RankedPointerReader( pointersList, upperBitsStart, skipPointers, skipPointersStart, numberOfPointers, pointerSize, frequency, log2Quantum );

			if ( hasCounts ) {
				long position = termNumber == 0 ? 0 : index.countsOffsets.getLong( termNumber );
				counts = new CountReader( countsList, position, frequency, occurrency, log2Quantum );
				count = 0;
				
				if ( hasPositions ) {
					position = termNumber == 0 ? 0 : index.positionsOffsets.getLong( termNumber  );

					indexReader.positionsLongWordBitReader.position( position );
					final int l = (int)indexReader.positionsLongWordBitReader.readGamma();
					numberOfPointers = QuasiSuccinctIndexWriter.numberOfPointers( occurrency, -1, log2Quantum, true, false );
					pointerSize = numberOfPointers == 0 ? -1 : (int)indexReader.positionsLongWordBitReader.readNonZeroGamma();
					positions = new PositionReader( positionsList, l, indexReader.positionsLongWordBitReader.position(), numberOfPointers, pointerSize, occurrency, log2Quantum );  
				}
			}

			currentTerm = termNumber;
			currentDocument = -1;
			return true;
		}
			
		@Override
		public long nextDocument() throws IOException {
			assert currentDocument != END_OF_LIST;
			if ( DEBUG ) System.err.println( this + ".nextDocument() [currentDocument = " + currentDocument + ", currentIndex = " + pointers.currentIndex + ", frequency = " + frequency + "]" );
			count = nextPosition = 0;
			final long nextDocument = pointers.getNextPrefixSum();
			if ( DEBUG ) System.err.println( this + ".nextDocument() => " + currentDocument );
			return currentDocument = nextDocument == numberOfDocuments ? END_OF_LIST : nextDocument;
		}

		@Override
		public long skipTo( long n ) throws IOException {
			if ( n == END_OF_LIST ) return currentDocument = END_OF_LIST;
			assert n < numberOfDocuments : n + " >= " + numberOfDocuments;
			if ( currentDocument >= n ) return currentDocument;
			count = nextPosition = 0;
			final long nextDocument = pointers.skipTo( n );
			return currentDocument = nextDocument == numberOfDocuments ? END_OF_LIST : nextDocument;
		}

		@Override
		public boolean mayHaveNext() {
			return currentDocument != END_OF_LIST;
		}

		@Override
		public int count() throws IOException {
			assert currentDocument != -1;
			assert currentDocument != END_OF_LIST;
			return (int)( count == 0 ? count = counts.getLong( pointers.currentIndex - 1 ) : count );
		}
	}

	private IndexIterator documents( final CharSequence term, final long termNumber ) {
		currentTerm = termNumber;

		//System.err.println( this + ".position(" + term + ")" );
		long position;

		if ( termNumber == 0 ) position = 0;
		else  {
			if ( index.pointersOffsets == null ) throw new IllegalStateException( "You cannot position an index without offsets" );
			position = index.pointersOffsets.getLong( termNumber );
		}

		pointersLongWordBitReader.position( position );
		final long occurrency = pointersLongWordBitReader.readNonZeroGamma();
		final long frequency = occurrency == 1 ? 1 : occurrency - pointersLongWordBitReader.readGamma();
		int l = QuasiSuccinctIndexWriter.lowerBits( frequency + 1, index.numberOfDocuments, false );

		if ( frequency + 1L + ( index.numberOfDocuments >>> l ) + ( frequency + 1L ) * l > index.numberOfDocuments ) {
			rankedIndexIterator.position( termNumber, frequency, occurrency );
			rankedIndexIterator.term( term ); 
			return rankedIndexIterator;
		}
		
		eliasFanoIndexIterator.position( termNumber, frequency, occurrency );
		eliasFanoIndexIterator.term( term );
		return eliasFanoIndexIterator;
	}

	@Override
	public IndexIterator documents( final long termNumber ) throws IOException {
		return documents( null, termNumber );
	}

	@Override
	public IndexIterator documents( final CharSequence term ) throws IOException {
		if ( closed ) throw new IllegalStateException( "This " + getClass().getSimpleName() + " has been closed" );
		if ( index.termMap != null ) {
			final long termIndex = index.termMap.getLong( term );
			if ( termIndex == -1 ) return index.getEmptyIndexIterator( term, termIndex );
			return documents( term, termIndex );
		}
		throw new UnsupportedOperationException( "Index " + index + " has no term map" );
	}

	@Override
	public IndexIterator nextIterator() throws IOException {
		if ( currentTerm == index.numberOfTerms - 1 ) return null;
		return documents( ++currentTerm );
	}
	
}
