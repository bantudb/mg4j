package it.unimi.di.big.mg4j.util;


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
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */
import it.unimi.dsi.fastutil.longs.AbstractLongBigList;
import it.unimi.dsi.fastutil.longs.Long2LongLinkedOpenHashMap;
import it.unimi.dsi.io.InputBitStream;

import java.io.IOException;

/** Provides semi-external random access to offsets of an {@link it.unimi.di.big.mg4j.index.Index index}. 
 * 
 * <p>This class is a semi-external {@link it.unimi.dsi.fastutil.longs.LongList} that
 * MG4J uses as default for accessing term offsets.
 *  
 * <p>When the number of terms in the index grows, storing each offset as a long in an
 * array can consume hundred of megabytes of memory, and most of this memory is wasted,
 * as it is occupied by offsets of <i>hapax legomena</i> (terms occurring just once in the
 * collection). Instead, this class accesses offsets in their
 * compressed forms, and provides entry points for random access to each offset. At construction
 * time, entry points are computed with a certain <em>step</em>, which is the number of offsets
 * accessible from each entry point, or, equivalently, the maximum number of offsets that will
 * be necessary to read to access a given offset.
 * 
 * <p>This class uses a small ({@link #CACHE_MAX_SIZE} entries) map to keep track of the most recently used
 * indices, so to answer queries to those indices more quickly.
 *
 * <p><strong>Warning:</strong> This class is not thread safe, and needs to be synchronised to be used in a
 * multithreaded environment. 
 *
 * @author Fabien Campagne
 * @author Sebastiano Vigna
 */
public class SemiExternalOffsetBigList extends AbstractLongBigList {
	/** The maximum number of entry in the cache map. */
	public static final int CACHE_MAX_SIZE = 1024;
	/** Position in the offset stream for each random access entry point (one each {@link #offsetStep} elements). */
	private final long[] position;
	/** An array parallel to {@link #position} recording the value of the offset for each random access entry point. */
	private final long[] startValue;
	/** Stream over the compressed offset information. */
	private final InputBitStream ibs;
	/** Maximum number of times {@link InputBitStream#readLongGamma()} will be called to access an offset. */
	private final int offsetStep;
	/** The number of offsets. */
	private final long numOffsets;
	/** A cache for the most recent queries */
	private Long2LongLinkedOpenHashMap cache;
	
	/** Creates a new semi-external list.
	 * 
	 * @param offsetRawData a bit stream containing the offsets in compressed form (&gamma;-encoded deltas).
	 * @param offsetStep the step used to build random-access entry points.
	 * @param numOffsets the overall number of offsets (i.e., the number of terms).
	 */

	public SemiExternalOffsetBigList( final InputBitStream offsetRawData, final int offsetStep, final long numOffsets ) throws IOException {
		int slots = (int)Math.max( ( numOffsets + offsetStep - 1 ) / offsetStep, numOffsets / ( 1L << 31 ) );
		this.position = new long[ slots ];
		this.startValue = new long[ slots ];
		this.offsetStep = offsetStep;
		this.numOffsets = numOffsets;
		this.ibs = offsetRawData;
		( this.cache = new Long2LongLinkedOpenHashMap() ).defaultReturnValue( -1 );
		prepareRandomAccess( numOffsets );
	}

	/** Scans {@link #ibs} and fills the necessary data in {@link #position} and {@link #startValue}.
	 * 
	 * @param numOffsets the number of offsets.
	 */
	
	private void prepareRandomAccess( final long numOffsets ) throws IOException {
		long offset = 0;
		ibs.position( 0 );
		
		int k = 0;
		int slotIndex = 0;
		
		for ( long i = numOffsets; i-- != 0; ) {
			offset += ibs.readLongGamma();

			if ( k-- == 0 ) {
				k = offsetStep - 1;

				startValue[ slotIndex ] = offset;
				position[ slotIndex ] = ibs.readBits();
				slotIndex++;
			}
		}
	}

	public final long getLong( final long index ) {
		if ( index < 0 || index >= numOffsets ) throw new IndexOutOfBoundsException( Long.toString( index ) );
		final long cached = cache.getAndMoveToLast( index );
		if ( cached != -1 ) return cached;

		final int slotNumber = (int)( index / offsetStep );
		final int k = (int)( index % offsetStep );
		long value = startValue[ slotNumber ];
		if ( k != 0 ) {
			try {
				ibs.position( position[ slotNumber ] );
				for ( int i = k; i-- != 0; ) {
					final long diff = ibs.readLongGamma();
					// System.out.println("diff: " + diff);
					value += diff;
				}
				
			}
			catch( IOException e ) {
				throw new RuntimeException( e );
			}
		}

		// Update cache
		if ( cache.size() >= CACHE_MAX_SIZE ) cache.removeFirstLong();
		cache.put( index, value );
		return value;
	}
	
	public long size64() {
		return numOffsets;
	}
}