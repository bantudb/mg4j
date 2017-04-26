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


import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.dsi.big.util.PrefixMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import it.unimi.dsi.util.ByteBufferLongBigList;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.nio.ByteOrder;

/** A quasi-succinct index.
 *
 * <p>A quasi-succinct index does not use gap-compression to represent its various components, but
 * rather the {@linkplain EliasFanoMonotoneLongBigList Elias&ndash;Fano representation} of monotone sequences. The index was described in detail
 * by Sebastiano Vigna in the paper &ldquo;Quasi-Succinct Indices&rdquo;, <i>Proceedings of the 6th ACM International Conference on Web Search and Data Mining, WSDM'13</i>, pages 83&minus;92. ACM, 2013. 
 * It is smaller than a &gamma;/&delta;-code
 * gap-compressed index, and significantly faster when computing conjunctive, phrasal or proximity operators,
 * as it provides constant-time access on average to every piece of information in the index.
 * 
 * <p>In a quasi-succinct index pointers, counters and positions are represented in three different
 * files, each of which has its own offset file. The file do not contain a {@linkplain InputBitStream byte-oriented
 * bitstream representation}, but rather 
 * arrays of 64-bit longwords with specified {@linkplain ByteOrder byte order} (by default the native one
 * for performance reasons). The longwords are either loaded in memory as a {@link LongBigArrayBigList}
 * or mapped using a {@link ByteBufferLongBigList}. The bit <var>k</var> of a file is the
 * bit <var>k</var> mod 64 of the longword of index &lfloor;<var>k</var> / 64&rfloor;.
 * 

 * <p>Note that the methods
 * providing {@linkplain #getPointersList() pointers}, {@linkplain #getCountsList() counts} and
 * {@linkplain #getPositionsList() positions} to index readers 
 * use reflection to detect whether the {@link LongBigList} storing
 * a component is a {@link ByteBufferLongBigList}, and 
 * in that case they return a {@linkplain ByteBufferLongBigList#copy() copy}.
 * 
 * @see QuasiSuccinctIndexReader
 * @see QuasiSuccinctIndexWriter
 * @author Sebastiano Vigna
 */
public class QuasiSuccinctIndex extends Index {
	private static final long serialVersionUID = 1L;
	/** The default quantum. */
	public final static int DEFAULT_QUANTUM = 256;

	/** Symbolic names for additional properties of a {@link QuasiSuccinctIndex}. */
	public static enum PropertyKeys {
		/** The byte order of the index. */
		BYTEORDER,			
	}

	/** The big list of longs representing the bitstream of pointers. */
	private final LongBigList pointers;
	/** The big list of longs representing the bitstream of counts. */
	private final LongBigList counts;
	/** The big list of longs representing the bitstream of positions. */
	private final LongBigList positions;
	/** The list of offsets into {@linkplain #getPointersList() pointers}. */
	protected final LongBigList pointersOffsets;
	/** The list of offsets into {@linkplain #getCountsList() counts}. */
	protected final LongBigList countsOffsets;
	/** The list of offsets into {@linkplain #getPositionsList() positions}. */
	protected final LongBigList positionsOffsets;
	/** The logarithm of the skipping quantum. */
	public final int log2Quantum;

	protected QuasiSuccinctIndex( final LongBigList index, final LongBigList counts, final LongBigList positions, 
			final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings, final long numberOfOccurrences, final int maxCount, final Payload payload, final int log2Quantum, final boolean hasCounts, final boolean hasPositions,
			final TermProcessor termProcessor, final String field, final Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, 
			final IntBigList sizes, final LongBigList indexOffsets, final LongBigList countsOffsets, final LongBigList positionsOffsets ) {
		super( numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, hasCounts, hasPositions, termProcessor, field, termMap, prefixMap, sizes, properties );
		this.pointers = index;
		this.counts = counts;
		this.positions = positions;
		this.pointersOffsets = indexOffsets;
		this.countsOffsets = countsOffsets;
		this.positionsOffsets = positionsOffsets;
		this.log2Quantum = log2Quantum;
	}

	@Override
	public IndexReader getReader( int bufferSize ) throws IOException {
		return new QuasiSuccinctIndexReader( this );
	}

	public String toString() {
		return this.getClass().getSimpleName() + "[" + field + "]";
	}
	
	protected LongBigList getPointersList() {
		return pointers instanceof ByteBufferLongBigList ? ((ByteBufferLongBigList)pointers).copy() : pointers;
	}

	protected LongBigList getCountsList() {
		return counts instanceof ByteBufferLongBigList ? ((ByteBufferLongBigList)counts).copy() : counts;
	}

	protected LongBigList getPositionsList() {
		return positions instanceof ByteBufferLongBigList ? ((ByteBufferLongBigList)positions).copy() : positions;		
	}
}
