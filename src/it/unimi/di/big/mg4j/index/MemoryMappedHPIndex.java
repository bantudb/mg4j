package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Sebastiano Vigna 
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
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.dsi.big.util.PrefixMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.ByteBufferInputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;

/** A memory-mapped {@link BitStreamHPIndex}. See the discussion in {@link MemoryMappedIndex} (and consider
 * that since a high-performance index is actually split in two files, memory mapping is actually easier).
 *  
 * @author Sebastiano Vigna
 * @since 1.2
 */

public class MemoryMappedHPIndex extends BitStreamHPIndex {
	private static final long serialVersionUID = 0L;
	
	/** The byte buffer containing the index. */
	protected final ByteBufferInputStream index;
	/** The byte buffer containing the positions. */
	protected final ByteBufferInputStream positions;

	public MemoryMappedHPIndex( final ByteBufferInputStream index, final ByteBufferInputStream positions, final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings,
			final long numberOfOccurrences, final int maxCount, final Payload payload, final Coding frequencyCoding, final Coding pointerCoding, final Coding countCoding, final Coding positionCoding, final int quantum, final int height, 
			final TermProcessor termProcessor, final String field, final Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final IntBigList sizes, final LongBigList offsets ) {
		super( numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, frequencyCoding, pointerCoding, countCoding, positionCoding, quantum, height, -1, termProcessor, field, properties, termMap, prefixMap, sizes, offsets );
		this.index = index;
		this.positions = positions;
	}

	@Override
	public InputBitStream getInputBitStream( int bufferSizeUnused ) {
		return new InputBitStream( getInputStream() );
	}
	
	@Override
	public ByteBufferInputStream getInputStream() {
		return index.copy();
	}

	@Override
	public InputBitStream getPositionsInputBitStream( int bufferSizeUnused ) throws IOException {
		return new InputBitStream( getPositionsInputStream() );
	}

	@Override
	public InputStream getPositionsInputStream() throws IOException {
		return positions.copy();
	}
}
