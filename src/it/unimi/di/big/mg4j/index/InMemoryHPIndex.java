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
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.IOException;
import java.io.InputStream;

/** A {@link BitStreamHPIndex} index loaded in memory.
 * 
 * <p>In-memory indices are created by loading into main memory an index.
 *  
 * @author Sebastiano Vigna
 * @since 1.2
 */

public class InMemoryHPIndex extends BitStreamHPIndex {
	private static final long serialVersionUID = 0L;
	
	/** The byte array containing the index. */
	protected final byte[] index;
	/** The byte array containing the positions. */
	protected final byte[] positions;

	public InMemoryHPIndex( final byte[] index, final byte[] positions, final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings,
			final long numberOfOccurrences, final int maxCount, final Payload payload, final Coding frequencyCoding, final Coding pointerCoding, final Coding countCoding, final Coding positionCoding, final int quantum, final int height, 
			final TermProcessor termProcessor, final String field, final Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final IntBigList sizes, final LongBigList offsets ) {
		super( numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, frequencyCoding, pointerCoding, countCoding, positionCoding, quantum, height, -1, termProcessor, field, properties, termMap, prefixMap, sizes, offsets );
		this.index = index;
		this.positions = positions;
	}

	public String toString() {
		return this.getClass().getSimpleName() + "(" + field + ")";
	}

	@Override
	public InputBitStream getInputBitStream( int bufferSizeUnused ) {
		return new InputBitStream( index );
	}
	
	@Override
	public InputStream getInputStream() {
		return new FastByteArrayInputStream( index );
	}

	@Override
	public InputBitStream getPositionsInputBitStream( int bufferSizeUnused ) throws IOException {
		return new InputBitStream( positions );
	}

	@Override
	public InputStream getPositionsInputStream() throws IOException {
		return new FastByteArrayInputStream( positions );
	}
}
