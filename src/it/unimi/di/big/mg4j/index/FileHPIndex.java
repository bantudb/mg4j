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
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.util.Properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/** A file-based high-performance index.
 *
 * <P>An instance of this class stores additional index data for indices
 * based on files: for instance, basename, compression flags, etc.
 * 
 * @author Paolo Boldi
 * @author Sebastiano Vigna
 * @since 0.9
 */

public class FileHPIndex extends BitStreamHPIndex {
	private static final long serialVersionUID = 0;
	
	/** The basename of this index. All file names will be stemmed from the basename. It may
	 * be <code>null</code>. */
	public final String basename;
	/** The file containing the index. It may be <code>null</code>. */
	public final File indexFile;
	/** The file containing the positions. It may be <code>null</code>. // TODO: Why ? */
	public final File positionsFile;

	public FileHPIndex( final String basename, final long numberOfDocuments, final long numberOfTerms, final long numberOfPostings,
			final long numberOfOccurrences, final int maxCount, final Payload payload, final Coding frequencyCoding, final Coding pointerCoding, final Coding countCoding, final Coding positionCoding, final int quantum, final int height, final int bufferSize, 
			final TermProcessor termProcessor, final String field, final Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final IntBigList sizes, final LongBigList offsets ) {
		super( numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, frequencyCoding, pointerCoding, countCoding, positionCoding, quantum, height, bufferSize, termProcessor, field, properties, termMap, prefixMap, sizes, offsets );
		this.basename = basename;
		this.indexFile = new File( basename + DiskBasedIndex.INDEX_EXTENSION );
		this.positionsFile = new File( basename + DiskBasedIndex.POSITIONS_EXTENSION );
	}

	public String toString() {
		return this.getClass().getSimpleName() + "(" + basename + ")";
	}

	@Override
	public FileInputStream getInputStream() throws FileNotFoundException {
		return new FileInputStream( indexFile );
	}

	@Override
	public InputBitStream getInputBitStream( final int bufferSize ) throws FileNotFoundException {
		return new InputBitStream( indexFile, bufferSize );
	}

	@Override
	public InputStream getPositionsInputStream() throws IOException {
		return new FileInputStream( positionsFile );
	}

	@Override
	public InputBitStream getPositionsInputBitStream( int bufferSize ) throws IOException {
		return new InputBitStream( positionsFile, bufferSize );
	}
}
