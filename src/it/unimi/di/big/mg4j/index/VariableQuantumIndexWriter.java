package it.unimi.di.big.mg4j.index;

import java.io.IOException;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2009-2016 Sebastiano Vigna 
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

/** An index writer supporting variable quanta.
 * 
 * <p>This interface provides an additional {@link #newInvertedList(long, double, long, long)} method
 * that accepts additional information. That information is used to compute the correct quantum
 * size for a specific list. This approach makes it possible to specify a target fraction of
 * the index that will be used to store skipping information.
 * 
 */

public interface VariableQuantumIndexWriter {
	/** Starts a new inverted list. The previous inverted list, if any, is actually written
	 * to the underlying bit stream.
	 *  
	 * <p>This method provides additional information that will be used to compute the
	 * correct quantum for the skip structure of the inverted list.
	 * 
	 * @param predictedFrequency the predicted frequency of the inverted list; this might
	 * just be an approximation.
	 * @param skipFraction the fraction of the inverted list that will be dedicated to
	 * skipping structures.
	 * @param predictedSize the predicted size of the part of the inverted list that stores 
	 * terms and counts.
	 * @param predictedPositionsSize the predicted size of the part of the inverted list that
	 * stores positions.
	 * @return the position (in bits) of the underlying bit stream where the new inverted
	 * list starts.
	 * @throws IllegalStateException if too few records were written for the previous inverted
	 * list.
	 * @see IndexWriter#newInvertedList()
	 */
	public long newInvertedList( long predictedFrequency, double skipFraction, long predictedSize, long predictedPositionsSize ) throws IOException;

}
