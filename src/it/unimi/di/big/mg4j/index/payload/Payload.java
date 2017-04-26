package it.unimi.di.big.mg4j.index.payload;


/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2007-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.collections.Predicate;

/** An index payload.
 * 
 * <p>The main responsibility of this class is that of providing efficient
 * ways to read and write a payload from and to bit streams. An instance of this
 * class has at any given time a <em>current value</em>, which is set when
 * {@linkplain #read(InputBitStream) reading}. 
 * and output when {@linkplain #write(OutputBitStream) writing}.
 *
 * <p>The current value can be modified using {@link #set(Object)}, and
 * each implementation must document thoroughly which objects are
 * accepted by this method.
 *
 * <p>It is expected that in most implementations reading and writing is
 * much more efficient than reading, {@linkplain #get() getting a value},
 * {@linkplain #set(Object) setting that value} in another instance, and
 * finally {@linkplain #write(OutputBitStream) writing}.
 * 
 * <p>Implementation of a payload might have parameters. If you need to know
 * whether two instances are <em>compatible</em>, in the sense that each
 * instance can read correctly data written by the other one, you can invoke
 * the {@link #compatibleWith(Payload)} method.
 * 
 * <p>Optionally, implementations can feature a <code>parse(String)</code> method
 * that returns an object of the correct type for {@link #set(Object)}. This method
 * can be used (for instance, by reflection) to try to build a payload from a string
 * specification (this is what happens in {@link DocumentIteratorBuilderVisitor}).
 */


public interface Payload extends Serializable, Comparable<Payload> {

	/** Serialises the current value of this payload to the given output bit stream. 
	 * 
	 * @param obs the bit stream receiving the bits.
	 * @return the number of bits written.
	 * @throws IllegalStateException if this serialiser contains no object.
	 */
	int write( OutputBitStream obs ) throws IOException;
	
	/** Sets the current value of this payload by reading from an input bit stream.
	 * 
	 * @param ibs a bit stream.
	 * @return the number of bits read.
	 */
	int read( InputBitStream ibs ) throws IOException;
	
	/** Returns the value of this payload.
	 * 
	 * <p>Implementing classes are expected to override covariantly the return
	 * value to the actual class stored by the payload.
	 * 
	 * @return the current value of this payload.
	 */
	Object get();
	
	/** Sets the current value of this payload.
	 * 
	 * @param o the new value of this payload.
	 */
	void set( Object o );
	
	/** Returns a copy of this payload.
	 * 
	 * <p>Implementing classes are expected to override covariantly the return
	 * value to the actual payload type.
	 * 
	 * @return a copy of this payload.
	 */
	Payload copy();

	/** Returns true if this payload instance is compatible with another instance.
	 * 
	 * @return true if this payload instance is compatible with another instance.
	 */
	boolean compatibleWith( Payload payload );
	
	/** Returns a payload filter matching the interval defined by the given parameters.
	 * 
	 * @param left the left extreme of the interval (inclusive). It will be cached (but not copied) internally.
	 * @param right the right extreme of the interval (exclusive). It will be cached (but not copied) internally.
	 * @return a payload filter for the interval defined above.
	 */
	public Predicate rangeFilter( final Payload left, final Payload right );
}
