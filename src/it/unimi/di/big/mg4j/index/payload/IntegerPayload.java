package it.unimi.di.big.mg4j.index.payload;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

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

/** A payload containing a long stored using &delta; coding.
 * 
 * <p>The method {@link #set(Object)} will accept any {@link Number}
 * that is in the range allowed by {@link Fast#int2nat(long)}.
 */


public class IntegerPayload extends AbstractPayload {
	private static final long serialVersionUID = 1L;
	/** Whether this payload has been ever set. */
	protected boolean unset = true;
	/** The current value of this payload, if {@link #unset} is false. */
	protected long value;

	public Long get() {
		if ( unset ) throw new IllegalStateException();
		return Long.valueOf( value );
	}

	public long getLong() {
		return value;
	}

	/** Sets the current value of this payload.
	 * 
	 * @param value the new value of this payload (must be
	 * in the range allowed by {@link Fast#int2nat(long)}).
	 */
	public void set( final long value ) {
		// Range check for Fast.int2nat()
		if ( value < -( 1L << 62 ) || value >= ( 1L << 62 ) ) throw new IllegalArgumentException( Long.toString( value ) );
		unset = false;
		this.value = value;
	}

	public void set( final Object value ) {
		set(((Number)value).longValue());
	}

	public int read( final InputBitStream ibs ) throws IOException {
		final long readBits = ibs.readBits();
		value = (int)Fast.nat2int( ibs.readLongDelta() );
		unset = false;
		return (int)( ibs.readBits() - readBits );
	}

	public int write( final OutputBitStream obs ) throws IOException {
		if ( unset ) throw new IllegalStateException();
		unset = false;
		return obs.writeLongDelta( Fast.int2nat( value ) );
	}

	public IntegerPayload copy() {
		final IntegerPayload copy = new IntegerPayload();
		copy.value = value;
		return copy;
	}

	public String toString() {
		if ( ! unset ) return get().toString();
		return "undefined";
	}

	public boolean compatibleWith( final Payload payload ) {
		return payload.getClass() == IntegerPayload.class;
	}

	public int compareTo( final Payload o ) {
		final long diff = value - ((IntegerPayload)o).value;
		return diff == 0 ? 0 : diff < 0 ? -1 : 1;
	}
	
	public boolean equals( final Payload o ) {
		return ( ( o instanceof IntegerPayload ) && ((IntegerPayload)o).value == value ); 
	}
	
	public int hashCode() {
		return HashCommon.long2int( value );
	}

	public Long parse( final String spec ) {
		return Long.valueOf( spec );
	}
	
}
