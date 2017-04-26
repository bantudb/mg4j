package it.unimi.di.big.mg4j.index.payload;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.Locale;


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

/** A payload containing a {@linkplain Date date} expressed as seconds from the Epoch 
 * and stored using &delta; coding.
 * 
 * <p>Note that since the date is stored in <em>seconds</em>, it is possible that
 * setting a current value and getting it back does not given an equal {@link Date} object.
 */


public class DatePayload extends AbstractPayload {
	private static final long serialVersionUID = 1L;
	protected long secondsFromEpoch = Long.MAX_VALUE;

	public Date get() {
		if ( secondsFromEpoch == Long.MAX_VALUE ) throw new IllegalStateException();
		return new Date( secondsFromEpoch * 1000 );
	}

	public void set( final Object date ) {
		secondsFromEpoch = ((Date)date).getTime() / 1000;
	}

	public int read( final InputBitStream ibs ) throws IOException {
		final long readBits = ibs.readBits();
		secondsFromEpoch = Fast.nat2int( ibs.readLongDelta() ); 
		return (int)( ibs.readBits() - readBits );
	}

	public int write( final OutputBitStream obs ) throws IOException {
		if ( secondsFromEpoch == Long.MAX_VALUE ) throw new IllegalStateException();
		return obs.writeLongDelta( Fast.int2nat( secondsFromEpoch ) );
	}

	public DatePayload copy() {
		final DatePayload copy = new DatePayload();
		copy.secondsFromEpoch = secondsFromEpoch;
		return copy;
	}

	public String toString() {
		if ( secondsFromEpoch != Long.MAX_VALUE ) return get().toString();
		return "undefined";
	}

	public boolean compatibleWith( final Payload payload ) {
		return payload.getClass() == DatePayload.class;
	}

	public int compareTo( final Payload o ) {
		final long diff = secondsFromEpoch - ((DatePayload)o).secondsFromEpoch;
		return diff == 0 ? 0 : diff < 0 ? -1 : 1;
	}
	
	public boolean equals( final Payload o ) {
		return ( ( o instanceof DatePayload ) && ((DatePayload)o).secondsFromEpoch == secondsFromEpoch ); 
	}
	
	public int hashCode() {
		return (int)secondsFromEpoch;
	}
	
	public Date parse( final String spec ) throws ParseException {
		return DateFormat.getDateInstance( DateFormat.SHORT, Locale.UK ).parse( spec );
	}
}
