package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
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

import it.unimi.dsi.lang.MutableString;

/** A term processor downcasing all characters. */

public class DowncaseTermProcessor implements TermProcessor {
	private static final long serialVersionUID = 1L;

	private final static DowncaseTermProcessor INSTANCE = new DowncaseTermProcessor();
	
	public final static TermProcessor getInstance() {
		return INSTANCE;
	}
	
	private DowncaseTermProcessor() {}
	
	public boolean processTerm( final MutableString term ) {
		if ( term == null ) return false;
		term.toLowerCase();
		return true;
	}
	
	public boolean processPrefix( final MutableString prefix ) {
		return processTerm( prefix );
	}
	
	private Object readResolve() {
		return INSTANCE;
	}

	public String toString() {
		return this.getClass().getName();
	}
	
	public DowncaseTermProcessor copy() { return this; }
}
