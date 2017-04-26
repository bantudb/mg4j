package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2003-2016 Sebastiano Vigna 
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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/** A container for constants and enums related to index compression. 
 * 
 * <p>Several MG4J index-related methods require a set of flags specified as a <em>flag map</em>,
 * that is, an {@link java.util.EnumMap}
 * from {@linkplain Component components} to {@linkplain Coding codings} (see, e.g., 
 * {@link BitStreamIndexWriter}). For the special component {@link Component#PAYLOADS}, the only
 * admissible value is <code>null</code>.
 * 
 * 
 * <p>Besides declaring the necessary enums, this class contains a parsing method
 * that turns an array of the form <samp><var>component</var>:<var>coding</var></samp> into a flag map.
 *  
 * @author Sebastiano Vigna 
 * @since 1.2
 */

public class CompressionFlags {
	/** A component of the index. To each component, a flag map associates a {@linkplain Coding coding}. */
	public static enum Component { FREQUENCIES, POINTERS, PAYLOADS, COUNTS, POSITIONS };
	/** A coding for an index component. */
	public static enum Coding { UNARY, GAMMA, DELTA, SHIFTED_GAMMA, ZETA, GOLOMB, SKEWED_GOLOMB, ARITHMETIC, INTERPOLATIVE, NIBBLE }
	/** A string used by {@link #valueOf(String[], Map)} to disable a component. */
	public static final String NONE = "NONE";
	
	/** An unmodifiable map representing the default flags for a standard index. */
	public static final Map<Component,Coding> DEFAULT_STANDARD_INDEX;
	
	/** An unmodifiable map representing the default flags for a quasi-succinct index. */
	public static final Map<Component,Coding> DEFAULT_QUASI_SUCCINCT_INDEX;
	
	/** An unmodifiable map representing the default flags for a payload-based index. */
	public static final Map<Component,Coding> DEFAULT_PAYLOAD_INDEX;
	
	/** An unmodifiable map representing the default flags for a standard index.
	 * @deprecated As of MG4J 1.2, replaced by {@link #DEFAULT_STANDARD_INDEX}. 
	 */
	@Deprecated
	public static final Map<Component,Coding> DEFAULT;
	
	static {
		Map<Component,Coding> map = new EnumMap<Component,Coding>( Component.class );
		DEFAULT = DEFAULT_STANDARD_INDEX = Collections.unmodifiableMap( map );
		map.put( Component.FREQUENCIES, Coding.GAMMA );
		// This used to be GOLOMB, but precomputed codes made Golomb codes very slow in comparison
		map.put( Component.POINTERS, Coding.DELTA );
		map.put( Component.COUNTS, Coding.GAMMA );
		// This used to be GOLOMB, but experience has shown that loading sizes is always a problem.
		map.put( Component.POSITIONS, Coding.DELTA );

		map = new EnumMap<Component,Coding>( Component.class );
		DEFAULT_PAYLOAD_INDEX = Collections.unmodifiableMap( map );
		map.put( Component.FREQUENCIES, Coding.GAMMA );
		map.put( Component.POINTERS, Coding.DELTA );
		map.put( Component.PAYLOADS, null );

		map = new EnumMap<Component,Coding>( Component.class );
		DEFAULT_QUASI_SUCCINCT_INDEX = Collections.unmodifiableMap( map );
		map.put( Component.POINTERS, null );
		map.put( Component.COUNTS, null );
		map.put( Component.POSITIONS, null );
	}


	/** Returns a flag map corresponding to a given array of strings.
	 * 
	 * <p>This method takes an array of (possibly untrimmed) flag strings 
	 * of the form <samp><var>component</var>:<var>coding</var></samp> and turns
	 * them into a flag map (see the {@linkplain CompressionFlags introduction}).
	 * The flag map can be initialised by an optional default map, and
	 * the special value <samp>NONE</samp> for <samp><var>coding</var></samp> may be
	 * used to delete a key (the corresponding key in the flag map will be missing).
	 * 
	 * <p>It is acceptable that strings in the array have whitespace around.
	 * 
	 * @param flag an array of (possibly untrimmed) flag strings of 
	 * the form <samp><var>component</var>:<var>coding</var></samp>.
	 * @param defaultMap a optional flag map of default values, or <code>null</code>.
	 * @return the corresponding flag map.
	 */
	public static Map<Component,Coding> valueOf( final String[] flag, final Map<Component,Coding> defaultMap ) {
		final EnumMap<Component,Coding> m = defaultMap != null ? new EnumMap<Component,Coding>( defaultMap ) : 
			new EnumMap<Component,Coding>( Component.class );  
		
		for( int i = 0; i < flag.length; i++ ) {
			final String[] spec = flag[ i ].trim().split( ":" );
			if ( spec.length != 2 ) throw new IllegalArgumentException( "Bad format: " + flag[ i ] );
			if ( spec[ 1 ].equals( NONE ) ) m.remove( Component.valueOf( spec[ 0 ] ) ); 
			else m.put( Component.valueOf( spec[ 0 ] ), Coding.valueOf( spec[ 1 ] ) );
		}
		return m;
	}		 
}
