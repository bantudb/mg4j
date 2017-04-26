package it.unimi.di.big.mg4j.query;


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
 *  or FITfNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;

// ALERT: this class need desperately to be documented.

/** A mutable string with a special method to append text that should be marked.
 * 
 * <p>A marking mutable string can mark several <em>fields</em> (which will often correspond to indexed fields).
 * Each time you {@linkplain #startField(SelectedInterval[]) start a field}, you pass some intervals to be marked. Then,
 * you call {@link #appendAndMark(WordReader)}, which will add words and nonwords coming from the provided
 * {@link it.unimi.dsi.io.WordReader}, marking as suggested by the interval set. The number of words
 * around each interval can be set in the constructor. When a field is finished, you must call {@link #endField()}.
 */
public class MarkingMutableString extends MutableString {
	private static final long serialVersionUID = 1L;

	/** The default number of words before and after each interval. */
	public final static int DEFAULT_INTERVAL_SURROUND = 8;
	
	public boolean resume = true;
	public boolean marking;
	/** The current set of intervals for marking. */
	private SelectedInterval[] interval;
	private int count;
	private int currMarkingInterval, currResumeInterval;
	private boolean skipping;
	private boolean oneCharOut;

	private final Marker marker;
	private final EscapeStrategy escapeStrategy;

	/** An escaping strategy. Such a strategy is used by a {@link MarkingMutableString} to escape
	 * strings passed to the {@link MarkingMutableString#appendAndMark(WordReader)} method. */

	public interface EscapeStrategy {
		public MutableString escape( MutableString s );
	};
	
	private static final char[] HTML_ESCAPE_CHAR = new char[] { '<', '&' };
	private static final String[] HTML_ESCAPE_STRING = new String[] { "&lt;", "&amp;" };
	
	/** A singleton for the strategy that escapes HTML. */

	private static final class HtmlEscape implements EscapeStrategy {
		private HtmlEscape() {}
		public MutableString escape( final MutableString s ) {
			return s.replace( HTML_ESCAPE_CHAR, HTML_ESCAPE_STRING );
		}
	}
	
	/** A singleton for the null escape strategy (which does nothing). */
	
	public static final EscapeStrategy NULL_ESCAPE = new NullEscape(); 
	
	private static final class NullEscape implements EscapeStrategy {
		private NullEscape() {}
		public MutableString escape( final MutableString s ) {
			return s;
		}
	}
	
	/** A singleton for the HTML escape strategy. */
	
	public static final EscapeStrategy HTML_ESCAPE = new HtmlEscape();
	/** The number of surrounding word around each interval. */
	private final int intervalSurround; 
	
	/** Creates a new loose empty marking mutable string.
	 * 
	 * @param marker a marker that will decide how to highlight intervals.
	 * @param escapeStrategy the escape strategy for strings passed to {@link #appendAndMark(WordReader)}, or <code>null</code>.
	 * @param intervalSurround the number of words printed before and after each interval. 
	 */
	public MarkingMutableString( final Marker marker, final EscapeStrategy escapeStrategy, final int intervalSurround ) {
		this.marker = marker;
		this.escapeStrategy = escapeStrategy;
		this.intervalSurround = intervalSurround;
	}
	
	/** Creates a new loose empty marking mutable string default interval surround.
	 * 
	 * @param marker a marker that will decide how to highlight intervals.
	 * @param escapeStrategy the escape strategy for strings passed to {@link #appendAndMark(WordReader)}, or <code>null</code>.
	 */
	public MarkingMutableString( final Marker marker, final EscapeStrategy escapeStrategy ) {
		this( marker, escapeStrategy, DEFAULT_INTERVAL_SURROUND );
	}

	/** Creates a new loose empty marking mutable string with default interval surround, 
	 * no escaping strategy and no term processor.
	 * 
	 * @param marker a marker that will decide how to highlight intervals.
	 */
	public MarkingMutableString( final Marker marker ) {
		this( marker, NULL_ESCAPE );
	}
	
	/** Prepares this marking mutable string for a new field. We append
	 * {@link TextMarker#startOfField()},
	 * the interval marking state is reset and the intervals for marking are set to <code>interval</code>. 
	 * 
	 * @param interval the new selected-interval array for marking.
	 */
	
	public MarkingMutableString startField( final SelectedInterval[] interval ) {
		if ( interval == null ) throw new IllegalArgumentException();
		count = -1;
		currResumeInterval = currMarkingInterval = 0;
		skipping = oneCharOut = marking = false;
		this.interval = interval;
		append( marker.startOfField() );
		return this;
	}
	
	/** Closes the current field. The value of {@link TextMarker#startOfField()} is appended to the string.
	 */
	public MarkingMutableString endField() {
		append( marker.endOfField() );
		return this;
	}
	
	private int leftRadius( int currResumeInterval ) {
		switch( interval[ currResumeInterval].type ) {
		case WHOLE: return intervalSurround;
		case PREFIX: return intervalSurround;
		case SUFFIX: return 0;
		default: throw new IllegalArgumentException();
		}
	}
	
	private int rightRadius( int currResumeInterval ) {
		switch( interval[ currResumeInterval].type ) {
		case WHOLE: return intervalSurround;
		case PREFIX: return 0;
		case SUFFIX: return intervalSurround;
		default: throw new IllegalArgumentException();
		}
	}
	
	public MarkingMutableString appendAndMark( final String s ) {
		return appendAndMark( new MutableString( s ) );
	}
	
	public MarkingMutableString appendAndMark( final MutableString s ) {
		return appendAndMark( new FastBufferedReader( s ) );
	}
	
	public MarkingMutableString appendAndMark( final WordReader wordReader ) {
		//System.err.println( interval[ currInterval ] + "|" + new String( array, offset, length ) );
		
		MutableString word = new MutableString(), nonWord = new MutableString();
		try {
			while( wordReader.next( word, nonWord ) ) {
				if ( word.length() != 0 ) count++;
				
				if ( resume ) {
					while( currResumeInterval < interval.length && interval[ currResumeInterval ].interval.compareTo( count, leftRadius( currResumeInterval), rightRadius( currResumeInterval ) ) > 0 ) currResumeInterval++;
					if ( currResumeInterval == interval.length || ! interval[ currResumeInterval ].interval.contains( count, leftRadius( currResumeInterval), rightRadius( currResumeInterval ) ) ) {
						if ( ! skipping && oneCharOut ) append( marker.endOfBlock() );
						// There's nothing else we can do...
						if ( resume && currResumeInterval == interval.length ) return this;
						// Otherwise, we continue, but skipping.
						skipping = true;
						continue;
					}
					
					if ( skipping ) append( marker.startOfBlock() );
					skipping = false;
				}
				
				if ( word.length() !=0 ) {
					if ( ! marking && currMarkingInterval < interval.length && interval[ currMarkingInterval ].interval.contains( count ) ) {
						append( marker.startOfMark() );
						marking = true;
					}
					
					append( word );

					if ( marking && ( currMarkingInterval == interval.length || ! interval[ currMarkingInterval ].interval.contains( count + 1 ) ) ) {
						append( marker.endOfMark() );
						marking = false;
					}

					oneCharOut = true;
					if ( currMarkingInterval < interval.length && interval[ currMarkingInterval ].interval.compareTo( count + 1 ) > 0 ) currMarkingInterval++;
				}
				
				if ( nonWord.length() > 0 ) {
					oneCharOut = true;
					nonWord.squeezeWhitespace();
					append( escapeStrategy.escape( nonWord ) );
				}
			}
			
			if ( marking ) append( marker.endOfMark() );
		} catch ( IOException e ) {
			throw new RuntimeException( e );
		}
		
		return this;
	}
}
