package it.unimi.di.big.mg4j.util.parser.callback;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Paolo Boldi
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

import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.parser.Attribute;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.Element;
import it.unimi.dsi.parser.callback.DefaultCallback;
import it.unimi.dsi.util.CircularCharArrayBuffer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A callback extracting anchor text. When instantiating the extractor, you can specify the number of characters to
 * be considered before the anchor, after the anchor or during the anchor (just the first characters are taken into
 * consideration in the last two characters, and just the last ones in the first case). 
 * 
 * <p>At the end of parsing, the result (the list of anchors) is available in {@link #anchors}, whose
 * elements provide the content of the <samp>href</samp> attribute 
 * the text of the anchor and around the anchor; text is however modified so that fragment of words at the beginning
 * of the pre-anchor context, or at the end of the post-anchor context, are cut away.
 * 
 * <p>For example, a fragment like:
 * 
 * <code>
 *    ...foo fOO FOO FOO <a href="xxx">ANCHOR TEXT</a> BAR BAR BAr bar... 
 * </code>
 * 
 * (where the uppercase part represents the pre- and post-anchor context) generates the element
 * 
 * <code>
 * 		Anchor("xxx", "FOO FOO ANCHOR TEXT BAR BAR")
 * </code>
 */

public class AnchorExtractor extends DefaultCallback {
	public static final Logger LOGGER = LoggerFactory.getLogger( AnchorExtractor.class );
	public static final boolean DEBUG = false;
	
	/** A class representing an anchor. It is used to return the results of parsing. 
	 * 
	 */
	public final static class Anchor implements VirtualDocumentFragment {
		private static final long serialVersionUID = 1L;
		/** The content of the <samp>href</samp> attribute for this anchor. */
		private final MutableString href;
		/** The text surrounding this anchor. */
		private final MutableString anchorText;
		
		public Anchor( final MutableString href, final MutableString anchorText ) {
			this.href = href;
			this.anchorText = anchorText;
		}

		public MutableString documentSpecifier() {
			return href;
		}

		public MutableString text() {
			return anchorText;
		}
		
		public String toString() {
			return "<" + href + ", \"" + anchorText + "\">";
		}
	}
	
	/** The resulting list of {@linkplain Anchor anchors}. */
	public final ObjectList<Anchor> anchors = new ObjectArrayList<Anchor>();

	/** The circular buffer for pre-anchor context. */
	private final CircularCharArrayBuffer preAnchor;
	/** The circular buffer for anchor. */
	private final MutableString anchor;
	/** The maximum number of characters in the anchor. */
	private final int maxAnchor;
	/** The maximum number of characters after anchor. */
	private final int maxPostAnchor;
	/** The post-anchor. */
	private final MutableString postAnchor;
	/** A token that will be inserted to delimit the anchor text, or {@code null} for no delimiter. */
	private final String delimiter;
	/** The current URL (if state is IN_ANCHOR). */
	private MutableString url;
	/** The resulting string (pre+anchor+post). */
	private MutableString result;
	/** When an anchor opens, the pre-anchor buffer is copied in this array. */
	private char[] preAnchorArray;

	private enum State {
		BEFORE_ANCHOR, IN_ANCHOR, AFTER_ANCHOR
	};
	private State state;
	
	/** Creates a new anchor extractor.
	 * 
	 * @param maxPreAnchor maximum number of characters before an anchor.
	 * @param maxAnchor maximum number of characters in an anchor.
	 * @param maxPostAnchor maximum number of characters after an anchor.
	 */
	public AnchorExtractor( final int maxPreAnchor, final int maxAnchor, final int maxPostAnchor ) {
		this( maxPreAnchor, maxAnchor, maxPostAnchor, null);
	}

	/** Creates a new anchor extractor.
	 * 
	 * @param maxPreAnchor maximum number of characters before an anchor.
	 * @param maxAnchor maximum number of characters in an anchor.
	 * @param maxPostAnchor maximum number of characters after an anchor.
	 * @param delimiter a token that will be inserted to delimit the anchor text, or {@code null} for no delimiter.
	 */
	public AnchorExtractor( final int maxPreAnchor, final int maxAnchor, final int maxPostAnchor, final String delimiter ) {
		preAnchor = new CircularCharArrayBuffer( maxPreAnchor );
		anchor = new MutableString( maxAnchor );
		postAnchor = new MutableString( maxPostAnchor );
		result = new MutableString( maxPreAnchor + maxAnchor + maxPostAnchor );
		this.maxPostAnchor = maxPostAnchor;
		this.maxAnchor = maxAnchor;
		this.delimiter = delimiter;
		state = State.BEFORE_ANCHOR;
	}

	public void configure( final BulletParser parser ) {
		parser.parseTags( true );
		parser.parseAttributes( true );
		parser.parseText( true );
		parser.parseAttribute( Attribute.HREF );
	}

	public void startDocument() {
		state = State.BEFORE_ANCHOR;
		anchors.clear();
		preAnchor.clear();
		anchor.setLength( 0 );
		postAnchor.setLength( 0 );
		url = null;
	}
	
	public void endDocument() {
		if ( url != null ) {
			emit();
		}
		url = null;
	}
	
	public boolean startElement( final Element element, final Map<Attribute,MutableString> attrMap ) {		
		if ( element == Element.A && attrMap != null && attrMap.containsKey( Attribute.HREF ) ) {
			if ( state == State.AFTER_ANCHOR ) {
				emit();
				state = State.BEFORE_ANCHOR;
			}
			if ( state == State.BEFORE_ANCHOR ) {
				preAnchorArray = preAnchor.toCharArray();
				preAnchor.clear();
				if ( DEBUG ) System.out.println( "Freezing now pre: <" + new String( preAnchorArray ) + ">" );
				state = State.IN_ANCHOR;
				url = attrMap.get( Attribute.HREF );
				anchor.setLength( 0 );
				postAnchor.setLength( 0 );
			} 
		}
		return true;
	}
	
	public boolean endElement( final Element element ) {
		if ( element == Element.A && state == State.IN_ANCHOR ) {
			state = State.AFTER_ANCHOR;
		}
		return true;
	}
	
	public boolean characters( final char[] characters, final int offset, final int length, final boolean flowBroken ) {
		switch ( state ) {
			case BEFORE_ANCHOR: 
				preAnchor.add( characters, offset, length );
				break;
			case IN_ANCHOR:
				anchor.append( characters, offset, Math.min( length, maxAnchor - anchor.length() ) );
				break;
			case AFTER_ANCHOR:
				preAnchor.add( characters, offset, length );
				postAnchor.append( characters, offset, Math.min( length, maxPostAnchor - postAnchor.length() ) );
				break;
		}
		if ( state == State.AFTER_ANCHOR && postAnchor.length() == maxPostAnchor && url != null ) {
			emit();
			state = State.BEFORE_ANCHOR;
		}
		return true;
	}


	private void emit() {
		int posPre, posPost, posAnchor;
		
		// Cut pre until the first start of word
		posPre = 0;
		if ( preAnchorArray.length > 0 && Character.isLetterOrDigit( preAnchorArray[ posPre ] ) )
			// Skip starting non-space
			for ( ; posPre < preAnchorArray.length && Character.isLetterOrDigit( preAnchorArray[ posPre ] ); posPre++ );
		// Same for post
		char[] postAnchorArray = postAnchor.array();
		posPost = postAnchor.length() - 1;
		if ( posPost >= 0 && Character.isLetterOrDigit( postAnchorArray[ posPost ] ) ) {
			// Skip ending non-space 
			for ( ; posPost >= 0 && Character.isLetterOrDigit( postAnchorArray[ posPost ] ); posPost-- );
		}
		// Same for anchor
		char[] anchorArray = anchor.array();
		posAnchor = anchor.length() - 1;
		if ( anchor.length() == maxAnchor && posAnchor >= 0 && Character.isLetterOrDigit( anchorArray[ posAnchor ] ) )
			// Skip starting non-space
			for ( ; posAnchor >= 0 && Character.isLetterOrDigit( anchorArray[ posAnchor ] ); posAnchor-- );
			
		result.setLength( 0 );
		result.append( preAnchorArray, posPre, preAnchorArray.length - posPre );
		if (delimiter != null) result.append(delimiter).append(' ');
		result.append( anchorArray, 0, posAnchor + 1 );
		if (delimiter != null) result.append(' ').append(delimiter);
		result.append( postAnchorArray, 0, posPost + 1 );
		anchors.add( new Anchor( url, result.copy() ) );
		url = null;
	}
}
