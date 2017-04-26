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


/** A marker for text/HTML output.
 * 
 * <p>This class has few instances, which are accessible by means of final static fields. 
 */

public class TextMarker implements Marker {
	/** A text marker showing the marked text in (ANSI) standout mode, with blocks surrounded by <samp>"&#46;&#46;"</samp> and terminated by a newline. */
	public final static TextMarker TEXT_STANDOUT = new TextMarker( "\u001B[7m", "\u001B[m", "...", "...   ", "", "\n" );
	/** A text marker showing the marked text in (ANSI) boldface, with blocks surrounded by <samp>"&#46;&#46;&#46;"</samp> and terminated by a newline. */
	public final static TextMarker TEXT_BOLDFACE = new TextMarker( "\u001B[1m", "\u001B[m", "...", "...   ", "", "\n" );
	/** An HTML marker showing the marked text in a <samp>strong</samp> element, surrounded by hellipsis (<samp>&hellip;</samp>) and terminated by a newline. */
	public final static TextMarker HTML_STRONG = new TextMarker( "<strong>", "</strong>", "\u2026", "\u2026\n", "<p>", "</p>" );

	/** The starting marker for a piece of block to be emphasized. */
	public final String startOfMark;
	/** The ending marker for a piece of block to be emphasized. */
	public final String endOfMark;
	/** The stating marker for a block. */
	public final String startOfBlock;
	/** The ending marker for a block. */
	public final String endOfBlock;
	/** The starting marker for a series of blocks belonging to the same field. */
	public final String startOfField;
	/** The ending marker for a series of blocks belonging to the same field. */
	public final String endOfField;
	
	/** Creates a new text marker.
	 * 
	 * @param startOfMark the starting marker for a piece of block to be emphasized.
	 * @param endOfMark the ending marker for a piece of block to be emphasized.
	 * @param startOfBlock the stating marker for a block.
	 * @param endOfBlock the ending marker for a block.
	 * @param startOfField the starting marker for a series of blocks belonging to the same field.
	 * @param endOfField the ending marker for a series of blocks belonging to the same field.
	 */
	public TextMarker( final String startOfMark, final String endOfMark, final String startOfBlock, final String endOfBlock, final String startOfField, final String endOfField ) {
		this.startOfMark = startOfMark;
		this.endOfMark = endOfMark;
		this.startOfBlock = startOfBlock;
		this.endOfBlock = endOfBlock;
		this.startOfField = startOfField;
		this.endOfField = endOfField;
	}
	
	public final String endOfBlock() {
		return endOfBlock;
	}
	public final String endOfField() {
		return endOfField;
	}
	public final String endOfMark() {
		return endOfMark;
	}
	public final String startOfBlock() {
		return startOfBlock;
	}
	public final String startOfField() {
		return startOfField;
	}
	public final String startOfMark() {
		return startOfMark;
	}
}
