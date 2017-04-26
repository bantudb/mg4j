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


/** A strategy for marking words.
 * 
 * <p>When showing snippets to the user, it is necessary to highlight intervals, and to mark the
 * start and the end of an excerpt. The first effect is usually some kind of highlighting or boldfacing, the
 * second an ellipsis or similar punctuation. This class allows to create interchangeable markers for
 * different purposes or kind of text (e.g., {@link it.unimi.di.big.mg4j.query.TextMarker#TEXT_STANDOUT},
 * {@link it.unimi.di.big.mg4j.query.TextMarker#HTML_STRONG}, and so on).
 */
public interface Marker {
	
	/** Returns the starting delimiter of a marked part.
	 *  
	 * @return the starting delimiter of a marked part.
	 */
	public String startOfMark();

	/** Returns the ending delimiter of a marked part.
	 *  
	 * @return the ending delimiter of a marked part.
	 */
	public String endOfMark();

	/** Returns the starting delimiter of a block.
	 *  
	 * @return the starting delimiter of a block.
	 */
	public String startOfBlock();

	/** Returns the ending delimiter of a block.
	 *  
	 * @return the ending delimiter of a block.
	 */
	public String endOfBlock();
	
	/** Returns the starting delimiter of a field.
	 *  
	 * @return the starting delimiter of a field.
	 */
	public String startOfField();

	/** Returns the ending delimiter of a field.
	 *  
	 * @return the ending delimiter of a field.
	 */
	public String endOfField();
	


}
