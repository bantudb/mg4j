package it.unimi.di.big.mg4j.search.score;

import java.util.Comparator;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */


/** A container used to return scored results with additional information. */

public final class DocumentScoreInfo<T> {
	/** The index of the document. */
	public long document;
	/** Its score. */
	public double score;
	/** Optional additional information. */
	public T info;

	public DocumentScoreInfo( final long document, final double score, final T info ) {
		this.document = document;
		this.score = score;
		this.info = info;
	}

	public DocumentScoreInfo( final long document, final double score ) {
		this.document = document;
		this.score = score;
	}

	public DocumentScoreInfo( final long document ) {
		this.document = document;
		this.score = -1;
	}

	public String toString() {
		return "[Document: " + document + "; score: " + score + "; info: " + info + "]";
	}
	
	/** A comparator that sorts {@link DocumentScoreInfo} instances by <em>increasing</em> document number. */
	
	public static final Comparator<DocumentScoreInfo<?>> DOCUMENT_COMPARATOR = new Comparator<DocumentScoreInfo<?>>() {
		public int compare( final DocumentScoreInfo<?> dsi0, final DocumentScoreInfo<?> dsi1 ) {
			return (int)Math.signum( dsi0.document - dsi1.document );
		}
	};
	
	/** A comparator that sorts {@link DocumentScoreInfo} instances by increasing score order and then by <em>decreasing</em> document order. */
	public static final Comparator<DocumentScoreInfo<?>> SCORE_DOCUMENT_COMPARATOR = new Comparator<DocumentScoreInfo<?>>() {
		public int compare( final DocumentScoreInfo<?> x, final DocumentScoreInfo<?> y ) {
			if ( x.score < y.score ) return -1;
			if ( x.score > y.score ) return 1;
			// Note that we want document in *increasing* score, but *decreasing* document number.
			return (int)Math.signum( y.document - x.document );
		}
	};
}
