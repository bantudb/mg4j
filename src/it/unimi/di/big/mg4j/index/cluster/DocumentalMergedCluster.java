package it.unimi.di.big.mg4j.index.cluster;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.util.BloomFilter;
import it.unimi.dsi.util.Properties;

/** A {@link it.unimi.di.big.mg4j.index.cluster.DocumentalCluster} that merges the
 * postings of its local indices. 
 *
 * @author Sebastiano Vigna
 */

public class DocumentalMergedCluster extends DocumentalCluster {
	private static final long serialVersionUID = 1L;
	
	public DocumentalMergedCluster( final Index[] localIndex, final DocumentalClusteringStrategy strategy, final boolean flat, final BloomFilter<Void>[] termFilter, final int numberOfDocuments, final int numberOfTerms, 
			final long numberOfPostings, final long numberOfOccurences, final int maxCount, final Payload payload, final boolean hasCounts, final boolean hasPositions,
			final TermProcessor termProcessor, final String field, final IntBigList sizes, final Properties properties ) {
		super( localIndex, strategy, flat, termFilter, numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurences, maxCount, payload, hasCounts, hasPositions, termProcessor, field, sizes, properties );
	}
}
