package it.unimi.di.big.mg4j.search.visitor;

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
import it.unimi.di.big.mg4j.index.IndexIterator;

import java.io.IOException;

/**
 * A visitor collecting the counts of terms in a {@link it.unimi.di.big.mg4j.search.DocumentIterator}
 * tree.
 * 
 * <p><strong>Note</strong>: in fact, the documentation of this class clarifies also the usage of
 * {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor} and
 * {@link it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor}.
 * 
 * <p>Several scoring schemes, such as {@linkplain it.unimi.di.big.mg4j.search.score.BM25Scorer BM25}
 * or cosine-based measures, require the counts (number of
 * occurrences in a given document) of terms in the query. Since we do not want restrict the ability
 * of the user to specify sophisticated constraints such as term proximity, order, consecutivity,
 * etc., we prefer not to use a <em>bag-of-words</em> query model, in which the user simply inputs
 * a number of terms (in that case, of course, the definition of the count of each term is trivial).
 * Rather, we provide a group of visitors that make it possible to retrieve counts for each term
 * appearing in the query.
 * 
 * <p>Since MG4J provides multi-index queries, each count is actually associated with a pair index/term
 * (e.g., the count of <samp>class</samp> in the main text might be different from the count of
 * <samp>class</samp> in the title). Moreover, we must be careful to define a sensible semantics,
 * as when logical operators alternate there might be occurrences of a term in a query whose count
 * might give misleading information (in particular if the same term appear several times).
 * 
 * <p>Thus, we define a <em>true path</em> on the query tree (which parallels the composite tree of
 * the associated {@link it.unimi.di.big.mg4j.document.DocumentIterator}) as a path from the root that
 * passes only through nodes whose associated subquery evaluates to true (in the Boolean sense). A
 * counter-collection visitor records in the counter arrays only the counts of index/term pairs appearing at
 * the end of a true path.
 * 
 * <p>For instance, in a query like <samp>a OR (title:b AND c)</samp> in a document that contains
 * <samp>a</samp> and <samp>c</samp> in the main text, but does not contain <samp>b</samp> in the
 * title, <em>only the count of <samp>a</samp> will be taken into consideration</em>. In the
 * same way, for a query whose outmost operation is a negation no counter will ever be written.
 * 
 * <h3>Preparing and using counters</h3>
 * 
 * <p>Instance of this class are useful only in connection with a
 * {@link it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor} (and, in turn, with a
 * {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor}). More precisely, there are three
 * phases:
 * <ol>
 * <li>after creating the {@link it.unimi.di.big.mg4j.search.DocumentIterator}, 
 * {@linkplain it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor#prepare() prepare}
 * a {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor} and perform a
 * {@linkplain it.unimi.di.big.mg4j.search.DocumentIterator#accept(DocumentIteratorVisitor) visit} 
 * to gather term information and possibly cache some data about the
 * {@linkplain it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor#terms(Index) terms appearing in the iterator};
 * <li>{@linkplain it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor#prepare() prepare}
 * a {@link it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor} based on the previous
 * {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor}, and perform a visit to 
 * read frequencies and prepare counters;
 * <li>start iterating: after each call to
 * {@link it.unimi.di.big.mg4j.search.DocumentIterator#nextDocument() nextDocument()},
 * {@linkplain it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor#clear() clear the counters},
 * perform a
 * {@linkplain it.unimi.di.big.mg4j.search.DocumentIterator#acceptOnTruePaths(DocumentIteratorVisitor) visit along 
 * true paths} using an instance of this class and inspect the data gathered in the
 * {@linkplain it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor} (see, for example, the source code of
 * {@link it.unimi.di.big.mg4j.search.score.CountScorer}).
 * </ol>
 * 
 * <p>Note that all visitors are reusable: just 
 * {@link it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor#prepare() prepare()} them before usage, but
 * be careful as a {@link it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor} must be prepared and visited
 * <em>after</em> the associated {@link it.unimi.di.big.mg4j.search.visitor.TermCollectionVisitor}
 * has been prepared and visited. The {@link it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor#prepare() prepare()}
 * method of this class is a no-op, so it is not necessary to call it. 
 */


public class CounterCollectionVisitor extends AbstractDocumentIteratorVisitor {

	private final CounterSetupVisitor counterSetupVisitor;

	/** Creates a new counter-collection visitor based on a given counter-setup visitor.
	 * 
	 * @param counterSetupVisitor a counter-setup visitor.
	 */

	public CounterCollectionVisitor( CounterSetupVisitor counterSetupVisitor ) {
		this.counterSetupVisitor = counterSetupVisitor;
	}

	public Boolean visit( final IndexIterator indexIterator ) throws IOException {
		counterSetupVisitor.update( indexIterator );
		return Boolean.TRUE;
	}

	public String toString() {
		return "[CounterCollectionVisitor for " + counterSetupVisitor + "]";
	}
}
