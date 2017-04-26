package it.unimi.di.big.mg4j.query.nodes;

import it.unimi.dsi.lang.MutableString;

public class DoublingTermExpander extends AbstractTermExpander {

	@Override
	public Query expand( Term term ) {
		return new MultiTerm( term, new Term( new MutableString( term.term ).append( term.term ) ) );
	}

	@Override
	public Query expand( Prefix prefix ) {
		return new MultiTerm( new Term( prefix.prefix ), new Term( new MutableString( prefix.prefix ).append( prefix.prefix ) ) );
	}
}
