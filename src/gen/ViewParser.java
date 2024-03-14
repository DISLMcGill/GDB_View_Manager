// Generated from src/View.g4 by ANTLR 4.8
package gen;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ViewParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
			new PredictionContextCache();
	public static final int
			T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9,
			T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17,
			T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24,
			T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, T__30=31,
			T__31=32, T__32=33, T__33=34, T__34=35, T__35=36, T__36=37, T__37=38,
			T__38=39, T__39=40, T__40=41, T__41=42, T__42=43, T__43=44, T__44=45,
			T__45=46, T__46=47, T__47=48, T__48=49, T__49=50, T__50=51, T__51=52,
			T__52=53, T__53=54, T__54=55, KEYWORD=56, RETURN=57, COMMAND=58, COMPARISON=59,
			OPERATOR=60, CONSTANTS=61, VALUE=62, NAME=63, WHITESPACE=64, ANY=65;
	public static final int
			RULE_root = 0, RULE_scope = 1, RULE_query = 2, RULE_changegraph = 3, RULE_pipeline = 4,
			RULE_replacements = 5, RULE_iteration = 6, RULE_validVal = 7, RULE_iterationCase = 8,
			RULE_size = 9, RULE_range = 10, RULE_pipeconditions = 11, RULE_pipeexpr = 12,
			RULE_function = 13, RULE_viewuse = 14, RULE_usedviews = 15, RULE_viewatom = 16,
			RULE_returnstmt = 17, RULE_retval = 18, RULE_expr = 19, RULE_variable = 20,
			RULE_type = 21, RULE_nodeName = 22, RULE_relation = 23, RULE_relationValue = 24,
			RULE_relationLength = 25, RULE_path = 26, RULE_conditions = 27, RULE_boolexpr = 28,
			RULE_attribute = 29, RULE_val = 30, RULE_indexing = 31, RULE_setattr = 32,
			RULE_arithmetic = 33, RULE_insertion = 34, RULE_insertrelation = 35, RULE_insertionVar = 36,
			RULE_insertAttributes = 37, RULE_as = 38, RULE_exists = 39;
	private static String[] makeRuleNames() {
		return new String[] {
				"root", "scope", "query", "changegraph", "pipeline", "replacements",
				"iteration", "validVal", "iterationCase", "size", "range", "pipeconditions",
				"pipeexpr", "function", "viewuse", "usedviews", "viewatom", "returnstmt",
				"retval", "expr", "variable", "type", "nodeName", "relation", "relationValue",
				"relationLength", "path", "conditions", "boolexpr", "attribute", "val",
				"indexing", "setattr", "arithmetic", "insertion", "insertrelation", "insertionVar",
				"insertAttributes", "as", "exists"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
				null, "'CG'", "'LOCAL'", "'GLOBAL'", "'SET'", "'DELETE'", "'REMOVE'",
				"'CREATE'", "'WITH'", "','", "'['", "'IN'", "'('", "')'", "'|'", "']'",
				"'NULL'", "'CASE WHEN'", "'THEN'", "'ELSE'", "'END'", "'SIZE'", "'size'",
				"'RANGE'", "'range'", "'WHERE'", "'='", "'COLLECT('", "'UNWIND'", "'COUNT('",
				"'COUNT(*)'", "'MAX('", "'WITH VIEWS'", "'-['", "']-'", "']->'", "'<-['",
				"'NODES('", "':'", "'*'", "'..'", "'*..'", "'AND'", "'OR'", "'not'",
				"'NOT'", "'.'", "'+'", "'-'", "'/'", "'{'", "'}'", "'AS'", "'as'", "'EXISTS'",
				"'exists'", null, "'RETURN'", "'CREATE VIEW AS'", null, null, null, null,
				null, "' '"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
				null, null, null, null, null, null, null, null, null, null, null, null,
				null, null, null, null, null, null, null, null, null, null, null, null,
				null, null, null, null, null, null, null, null, null, null, null, null,
				null, null, null, null, null, null, null, null, null, null, null, null,
				null, null, null, null, null, null, null, null, "KEYWORD", "RETURN",
				"COMMAND", "COMPARISON", "OPERATOR", "CONSTANTS", "VALUE", "NAME", "WHITESPACE",
				"ANY"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "View.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public ViewParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class RootContext extends ParserRuleContext {
		public TerminalNode COMMAND() { return getToken(ViewParser.COMMAND, 0); }
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public ViewuseContext viewuse() {
			return getRuleContext(ViewuseContext.class,0);
		}
		public ScopeContext scope() {
			return getRuleContext(ScopeContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ChangegraphContext changegraph() {
			return getRuleContext(ChangegraphContext.class,0);
		}
		public RootContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_root; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterRoot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitRoot(this);
		}
	}

	public final RootContext root() throws RecognitionException {
		RootContext _localctx = new RootContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_root);
		try {
			setState(93);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(80);
					match(COMMAND);
					setState(81);
					match(NAME);
					setState(82);
					viewuse();
					setState(83);
					scope();
					setState(84);
					query();
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(86);
					match(T__0);
					setState(87);
					query();
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(88);
					changegraph();
				}
				break;
				case 4:
					enterOuterAlt(_localctx, 4);
				{
					setState(89);
					viewuse();
					setState(90);
					scope();
					setState(91);
					query();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ScopeContext extends ParserRuleContext {
		public ScopeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scope; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterScope(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitScope(this);
		}
	}

	public final ScopeContext scope() throws RecognitionException {
		ScopeContext _localctx = new ScopeContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_scope);
		try {
			setState(98);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case T__1:
					enterOuterAlt(_localctx, 1);
				{
					setState(95);
					match(T__1);
				}
				break;
				case T__2:
					enterOuterAlt(_localctx, 2);
				{
					setState(96);
					match(T__2);
				}
				break;
				case KEYWORD:
					enterOuterAlt(_localctx, 3);
				{
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public TerminalNode KEYWORD() { return getToken(ViewParser.KEYWORD, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public ConditionsContext conditions() {
			return getRuleContext(ConditionsContext.class,0);
		}
		public ReturnstmtContext returnstmt() {
			return getRuleContext(ReturnstmtContext.class,0);
		}
		public PipelineContext pipeline() {
			return getRuleContext(PipelineContext.class,0);
		}
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitQuery(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_query);
		int _la;
		try {
			setState(116);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(100);
					match(KEYWORD);
					setState(101);
					expr();
					setState(102);
					conditions();
					setState(104);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__7) {
						{
							setState(103);
							pipeline();
						}
					}

					setState(106);
					returnstmt();
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(108);
					match(KEYWORD);
					setState(109);
					path();
					setState(110);
					conditions();
					setState(112);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__7) {
						{
							setState(111);
							pipeline();
						}
					}

					setState(114);
					returnstmt();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ChangegraphContext extends ParserRuleContext {
		public TerminalNode KEYWORD() { return getToken(ViewParser.KEYWORD, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public ConditionsContext conditions() {
			return getRuleContext(ConditionsContext.class,0);
		}
		public SetattrContext setattr() {
			return getRuleContext(SetattrContext.class,0);
		}
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public AttributeContext attribute() {
			return getRuleContext(AttributeContext.class,0);
		}
		public InsertionContext insertion() {
			return getRuleContext(InsertionContext.class,0);
		}
		public ChangegraphContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_changegraph; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterChangegraph(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitChangegraph(this);
		}
	}

	public final ChangegraphContext changegraph() throws RecognitionException {
		ChangegraphContext _localctx = new ChangegraphContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_changegraph);
		try {
			setState(144);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(118);
					match(KEYWORD);
					setState(119);
					expr();
					setState(120);
					conditions();
					setState(121);
					match(T__3);
					setState(122);
					setattr();
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(124);
					match(KEYWORD);
					setState(125);
					expr();
					setState(126);
					conditions();
					setState(127);
					match(T__4);
					setState(128);
					match(NAME);
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(130);
					match(KEYWORD);
					setState(131);
					expr();
					setState(132);
					conditions();
					setState(133);
					match(T__5);
					setState(134);
					attribute(0);
				}
				break;
				case 4:
					enterOuterAlt(_localctx, 4);
				{
					setState(136);
					match(KEYWORD);
					setState(137);
					expr();
					setState(138);
					conditions();
					setState(139);
					match(T__6);
					setState(140);
					insertion(0);
				}
				break;
				case 5:
					enterOuterAlt(_localctx, 5);
				{
					setState(142);
					match(T__6);
					setState(143);
					insertion(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PipelineContext extends ParserRuleContext {
		public ReplacementsContext replacements() {
			return getRuleContext(ReplacementsContext.class,0);
		}
		public PipeconditionsContext pipeconditions() {
			return getRuleContext(PipeconditionsContext.class,0);
		}
		public PipelineContext pipeline() {
			return getRuleContext(PipelineContext.class,0);
		}
		public PipelineContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pipeline; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterPipeline(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitPipeline(this);
		}
	}

	public final PipelineContext pipeline() throws RecognitionException {
		PipelineContext _localctx = new PipelineContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_pipeline);
		try {
			setState(155);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(146);
					match(T__7);
					setState(147);
					replacements();
					setState(148);
					pipeconditions();
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(150);
					match(T__7);
					setState(151);
					replacements();
					setState(152);
					pipeconditions();
					setState(153);
					pipeline();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ReplacementsContext extends ParserRuleContext {
		public List<TerminalNode> NAME() { return getTokens(ViewParser.NAME); }
		public TerminalNode NAME(int i) {
			return getToken(ViewParser.NAME, i);
		}
		public AsContext as() {
			return getRuleContext(AsContext.class,0);
		}
		public ReplacementsContext replacements() {
			return getRuleContext(ReplacementsContext.class,0);
		}
		public FunctionContext function() {
			return getRuleContext(FunctionContext.class,0);
		}
		public IterationContext iteration() {
			return getRuleContext(IterationContext.class,0);
		}
		public ReplacementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_replacements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterReplacements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitReplacements(this);
		}
	}

	public final ReplacementsContext replacements() throws RecognitionException {
		ReplacementsContext _localctx = new ReplacementsContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_replacements);
		try {
			setState(191);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(157);
					match(NAME);
					setState(158);
					as();
					setState(159);
					match(NAME);
					{
						setState(160);
						match(T__8);
						setState(161);
						replacements();
					}
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(163);
					function();
					setState(164);
					as();
					setState(165);
					match(NAME);
					{
						setState(166);
						match(T__8);
						setState(167);
						replacements();
					}
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(169);
					match(NAME);
					{
						setState(170);
						match(T__8);
						setState(171);
						replacements();
					}
				}
				break;
				case 4:
					enterOuterAlt(_localctx, 4);
				{
					setState(172);
					match(NAME);
					setState(173);
					as();
					setState(174);
					match(NAME);
				}
				break;
				case 5:
					enterOuterAlt(_localctx, 5);
				{
					setState(176);
					function();
					setState(177);
					as();
					setState(178);
					match(NAME);
				}
				break;
				case 6:
					enterOuterAlt(_localctx, 6);
				{
					setState(180);
					match(NAME);
				}
				break;
				case 7:
					enterOuterAlt(_localctx, 7);
				{
					setState(181);
					iteration();
					setState(182);
					as();
					setState(183);
					match(NAME);
					{
						setState(184);
						match(T__8);
						setState(185);
						replacements();
					}
				}
				break;
				case 8:
					enterOuterAlt(_localctx, 8);
				{
					setState(187);
					iteration();
					setState(188);
					as();
					setState(189);
					match(NAME);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IterationContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public RangeContext range() {
			return getRuleContext(RangeContext.class,0);
		}
		public List<ValidValContext> validVal() {
			return getRuleContexts(ValidValContext.class);
		}
		public ValidValContext validVal(int i) {
			return getRuleContext(ValidValContext.class,i);
		}
		public IterationCaseContext iterationCase() {
			return getRuleContext(IterationCaseContext.class,0);
		}
		public IterationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_iteration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterIteration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitIteration(this);
		}
	}

	public final IterationContext iteration() throws RecognitionException {
		IterationContext _localctx = new IterationContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_iteration);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(193);
				match(T__9);
				setState(194);
				match(NAME);
				setState(195);
				match(T__10);
				setState(196);
				range();
				setState(197);
				match(T__11);
				setState(198);
				validVal(0);
				setState(199);
				match(T__8);
				setState(200);
				validVal(0);
				setState(201);
				match(T__12);
				setState(202);
				match(T__13);
				setState(203);
				iterationCase();
				setState(204);
				match(T__14);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValidValContext extends ParserRuleContext {
		public SizeContext size() {
			return getRuleContext(SizeContext.class,0);
		}
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public TerminalNode VALUE() { return getToken(ViewParser.VALUE, 0); }
		public IndexingContext indexing() {
			return getRuleContext(IndexingContext.class,0);
		}
		public List<ValidValContext> validVal() {
			return getRuleContexts(ValidValContext.class);
		}
		public ValidValContext validVal(int i) {
			return getRuleContext(ValidValContext.class,i);
		}
		public ArithmeticContext arithmetic() {
			return getRuleContext(ArithmeticContext.class,0);
		}
		public ValidValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_validVal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterValidVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitValidVal(this);
		}
	}

	public final ValidValContext validVal() throws RecognitionException {
		return validVal(0);
	}

	private ValidValContext validVal(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValidValContext _localctx = new ValidValContext(_ctx, _parentState);
		ValidValContext _prevctx = _localctx;
		int _startState = 14;
		enterRecursionRule(_localctx, 14, RULE_validVal, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(215);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
					case T__20:
					case T__21:
					{
						setState(207);
						size();
						setState(208);
						match(T__11);
						setState(209);
						match(NAME);
						setState(210);
						match(T__12);
					}
					break;
					case VALUE:
					{
						setState(212);
						match(VALUE);
					}
					break;
					case T__15:
					{
						setState(213);
						match(T__15);
					}
					break;
					case NAME:
					{
						setState(214);
						indexing();
					}
					break;
					default:
						throw new NoViableAltException(this);
				}
				_ctx.stop = _input.LT(-1);
				setState(223);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						if ( _parseListeners!=null ) triggerExitRuleEvent();
						_prevctx = _localctx;
						{
							{
								_localctx = new ValidValContext(_parentctx, _parentState);
								pushNewRecursionContext(_localctx, _startState, RULE_validVal);
								setState(217);
								if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
								setState(218);
								arithmetic();
								setState(219);
								validVal(5);
							}
						}
					}
					setState(225);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class IterationCaseContext extends ParserRuleContext {
		public PipeexprContext pipeexpr() {
			return getRuleContext(PipeexprContext.class,0);
		}
		public List<ValidValContext> validVal() {
			return getRuleContexts(ValidValContext.class);
		}
		public ValidValContext validVal(int i) {
			return getRuleContext(ValidValContext.class,i);
		}
		public IterationCaseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_iterationCase; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterIterationCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitIterationCase(this);
		}
	}

	public final IterationCaseContext iterationCase() throws RecognitionException {
		IterationCaseContext _localctx = new IterationCaseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_iterationCase);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(226);
				match(T__16);
				setState(227);
				pipeexpr();
				setState(228);
				match(T__17);
				setState(229);
				validVal(0);
				setState(230);
				match(T__18);
				setState(231);
				validVal(0);
				setState(232);
				match(T__19);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SizeContext extends ParserRuleContext {
		public SizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterSize(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitSize(this);
		}
	}

	public final SizeContext size() throws RecognitionException {
		SizeContext _localctx = new SizeContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_size);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(234);
				_la = _input.LA(1);
				if ( !(_la==T__20 || _la==T__21) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeContext extends ParserRuleContext {
		public RangeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_range; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterRange(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitRange(this);
		}
	}

	public final RangeContext range() throws RecognitionException {
		RangeContext _localctx = new RangeContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_range);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(236);
				_la = _input.LA(1);
				if ( !(_la==T__22 || _la==T__23) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PipeconditionsContext extends ParserRuleContext {
		public PipeexprContext pipeexpr() {
			return getRuleContext(PipeexprContext.class,0);
		}
		public PipeconditionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pipeconditions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterPipeconditions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitPipeconditions(this);
		}
	}

	public final PipeconditionsContext pipeconditions() throws RecognitionException {
		PipeconditionsContext _localctx = new PipeconditionsContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_pipeconditions);
		try {
			setState(241);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case T__24:
					enterOuterAlt(_localctx, 1);
				{
					setState(238);
					match(T__24);
					setState(239);
					pipeexpr();
				}
				break;
				case T__7:
				case RETURN:
					enterOuterAlt(_localctx, 2);
				{
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PipeexprContext extends ParserRuleContext {
		public List<AttributeContext> attribute() {
			return getRuleContexts(AttributeContext.class);
		}
		public AttributeContext attribute(int i) {
			return getRuleContext(AttributeContext.class,i);
		}
		public TerminalNode COMPARISON() { return getToken(ViewParser.COMPARISON, 0); }
		public ValContext val() {
			return getRuleContext(ValContext.class,0);
		}
		public FunctionContext function() {
			return getRuleContext(FunctionContext.class,0);
		}
		public PipeexprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pipeexpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterPipeexpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitPipeexpr(this);
		}
	}

	public final PipeexprContext pipeexpr() throws RecognitionException {
		PipeexprContext _localctx = new PipeexprContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_pipeexpr);
		try {
			setState(275);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(243);
					attribute(0);
					setState(244);
					match(COMPARISON);
					setState(245);
					attribute(0);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(247);
					attribute(0);
					setState(248);
					match(COMPARISON);
					setState(249);
					val();
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(251);
					attribute(0);
					setState(252);
					match(T__25);
					setState(253);
					attribute(0);
				}
				break;
				case 4:
					enterOuterAlt(_localctx, 4);
				{
					setState(255);
					attribute(0);
					setState(256);
					match(T__25);
					setState(257);
					val();
				}
				break;
				case 5:
					enterOuterAlt(_localctx, 5);
				{
					setState(259);
					function();
					setState(260);
					match(COMPARISON);
					setState(261);
					attribute(0);
				}
				break;
				case 6:
					enterOuterAlt(_localctx, 6);
				{
					setState(263);
					function();
					setState(264);
					match(COMPARISON);
					setState(265);
					val();
				}
				break;
				case 7:
					enterOuterAlt(_localctx, 7);
				{
					setState(267);
					function();
					setState(268);
					match(T__25);
					setState(269);
					attribute(0);
				}
				break;
				case 8:
					enterOuterAlt(_localctx, 8);
				{
					setState(271);
					function();
					setState(272);
					match(T__25);
					setState(273);
					val();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public FunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitFunction(this);
		}
	}

	public final FunctionContext function() throws RecognitionException {
		FunctionContext _localctx = new FunctionContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_function);
		try {
			setState(289);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case T__26:
					enterOuterAlt(_localctx, 1);
				{
					setState(277);
					match(T__26);
					setState(278);
					match(NAME);
					setState(279);
					match(T__12);
				}
				break;
				case T__27:
					enterOuterAlt(_localctx, 2);
				{
					setState(280);
					match(T__27);
					setState(281);
					match(NAME);
				}
				break;
				case T__28:
					enterOuterAlt(_localctx, 3);
				{
					setState(282);
					match(T__28);
					setState(283);
					match(NAME);
					setState(284);
					match(T__12);
				}
				break;
				case T__29:
					enterOuterAlt(_localctx, 4);
				{
					setState(285);
					match(T__29);
				}
				break;
				case T__30:
					enterOuterAlt(_localctx, 5);
				{
					setState(286);
					match(T__30);
					setState(287);
					match(NAME);
					setState(288);
					match(T__12);
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ViewuseContext extends ParserRuleContext {
		public UsedviewsContext usedviews() {
			return getRuleContext(UsedviewsContext.class,0);
		}
		public ViewuseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_viewuse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterViewuse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitViewuse(this);
		}
	}

	public final ViewuseContext viewuse() throws RecognitionException {
		ViewuseContext _localctx = new ViewuseContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_viewuse);
		try {
			setState(294);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case T__31:
					enterOuterAlt(_localctx, 1);
				{
					setState(291);
					match(T__31);
					setState(292);
					usedviews();
				}
				break;
				case T__1:
				case T__2:
				case KEYWORD:
					enterOuterAlt(_localctx, 2);
				{
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UsedviewsContext extends ParserRuleContext {
		public List<TerminalNode> NAME() { return getTokens(ViewParser.NAME); }
		public TerminalNode NAME(int i) {
			return getToken(ViewParser.NAME, i);
		}
		public UsedviewsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_usedviews; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterUsedviews(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitUsedviews(this);
		}
	}

	public final UsedviewsContext usedviews() throws RecognitionException {
		UsedviewsContext _localctx = new UsedviewsContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_usedviews);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(299);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==NAME) {
					{
						{
							setState(296);
							match(NAME);
						}
					}
					setState(301);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ViewatomContext extends ParserRuleContext {
		public List<VariableContext> variable() {
			return getRuleContexts(VariableContext.class);
		}
		public VariableContext variable(int i) {
			return getRuleContext(VariableContext.class,i);
		}
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public List<ViewatomContext> viewatom() {
			return getRuleContexts(ViewatomContext.class);
		}
		public ViewatomContext viewatom(int i) {
			return getRuleContext(ViewatomContext.class,i);
		}
		public ViewatomContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_viewatom; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterViewatom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitViewatom(this);
		}
	}

	public final ViewatomContext viewatom() throws RecognitionException {
		return viewatom(0);
	}

	private ViewatomContext viewatom(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ViewatomContext _localctx = new ViewatomContext(_ctx, _parentState);
		ViewatomContext _prevctx = _localctx;
		int _startState = 32;
		enterRecursionRule(_localctx, 32, RULE_viewatom, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(352);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
					case 1:
					{
						setState(303);
						variable();
					}
					break;
					case 2:
					{
						setState(304);
						variable();
						setState(305);
						match(T__32);
						setState(306);
						relation();
						setState(307);
						match(T__33);
						setState(308);
						viewatom(8);
					}
					break;
					case 3:
					{
						setState(310);
						variable();
						setState(311);
						match(T__32);
						setState(312);
						relation();
						setState(313);
						match(T__34);
						setState(314);
						viewatom(7);
					}
					break;
					case 4:
					{
						setState(316);
						variable();
						setState(317);
						match(T__35);
						setState(318);
						relation();
						setState(319);
						match(T__33);
						setState(320);
						viewatom(6);
					}
					break;
					case 5:
					{
						setState(322);
						variable();
						setState(323);
						match(T__35);
						setState(324);
						relation();
						setState(325);
						match(T__34);
						setState(326);
						viewatom(5);
					}
					break;
					case 6:
					{
						setState(328);
						variable();
						setState(329);
						match(T__32);
						setState(330);
						relation();
						setState(331);
						match(T__33);
						setState(332);
						variable();
					}
					break;
					case 7:
					{
						setState(334);
						variable();
						setState(335);
						match(T__32);
						setState(336);
						relation();
						setState(337);
						match(T__34);
						setState(338);
						variable();
					}
					break;
					case 8:
					{
						setState(340);
						variable();
						setState(341);
						match(T__35);
						setState(342);
						relation();
						setState(343);
						match(T__33);
						setState(344);
						variable();
					}
					break;
					case 9:
					{
						setState(346);
						variable();
						setState(347);
						match(T__35);
						setState(348);
						relation();
						setState(349);
						match(T__34);
						setState(350);
						variable();
					}
					break;
				}
				_ctx.stop = _input.LT(-1);
				setState(404);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						if ( _parseListeners!=null ) triggerExitRuleEvent();
						_prevctx = _localctx;
						{
							setState(402);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
								case 1:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(354);
									if (!(precpred(_ctx, 16))) throw new FailedPredicateException(this, "precpred(_ctx, 16)");
									setState(355);
									match(T__32);
									setState(356);
									relation();
									setState(357);
									match(T__33);
									setState(358);
									viewatom(17);
								}
								break;
								case 2:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(360);
									if (!(precpred(_ctx, 15))) throw new FailedPredicateException(this, "precpred(_ctx, 15)");
									setState(361);
									match(T__32);
									setState(362);
									relation();
									setState(363);
									match(T__34);
									setState(364);
									viewatom(16);
								}
								break;
								case 3:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(366);
									if (!(precpred(_ctx, 14))) throw new FailedPredicateException(this, "precpred(_ctx, 14)");
									setState(367);
									match(T__35);
									setState(368);
									relation();
									setState(369);
									match(T__33);
									setState(370);
									viewatom(15);
								}
								break;
								case 4:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(372);
									if (!(precpred(_ctx, 13))) throw new FailedPredicateException(this, "precpred(_ctx, 13)");
									setState(373);
									match(T__35);
									setState(374);
									relation();
									setState(375);
									match(T__34);
									setState(376);
									viewatom(14);
								}
								break;
								case 5:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(378);
									if (!(precpred(_ctx, 12))) throw new FailedPredicateException(this, "precpred(_ctx, 12)");
									setState(379);
									match(T__32);
									setState(380);
									relation();
									setState(381);
									match(T__33);
									setState(382);
									variable();
								}
								break;
								case 6:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(384);
									if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
									setState(385);
									match(T__32);
									setState(386);
									relation();
									setState(387);
									match(T__34);
									setState(388);
									variable();
								}
								break;
								case 7:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(390);
									if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
									setState(391);
									match(T__35);
									setState(392);
									relation();
									setState(393);
									match(T__33);
									setState(394);
									variable();
								}
								break;
								case 8:
								{
									_localctx = new ViewatomContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_viewatom);
									setState(396);
									if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
									setState(397);
									match(T__35);
									setState(398);
									relation();
									setState(399);
									match(T__34);
									setState(400);
									variable();
								}
								break;
							}
						}
					}
					setState(406);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ReturnstmtContext extends ParserRuleContext {
		public TerminalNode RETURN() { return getToken(ViewParser.RETURN, 0); }
		public List<RetvalContext> retval() {
			return getRuleContexts(RetvalContext.class);
		}
		public RetvalContext retval(int i) {
			return getRuleContext(RetvalContext.class,i);
		}
		public ReturnstmtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_returnstmt; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterReturnstmt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitReturnstmt(this);
		}
	}

	public final ReturnstmtContext returnstmt() throws RecognitionException {
		ReturnstmtContext _localctx = new ReturnstmtContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_returnstmt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(407);
				match(RETURN);
				setState(408);
				retval();
				setState(413);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__8) {
					{
						{
							setState(409);
							match(T__8);
							setState(410);
							retval();
						}
					}
					setState(415);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RetvalContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public AttributeContext attribute() {
			return getRuleContext(AttributeContext.class,0);
		}
		public RetvalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_retval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterRetval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitRetval(this);
		}
	}

	public final RetvalContext retval() throws RecognitionException {
		RetvalContext _localctx = new RetvalContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_retval);
		try {
			setState(420);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case T__36:
					enterOuterAlt(_localctx, 1);
				{
					setState(416);
					match(T__36);
					setState(417);
					match(NAME);
					setState(418);
					match(T__12);
				}
				break;
				case CONSTANTS:
				case VALUE:
				case NAME:
					enterOuterAlt(_localctx, 2);
				{
					setState(419);
					attribute(0);
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExprContext extends ParserRuleContext {
		public ViewatomContext viewatom() {
			return getRuleContext(ViewatomContext.class,0);
		}
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitExpr(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		ExprContext _localctx = new ExprContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(422);
				viewatom(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VariableContext extends ParserRuleContext {
		public NodeNameContext nodeName() {
			return getRuleContext(NodeNameContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public VariableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitVariable(this);
		}
	}

	public final VariableContext variable() throws RecognitionException {
		VariableContext _localctx = new VariableContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_variable);
		try {
			setState(434);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(424);
					match(T__11);
					setState(425);
					nodeName();
					setState(426);
					match(T__12);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(428);
					match(T__11);
					setState(429);
					nodeName();
					setState(430);
					match(T__37);
					setState(431);
					type();
					setState(432);
					match(T__12);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TypeContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public TypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitType(this);
		}
	}

	public final TypeContext type() throws RecognitionException {
		TypeContext _localctx = new TypeContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(436);
				match(NAME);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeNameContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public NodeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterNodeName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitNodeName(this);
		}
	}

	public final NodeNameContext nodeName() throws RecognitionException {
		NodeNameContext _localctx = new NodeNameContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_nodeName);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(438);
				match(NAME);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationValueContext relationValue() {
			return getRuleContext(RelationValueContext.class,0);
		}
		public List<TypeContext> type() {
			return getRuleContexts(TypeContext.class);
		}
		public TypeContext type(int i) {
			return getRuleContext(TypeContext.class,i);
		}
		public RelationLengthContext relationLength() {
			return getRuleContext(RelationLengthContext.class,0);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitRelation(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_relation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(441);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NAME) {
					{
						setState(440);
						relationValue();
					}
				}

				setState(445);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__37) {
					{
						setState(443);
						match(T__37);
						setState(444);
						type();
					}
				}

				setState(451);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__13) {
					{
						{
							setState(447);
							match(T__13);
							setState(448);
							type();
						}
					}
					setState(453);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(455);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__38 || _la==T__40) {
					{
						setState(454);
						relationLength();
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationValueContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public RelationValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterRelationValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitRelationValue(this);
		}
	}

	public final RelationValueContext relationValue() throws RecognitionException {
		RelationValueContext _localctx = new RelationValueContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_relationValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(457);
				match(NAME);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationLengthContext extends ParserRuleContext {
		public List<TerminalNode> VALUE() { return getTokens(ViewParser.VALUE); }
		public TerminalNode VALUE(int i) {
			return getToken(ViewParser.VALUE, i);
		}
		public RelationLengthContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationLength; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterRelationLength(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitRelationLength(this);
		}
	}

	public final RelationLengthContext relationLength() throws RecognitionException {
		RelationLengthContext _localctx = new RelationLengthContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_relationLength);
		try {
			setState(471);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(459);
					match(T__38);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(460);
					match(T__38);
					setState(461);
					match(VALUE);
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(462);
					match(T__38);
					setState(463);
					match(VALUE);
					setState(464);
					match(T__39);
					setState(465);
					match(VALUE);
				}
				break;
				case 4:
					enterOuterAlt(_localctx, 4);
				{
					setState(466);
					match(T__40);
					setState(467);
					match(VALUE);
				}
				break;
				case 5:
					enterOuterAlt(_localctx, 5);
				{
					setState(468);
					match(T__38);
					setState(469);
					match(VALUE);
					setState(470);
					match(T__39);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PathContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitPath(this);
		}
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_path);
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(473);
				match(NAME);
				setState(474);
				match(T__25);
				setState(475);
				expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConditionsContext extends ParserRuleContext {
		public BoolexprContext boolexpr() {
			return getRuleContext(BoolexprContext.class,0);
		}
		public ConditionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conditions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterConditions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitConditions(this);
		}
	}

	public final ConditionsContext conditions() throws RecognitionException {
		ConditionsContext _localctx = new ConditionsContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_conditions);
		try {
			setState(480);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
				case T__24:
					enterOuterAlt(_localctx, 1);
				{
					setState(477);
					match(T__24);
					setState(478);
					boolexpr(0);
				}
				break;
				case T__3:
				case T__4:
				case T__5:
				case T__6:
				case T__7:
				case RETURN:
					enterOuterAlt(_localctx, 2);
				{
				}
				break;
				default:
					throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BoolexprContext extends ParserRuleContext {
		public List<AttributeContext> attribute() {
			return getRuleContexts(AttributeContext.class);
		}
		public AttributeContext attribute(int i) {
			return getRuleContext(AttributeContext.class,i);
		}
		public TerminalNode COMPARISON() { return getToken(ViewParser.COMPARISON, 0); }
		public ValContext val() {
			return getRuleContext(ValContext.class,0);
		}
		public TerminalNode VALUE() { return getToken(ViewParser.VALUE, 0); }
		public TerminalNode OPERATOR() { return getToken(ViewParser.OPERATOR, 0); }
		public List<TerminalNode> NAME() { return getTokens(ViewParser.NAME); }
		public TerminalNode NAME(int i) {
			return getToken(ViewParser.NAME, i);
		}
		public List<BoolexprContext> boolexpr() {
			return getRuleContexts(BoolexprContext.class);
		}
		public BoolexprContext boolexpr(int i) {
			return getRuleContext(BoolexprContext.class,i);
		}
		public ExistsContext exists() {
			return getRuleContext(ExistsContext.class,0);
		}
		public BoolexprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_boolexpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterBoolexpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitBoolexpr(this);
		}
	}

	public final BoolexprContext boolexpr() throws RecognitionException {
		return boolexpr(0);
	}

	private BoolexprContext boolexpr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BoolexprContext _localctx = new BoolexprContext(_ctx, _parentState);
		BoolexprContext _prevctx = _localctx;
		int _startState = 56;
		enterRecursionRule(_localctx, 56, RULE_boolexpr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(518);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
					case 1:
					{
						setState(483);
						attribute(0);
						setState(484);
						match(COMPARISON);
						setState(485);
						attribute(0);
					}
					break;
					case 2:
					{
						setState(487);
						attribute(0);
						setState(488);
						match(COMPARISON);
						setState(489);
						val();
					}
					break;
					case 3:
					{
						setState(491);
						attribute(0);
						setState(492);
						match(T__25);
						setState(493);
						attribute(0);
					}
					break;
					case 4:
					{
						setState(495);
						attribute(0);
						setState(496);
						match(T__25);
						setState(497);
						val();
					}
					break;
					case 5:
					{
						setState(499);
						match(VALUE);
						setState(500);
						match(OPERATOR);
						setState(501);
						attribute(0);
					}
					break;
					case 6:
					{
						setState(502);
						match(NAME);
						setState(503);
						match(T__10);
						setState(504);
						match(NAME);
					}
					break;
					case 7:
					{
						setState(505);
						match(T__11);
						setState(506);
						boolexpr(0);
						setState(507);
						match(T__12);
					}
					break;
					case 8:
					{
						setState(509);
						match(T__43);
						setState(510);
						boolexpr(3);
					}
					break;
					case 9:
					{
						setState(511);
						match(T__44);
						setState(512);
						boolexpr(2);
					}
					break;
					case 10:
					{
						setState(513);
						exists();
						setState(514);
						match(T__11);
						setState(515);
						attribute(0);
						setState(516);
						match(T__12);
					}
					break;
				}
				_ctx.stop = _input.LT(-1);
				setState(528);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,29,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						if ( _parseListeners!=null ) triggerExitRuleEvent();
						_prevctx = _localctx;
						{
							setState(526);
							_errHandler.sync(this);
							switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
								case 1:
								{
									_localctx = new BoolexprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_boolexpr);
									setState(520);
									if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
									setState(521);
									match(T__41);
									setState(522);
									boolexpr(9);
								}
								break;
								case 2:
								{
									_localctx = new BoolexprContext(_parentctx, _parentState);
									pushNewRecursionContext(_localctx, _startState, RULE_boolexpr);
									setState(523);
									if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
									setState(524);
									match(T__42);
									setState(525);
									boolexpr(8);
								}
								break;
							}
						}
					}
					setState(530);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,29,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class AttributeContext extends ParserRuleContext {
		public List<TerminalNode> NAME() { return getTokens(ViewParser.NAME); }
		public TerminalNode NAME(int i) {
			return getToken(ViewParser.NAME, i);
		}
		public ValContext val() {
			return getRuleContext(ValContext.class,0);
		}
		public ArithmeticContext arithmetic() {
			return getRuleContext(ArithmeticContext.class,0);
		}
		public AttributeContext attribute() {
			return getRuleContext(AttributeContext.class,0);
		}
		public IndexingContext indexing() {
			return getRuleContext(IndexingContext.class,0);
		}
		public AttributeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_attribute; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterAttribute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitAttribute(this);
		}
	}

	public final AttributeContext attribute() throws RecognitionException {
		return attribute(0);
	}

	private AttributeContext attribute(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		AttributeContext _localctx = new AttributeContext(_ctx, _parentState);
		AttributeContext _prevctx = _localctx;
		int _startState = 58;
		enterRecursionRule(_localctx, 58, RULE_attribute, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(542);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
					case 1:
					{
						setState(532);
						match(NAME);
						setState(535);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
							case 1:
							{
								setState(533);
								match(T__45);
								setState(534);
								match(NAME);
							}
							break;
						}
					}
					break;
					case 2:
					{
						setState(537);
						val();
						setState(538);
						arithmetic();
						setState(539);
						attribute(3);
					}
					break;
					case 3:
					{
						setState(541);
						indexing();
					}
					break;
				}
				_ctx.stop = _input.LT(-1);
				setState(550);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,32,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						if ( _parseListeners!=null ) triggerExitRuleEvent();
						_prevctx = _localctx;
						{
							{
								_localctx = new AttributeContext(_parentctx, _parentState);
								pushNewRecursionContext(_localctx, _startState, RULE_attribute);
								setState(544);
								if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
								setState(545);
								arithmetic();
								setState(546);
								val();
							}
						}
					}
					setState(552);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,32,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ValContext extends ParserRuleContext {
		public TerminalNode VALUE() { return getToken(ViewParser.VALUE, 0); }
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public TerminalNode CONSTANTS() { return getToken(ViewParser.CONSTANTS, 0); }
		public ValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_val; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitVal(this);
		}
	}

	public final ValContext val() throws RecognitionException {
		ValContext _localctx = new ValContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_val);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(553);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << CONSTANTS) | (1L << VALUE) | (1L << NAME))) != 0)) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IndexingContext extends ParserRuleContext {
		public List<TerminalNode> NAME() { return getTokens(ViewParser.NAME); }
		public TerminalNode NAME(int i) {
			return getToken(ViewParser.NAME, i);
		}
		public TerminalNode VALUE() { return getToken(ViewParser.VALUE, 0); }
		public IndexingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_indexing; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterIndexing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitIndexing(this);
		}
	}

	public final IndexingContext indexing() throws RecognitionException {
		IndexingContext _localctx = new IndexingContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_indexing);
		try {
			setState(563);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(555);
					match(NAME);
					setState(556);
					match(T__9);
					setState(557);
					match(VALUE);
					setState(558);
					match(T__14);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(559);
					match(NAME);
					setState(560);
					match(T__9);
					setState(561);
					match(NAME);
					setState(562);
					match(T__14);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetattrContext extends ParserRuleContext {
		public List<AttributeContext> attribute() {
			return getRuleContexts(AttributeContext.class);
		}
		public AttributeContext attribute(int i) {
			return getRuleContext(AttributeContext.class,i);
		}
		public ValContext val() {
			return getRuleContext(ValContext.class,0);
		}
		public SetattrContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setattr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterSetattr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitSetattr(this);
		}
	}

	public final SetattrContext setattr() throws RecognitionException {
		SetattrContext _localctx = new SetattrContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_setattr);
		try {
			setState(573);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(565);
					attribute(0);
					setState(566);
					match(T__25);
					setState(567);
					attribute(0);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(569);
					attribute(0);
					setState(570);
					match(T__25);
					setState(571);
					val();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticContext extends ParserRuleContext {
		public ArithmeticContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmetic; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterArithmetic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitArithmetic(this);
		}
	}

	public final ArithmeticContext arithmetic() throws RecognitionException {
		ArithmeticContext _localctx = new ArithmeticContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_arithmetic);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(575);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__38) | (1L << T__46) | (1L << T__47) | (1L << T__48))) != 0)) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertionContext extends ParserRuleContext {
		public InsertionVarContext insertionVar() {
			return getRuleContext(InsertionVarContext.class,0);
		}
		public List<InsertionContext> insertion() {
			return getRuleContexts(InsertionContext.class);
		}
		public InsertionContext insertion(int i) {
			return getRuleContext(InsertionContext.class,i);
		}
		public InsertrelationContext insertrelation() {
			return getRuleContext(InsertrelationContext.class,0);
		}
		public InsertionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertion; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterInsertion(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitInsertion(this);
		}
	}

	public final InsertionContext insertion() throws RecognitionException {
		return insertion(0);
	}

	private InsertionContext insertion(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		InsertionContext _localctx = new InsertionContext(_ctx, _parentState);
		InsertionContext _prevctx = _localctx;
		int _startState = 68;
		enterRecursionRule(_localctx, 68, RULE_insertion, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				{
					setState(578);
					insertionVar();
				}
				_ctx.stop = _input.LT(-1);
				setState(588);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,35,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						if ( _parseListeners!=null ) triggerExitRuleEvent();
						_prevctx = _localctx;
						{
							{
								_localctx = new InsertionContext(_parentctx, _parentState);
								pushNewRecursionContext(_localctx, _startState, RULE_insertion);
								setState(580);
								if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
								setState(581);
								match(T__32);
								setState(582);
								insertrelation();
								setState(583);
								match(T__33);
								setState(584);
								insertion(2);
							}
						}
					}
					setState(590);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,35,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class InsertrelationContext extends ParserRuleContext {
		public RelationValueContext relationValue() {
			return getRuleContext(RelationValueContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public InsertrelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertrelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterInsertrelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitInsertrelation(this);
		}
	}

	public final InsertrelationContext insertrelation() throws RecognitionException {
		InsertrelationContext _localctx = new InsertrelationContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_insertrelation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(592);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NAME) {
					{
						setState(591);
						relationValue();
					}
				}

				setState(596);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__37) {
					{
						setState(594);
						match(T__37);
						setState(595);
						type();
					}
				}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertionVarContext extends ParserRuleContext {
		public NodeNameContext nodeName() {
			return getRuleContext(NodeNameContext.class,0);
		}
		public TypeContext type() {
			return getRuleContext(TypeContext.class,0);
		}
		public InsertAttributesContext insertAttributes() {
			return getRuleContext(InsertAttributesContext.class,0);
		}
		public InsertionVarContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertionVar; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterInsertionVar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitInsertionVar(this);
		}
	}

	public final InsertionVarContext insertionVar() throws RecognitionException {
		InsertionVarContext _localctx = new InsertionVarContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_insertionVar);
		int _la;
		try {
			setState(620);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
				case 1:
					enterOuterAlt(_localctx, 1);
				{
					setState(598);
					match(T__11);
					setState(599);
					nodeName();
					setState(600);
					match(T__12);
				}
				break;
				case 2:
					enterOuterAlt(_localctx, 2);
				{
					setState(602);
					match(T__11);
					setState(603);
					nodeName();
					setState(604);
					match(T__37);
					setState(605);
					type();
					setState(606);
					match(T__12);
				}
				break;
				case 3:
					enterOuterAlt(_localctx, 3);
				{
					setState(608);
					match(T__11);
					setState(609);
					nodeName();
					setState(610);
					match(T__37);
					setState(611);
					type();
					setState(616);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__49) {
						{
							setState(612);
							match(T__49);
							setState(613);
							insertAttributes();
							setState(614);
							match(T__50);
						}
					}

					setState(618);
					match(T__12);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertAttributesContext extends ParserRuleContext {
		public TerminalNode NAME() { return getToken(ViewParser.NAME, 0); }
		public ValContext val() {
			return getRuleContext(ValContext.class,0);
		}
		public List<InsertAttributesContext> insertAttributes() {
			return getRuleContexts(InsertAttributesContext.class);
		}
		public InsertAttributesContext insertAttributes(int i) {
			return getRuleContext(InsertAttributesContext.class,i);
		}
		public InsertAttributesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertAttributes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterInsertAttributes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitInsertAttributes(this);
		}
	}

	public final InsertAttributesContext insertAttributes() throws RecognitionException {
		InsertAttributesContext _localctx = new InsertAttributesContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_insertAttributes);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
				setState(622);
				match(NAME);
				setState(623);
				match(T__37);
				setState(624);
				val();
				setState(629);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,40,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
							{
								setState(625);
								match(T__8);
								setState(626);
								insertAttributes();
							}
						}
					}
					setState(631);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,40,_ctx);
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AsContext extends ParserRuleContext {
		public AsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_as; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterAs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitAs(this);
		}
	}

	public final AsContext as() throws RecognitionException {
		AsContext _localctx = new AsContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_as);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(632);
				_la = _input.LA(1);
				if ( !(_la==T__51 || _la==T__52) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExistsContext extends ParserRuleContext {
		public ExistsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exists; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ViewListener ) ((ViewListener)listener).exitExists(this);
		}
	}

	public final ExistsContext exists() throws RecognitionException {
		ExistsContext _localctx = new ExistsContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_exists);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
				setState(634);
				_la = _input.LA(1);
				if ( !(_la==T__53 || _la==T__54) ) {
					_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
			case 7:
				return validVal_sempred((ValidValContext)_localctx, predIndex);
			case 16:
				return viewatom_sempred((ViewatomContext)_localctx, predIndex);
			case 28:
				return boolexpr_sempred((BoolexprContext)_localctx, predIndex);
			case 29:
				return attribute_sempred((AttributeContext)_localctx, predIndex);
			case 34:
				return insertion_sempred((InsertionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean validVal_sempred(ValidValContext _localctx, int predIndex) {
		switch (predIndex) {
			case 0:
				return precpred(_ctx, 4);
		}
		return true;
	}
	private boolean viewatom_sempred(ViewatomContext _localctx, int predIndex) {
		switch (predIndex) {
			case 1:
				return precpred(_ctx, 16);
			case 2:
				return precpred(_ctx, 15);
			case 3:
				return precpred(_ctx, 14);
			case 4:
				return precpred(_ctx, 13);
			case 5:
				return precpred(_ctx, 12);
			case 6:
				return precpred(_ctx, 11);
			case 7:
				return precpred(_ctx, 10);
			case 8:
				return precpred(_ctx, 9);
		}
		return true;
	}
	private boolean boolexpr_sempred(BoolexprContext _localctx, int predIndex) {
		switch (predIndex) {
			case 9:
				return precpred(_ctx, 8);
			case 10:
				return precpred(_ctx, 7);
		}
		return true;
	}
	private boolean attribute_sempred(AttributeContext _localctx, int predIndex) {
		switch (predIndex) {
			case 11:
				return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean insertion_sempred(InsertionContext _localctx, int predIndex) {
		switch (predIndex) {
			case 12:
				return precpred(_ctx, 1);
		}
		return true;
	}

	public static final String _serializedATN =
			"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3C\u027f\4\2\t\2\4"+
					"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
					"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
					"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
					"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
					"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\3\2\3\2\3\2\3"+
					"\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2`\n\2\3\3\3\3\3\3\5\3e\n\3\3"+
					"\4\3\4\3\4\3\4\5\4k\n\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4s\n\4\3\4\3\4\5\4w"+
					"\n\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
					"\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5\u0093\n\5\3\6\3\6\3\6\3\6\3"+
					"\6\3\6\3\6\3\6\3\6\5\6\u009e\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3"+
					"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
					"\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u00c2\n\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b"+
					"\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u00da"+
					"\n\t\3\t\3\t\3\t\3\t\7\t\u00e0\n\t\f\t\16\t\u00e3\13\t\3\n\3\n\3\n\3\n"+
					"\3\n\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\r\5\r\u00f4\n\r\3\16\3\16"+
					"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16"+
					"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16"+
					"\3\16\3\16\5\16\u0116\n\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
					"\3\17\3\17\3\17\5\17\u0124\n\17\3\20\3\20\3\20\5\20\u0129\n\20\3\21\7"+
					"\21\u012c\n\21\f\21\16\21\u012f\13\21\3\22\3\22\3\22\3\22\3\22\3\22\3"+
					"\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3"+
					"\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3"+
					"\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3"+
					"\22\3\22\5\22\u0163\n\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
					"\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
					"\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
					"\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\7\22\u0195\n\22"+
					"\f\22\16\22\u0198\13\22\3\23\3\23\3\23\3\23\7\23\u019e\n\23\f\23\16\23"+
					"\u01a1\13\23\3\24\3\24\3\24\3\24\5\24\u01a7\n\24\3\25\3\25\3\26\3\26\3"+
					"\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\5\26\u01b5\n\26\3\27\3\27\3\30"+
					"\3\30\3\31\5\31\u01bc\n\31\3\31\3\31\5\31\u01c0\n\31\3\31\3\31\7\31\u01c4"+
					"\n\31\f\31\16\31\u01c7\13\31\3\31\5\31\u01ca\n\31\3\32\3\32\3\33\3\33"+
					"\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\5\33\u01da\n\33\3\34"+
					"\3\34\3\34\3\34\3\35\3\35\3\35\5\35\u01e3\n\35\3\36\3\36\3\36\3\36\3\36"+
					"\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36"+
					"\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36"+
					"\3\36\3\36\3\36\5\36\u0209\n\36\3\36\3\36\3\36\3\36\3\36\3\36\7\36\u0211"+
					"\n\36\f\36\16\36\u0214\13\36\3\37\3\37\3\37\3\37\5\37\u021a\n\37\3\37"+
					"\3\37\3\37\3\37\3\37\5\37\u0221\n\37\3\37\3\37\3\37\3\37\7\37\u0227\n"+
					"\37\f\37\16\37\u022a\13\37\3 \3 \3!\3!\3!\3!\3!\3!\3!\3!\5!\u0236\n!\3"+
					"\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u0240\n\"\3#\3#\3$\3$\3$\3$\3$\3$\3"+
					"$\3$\3$\7$\u024d\n$\f$\16$\u0250\13$\3%\5%\u0253\n%\3%\3%\5%\u0257\n%"+
					"\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\3&\5&\u026b\n&\3&"+
					"\3&\5&\u026f\n&\3\'\3\'\3\'\3\'\3\'\7\'\u0276\n\'\f\'\16\'\u0279\13\'"+
					"\3(\3(\3)\3)\3)\2\7\20\":<F*\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \""+
					"$&(*,.\60\62\64\668:<>@BDFHJLNP\2\b\3\2\27\30\3\2\31\32\3\2?A\4\2))\61"+
					"\63\3\2\66\67\3\289\2\u02b0\2_\3\2\2\2\4d\3\2\2\2\6v\3\2\2\2\b\u0092\3"+
					"\2\2\2\n\u009d\3\2\2\2\f\u00c1\3\2\2\2\16\u00c3\3\2\2\2\20\u00d9\3\2\2"+
					"\2\22\u00e4\3\2\2\2\24\u00ec\3\2\2\2\26\u00ee\3\2\2\2\30\u00f3\3\2\2\2"+
					"\32\u0115\3\2\2\2\34\u0123\3\2\2\2\36\u0128\3\2\2\2 \u012d\3\2\2\2\"\u0162"+
					"\3\2\2\2$\u0199\3\2\2\2&\u01a6\3\2\2\2(\u01a8\3\2\2\2*\u01b4\3\2\2\2,"+
					"\u01b6\3\2\2\2.\u01b8\3\2\2\2\60\u01bb\3\2\2\2\62\u01cb\3\2\2\2\64\u01d9"+
					"\3\2\2\2\66\u01db\3\2\2\28\u01e2\3\2\2\2:\u0208\3\2\2\2<\u0220\3\2\2\2"+
					">\u022b\3\2\2\2@\u0235\3\2\2\2B\u023f\3\2\2\2D\u0241\3\2\2\2F\u0243\3"+
					"\2\2\2H\u0252\3\2\2\2J\u026e\3\2\2\2L\u0270\3\2\2\2N\u027a\3\2\2\2P\u027c"+
					"\3\2\2\2RS\7<\2\2ST\7A\2\2TU\5\36\20\2UV\5\4\3\2VW\5\6\4\2W`\3\2\2\2X"+
					"Y\7\3\2\2Y`\5\6\4\2Z`\5\b\5\2[\\\5\36\20\2\\]\5\4\3\2]^\5\6\4\2^`\3\2"+
					"\2\2_R\3\2\2\2_X\3\2\2\2_Z\3\2\2\2_[\3\2\2\2`\3\3\2\2\2ae\7\4\2\2be\7"+
					"\5\2\2ce\3\2\2\2da\3\2\2\2db\3\2\2\2dc\3\2\2\2e\5\3\2\2\2fg\7:\2\2gh\5"+
					"(\25\2hj\58\35\2ik\5\n\6\2ji\3\2\2\2jk\3\2\2\2kl\3\2\2\2lm\5$\23\2mw\3"+
					"\2\2\2no\7:\2\2op\5\66\34\2pr\58\35\2qs\5\n\6\2rq\3\2\2\2rs\3\2\2\2st"+
					"\3\2\2\2tu\5$\23\2uw\3\2\2\2vf\3\2\2\2vn\3\2\2\2w\7\3\2\2\2xy\7:\2\2y"+
					"z\5(\25\2z{\58\35\2{|\7\6\2\2|}\5B\"\2}\u0093\3\2\2\2~\177\7:\2\2\177"+
					"\u0080\5(\25\2\u0080\u0081\58\35\2\u0081\u0082\7\7\2\2\u0082\u0083\7A"+
					"\2\2\u0083\u0093\3\2\2\2\u0084\u0085\7:\2\2\u0085\u0086\5(\25\2\u0086"+
					"\u0087\58\35\2\u0087\u0088\7\b\2\2\u0088\u0089\5<\37\2\u0089\u0093\3\2"+
					"\2\2\u008a\u008b\7:\2\2\u008b\u008c\5(\25\2\u008c\u008d\58\35\2\u008d"+
					"\u008e\7\t\2\2\u008e\u008f\5F$\2\u008f\u0093\3\2\2\2\u0090\u0091\7\t\2"+
					"\2\u0091\u0093\5F$\2\u0092x\3\2\2\2\u0092~\3\2\2\2\u0092\u0084\3\2\2\2"+
					"\u0092\u008a\3\2\2\2\u0092\u0090\3\2\2\2\u0093\t\3\2\2\2\u0094\u0095\7"+
					"\n\2\2\u0095\u0096\5\f\7\2\u0096\u0097\5\30\r\2\u0097\u009e\3\2\2\2\u0098"+
					"\u0099\7\n\2\2\u0099\u009a\5\f\7\2\u009a\u009b\5\30\r\2\u009b\u009c\5"+
					"\n\6\2\u009c\u009e\3\2\2\2\u009d\u0094\3\2\2\2\u009d\u0098\3\2\2\2\u009e"+
					"\13\3\2\2\2\u009f\u00a0\7A\2\2\u00a0\u00a1\5N(\2\u00a1\u00a2\7A\2\2\u00a2"+
					"\u00a3\7\13\2\2\u00a3\u00a4\5\f\7\2\u00a4\u00c2\3\2\2\2\u00a5\u00a6\5"+
					"\34\17\2\u00a6\u00a7\5N(\2\u00a7\u00a8\7A\2\2\u00a8\u00a9\7\13\2\2\u00a9"+
					"\u00aa\5\f\7\2\u00aa\u00c2\3\2\2\2\u00ab\u00ac\7A\2\2\u00ac\u00ad\7\13"+
					"\2\2\u00ad\u00c2\5\f\7\2\u00ae\u00af\7A\2\2\u00af\u00b0\5N(\2\u00b0\u00b1"+
					"\7A\2\2\u00b1\u00c2\3\2\2\2\u00b2\u00b3\5\34\17\2\u00b3\u00b4\5N(\2\u00b4"+
					"\u00b5\7A\2\2\u00b5\u00c2\3\2\2\2\u00b6\u00c2\7A\2\2\u00b7\u00b8\5\16"+
					"\b\2\u00b8\u00b9\5N(\2\u00b9\u00ba\7A\2\2\u00ba\u00bb\7\13\2\2\u00bb\u00bc"+
					"\5\f\7\2\u00bc\u00c2\3\2\2\2\u00bd\u00be\5\16\b\2\u00be\u00bf\5N(\2\u00bf"+
					"\u00c0\7A\2\2\u00c0\u00c2\3\2\2\2\u00c1\u009f\3\2\2\2\u00c1\u00a5\3\2"+
					"\2\2\u00c1\u00ab\3\2\2\2\u00c1\u00ae\3\2\2\2\u00c1\u00b2\3\2\2\2\u00c1"+
					"\u00b6\3\2\2\2\u00c1\u00b7\3\2\2\2\u00c1\u00bd\3\2\2\2\u00c2\r\3\2\2\2"+
					"\u00c3\u00c4\7\f\2\2\u00c4\u00c5\7A\2\2\u00c5\u00c6\7\r\2\2\u00c6\u00c7"+
					"\5\26\f\2\u00c7\u00c8\7\16\2\2\u00c8\u00c9\5\20\t\2\u00c9\u00ca\7\13\2"+
					"\2\u00ca\u00cb\5\20\t\2\u00cb\u00cc\7\17\2\2\u00cc\u00cd\7\20\2\2\u00cd"+
					"\u00ce\5\22\n\2\u00ce\u00cf\7\21\2\2\u00cf\17\3\2\2\2\u00d0\u00d1\b\t"+
					"\1\2\u00d1\u00d2\5\24\13\2\u00d2\u00d3\7\16\2\2\u00d3\u00d4\7A\2\2\u00d4"+
					"\u00d5\7\17\2\2\u00d5\u00da\3\2\2\2\u00d6\u00da\7@\2\2\u00d7\u00da\7\22"+
					"\2\2\u00d8\u00da\5@!\2\u00d9\u00d0\3\2\2\2\u00d9\u00d6\3\2\2\2\u00d9\u00d7"+
					"\3\2\2\2\u00d9\u00d8\3\2\2\2\u00da\u00e1\3\2\2\2\u00db\u00dc\f\6\2\2\u00dc"+
					"\u00dd\5D#\2\u00dd\u00de\5\20\t\7\u00de\u00e0\3\2\2\2\u00df\u00db\3\2"+
					"\2\2\u00e0\u00e3\3\2\2\2\u00e1\u00df\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e2"+
					"\21\3\2\2\2\u00e3\u00e1\3\2\2\2\u00e4\u00e5\7\23\2\2\u00e5\u00e6\5\32"+
					"\16\2\u00e6\u00e7\7\24\2\2\u00e7\u00e8\5\20\t\2\u00e8\u00e9\7\25\2\2\u00e9"+
					"\u00ea\5\20\t\2\u00ea\u00eb\7\26\2\2\u00eb\23\3\2\2\2\u00ec\u00ed\t\2"+
					"\2\2\u00ed\25\3\2\2\2\u00ee\u00ef\t\3\2\2\u00ef\27\3\2\2\2\u00f0\u00f1"+
					"\7\33\2\2\u00f1\u00f4\5\32\16\2\u00f2\u00f4\3\2\2\2\u00f3\u00f0\3\2\2"+
					"\2\u00f3\u00f2\3\2\2\2\u00f4\31\3\2\2\2\u00f5\u00f6\5<\37\2\u00f6\u00f7"+
					"\7=\2\2\u00f7\u00f8\5<\37\2\u00f8\u0116\3\2\2\2\u00f9\u00fa\5<\37\2\u00fa"+
					"\u00fb\7=\2\2\u00fb\u00fc\5> \2\u00fc\u0116\3\2\2\2\u00fd\u00fe\5<\37"+
					"\2\u00fe\u00ff\7\34\2\2\u00ff\u0100\5<\37\2\u0100\u0116\3\2\2\2\u0101"+
					"\u0102\5<\37\2\u0102\u0103\7\34\2\2\u0103\u0104\5> \2\u0104\u0116\3\2"+
					"\2\2\u0105\u0106\5\34\17\2\u0106\u0107\7=\2\2\u0107\u0108\5<\37\2\u0108"+
					"\u0116\3\2\2\2\u0109\u010a\5\34\17\2\u010a\u010b\7=\2\2\u010b\u010c\5"+
					"> \2\u010c\u0116\3\2\2\2\u010d\u010e\5\34\17\2\u010e\u010f\7\34\2\2\u010f"+
					"\u0110\5<\37\2\u0110\u0116\3\2\2\2\u0111\u0112\5\34\17\2\u0112\u0113\7"+
					"\34\2\2\u0113\u0114\5> \2\u0114\u0116\3\2\2\2\u0115\u00f5\3\2\2\2\u0115"+
					"\u00f9\3\2\2\2\u0115\u00fd\3\2\2\2\u0115\u0101\3\2\2\2\u0115\u0105\3\2"+
					"\2\2\u0115\u0109\3\2\2\2\u0115\u010d\3\2\2\2\u0115\u0111\3\2\2\2\u0116"+
					"\33\3\2\2\2\u0117\u0118\7\35\2\2\u0118\u0119\7A\2\2\u0119\u0124\7\17\2"+
					"\2\u011a\u011b\7\36\2\2\u011b\u0124\7A\2\2\u011c\u011d\7\37\2\2\u011d"+
					"\u011e\7A\2\2\u011e\u0124\7\17\2\2\u011f\u0124\7 \2\2\u0120\u0121\7!\2"+
					"\2\u0121\u0122\7A\2\2\u0122\u0124\7\17\2\2\u0123\u0117\3\2\2\2\u0123\u011a"+
					"\3\2\2\2\u0123\u011c\3\2\2\2\u0123\u011f\3\2\2\2\u0123\u0120\3\2\2\2\u0124"+
					"\35\3\2\2\2\u0125\u0126\7\"\2\2\u0126\u0129\5 \21\2\u0127\u0129\3\2\2"+
					"\2\u0128\u0125\3\2\2\2\u0128\u0127\3\2\2\2\u0129\37\3\2\2\2\u012a\u012c"+
					"\7A\2\2\u012b\u012a\3\2\2\2\u012c\u012f\3\2\2\2\u012d\u012b\3\2\2\2\u012d"+
					"\u012e\3\2\2\2\u012e!\3\2\2\2\u012f\u012d\3\2\2\2\u0130\u0131\b\22\1\2"+
					"\u0131\u0163\5*\26\2\u0132\u0133\5*\26\2\u0133\u0134\7#\2\2\u0134\u0135"+
					"\5\60\31\2\u0135\u0136\7$\2\2\u0136\u0137\5\"\22\n\u0137\u0163\3\2\2\2"+
					"\u0138\u0139\5*\26\2\u0139\u013a\7#\2\2\u013a\u013b\5\60\31\2\u013b\u013c"+
					"\7%\2\2\u013c\u013d\5\"\22\t\u013d\u0163\3\2\2\2\u013e\u013f\5*\26\2\u013f"+
					"\u0140\7&\2\2\u0140\u0141\5\60\31\2\u0141\u0142\7$\2\2\u0142\u0143\5\""+
					"\22\b\u0143\u0163\3\2\2\2\u0144\u0145\5*\26\2\u0145\u0146\7&\2\2\u0146"+
					"\u0147\5\60\31\2\u0147\u0148\7%\2\2\u0148\u0149\5\"\22\7\u0149\u0163\3"+
					"\2\2\2\u014a\u014b\5*\26\2\u014b\u014c\7#\2\2\u014c\u014d\5\60\31\2\u014d"+
					"\u014e\7$\2\2\u014e\u014f\5*\26\2\u014f\u0163\3\2\2\2\u0150\u0151\5*\26"+
					"\2\u0151\u0152\7#\2\2\u0152\u0153\5\60\31\2\u0153\u0154\7%\2\2\u0154\u0155"+
					"\5*\26\2\u0155\u0163\3\2\2\2\u0156\u0157\5*\26\2\u0157\u0158\7&\2\2\u0158"+
					"\u0159\5\60\31\2\u0159\u015a\7$\2\2\u015a\u015b\5*\26\2\u015b\u0163\3"+
					"\2\2\2\u015c\u015d\5*\26\2\u015d\u015e\7&\2\2\u015e\u015f\5\60\31\2\u015f"+
					"\u0160\7%\2\2\u0160\u0161\5*\26\2\u0161\u0163\3\2\2\2\u0162\u0130\3\2"+
					"\2\2\u0162\u0132\3\2\2\2\u0162\u0138\3\2\2\2\u0162\u013e\3\2\2\2\u0162"+
					"\u0144\3\2\2\2\u0162\u014a\3\2\2\2\u0162\u0150\3\2\2\2\u0162\u0156\3\2"+
					"\2\2\u0162\u015c\3\2\2\2\u0163\u0196\3\2\2\2\u0164\u0165\f\22\2\2\u0165"+
					"\u0166\7#\2\2\u0166\u0167\5\60\31\2\u0167\u0168\7$\2\2\u0168\u0169\5\""+
					"\22\23\u0169\u0195\3\2\2\2\u016a\u016b\f\21\2\2\u016b\u016c\7#\2\2\u016c"+
					"\u016d\5\60\31\2\u016d\u016e\7%\2\2\u016e\u016f\5\"\22\22\u016f\u0195"+
					"\3\2\2\2\u0170\u0171\f\20\2\2\u0171\u0172\7&\2\2\u0172\u0173\5\60\31\2"+
					"\u0173\u0174\7$\2\2\u0174\u0175\5\"\22\21\u0175\u0195\3\2\2\2\u0176\u0177"+
					"\f\17\2\2\u0177\u0178\7&\2\2\u0178\u0179\5\60\31\2\u0179\u017a\7%\2\2"+
					"\u017a\u017b\5\"\22\20\u017b\u0195\3\2\2\2\u017c\u017d\f\16\2\2\u017d"+
					"\u017e\7#\2\2\u017e\u017f\5\60\31\2\u017f\u0180\7$\2\2\u0180\u0181\5*"+
					"\26\2\u0181\u0195\3\2\2\2\u0182\u0183\f\r\2\2\u0183\u0184\7#\2\2\u0184"+
					"\u0185\5\60\31\2\u0185\u0186\7%\2\2\u0186\u0187\5*\26\2\u0187\u0195\3"+
					"\2\2\2\u0188\u0189\f\f\2\2\u0189\u018a\7&\2\2\u018a\u018b\5\60\31\2\u018b"+
					"\u018c\7$\2\2\u018c\u018d\5*\26\2\u018d\u0195\3\2\2\2\u018e\u018f\f\13"+
					"\2\2\u018f\u0190\7&\2\2\u0190\u0191\5\60\31\2\u0191\u0192\7%\2\2\u0192"+
					"\u0193\5*\26\2\u0193\u0195\3\2\2\2\u0194\u0164\3\2\2\2\u0194\u016a\3\2"+
					"\2\2\u0194\u0170\3\2\2\2\u0194\u0176\3\2\2\2\u0194\u017c\3\2\2\2\u0194"+
					"\u0182\3\2\2\2\u0194\u0188\3\2\2\2\u0194\u018e\3\2\2\2\u0195\u0198\3\2"+
					"\2\2\u0196\u0194\3\2\2\2\u0196\u0197\3\2\2\2\u0197#\3\2\2\2\u0198\u0196"+
					"\3\2\2\2\u0199\u019a\7;\2\2\u019a\u019f\5&\24\2\u019b\u019c\7\13\2\2\u019c"+
					"\u019e\5&\24\2\u019d\u019b\3\2\2\2\u019e\u01a1\3\2\2\2\u019f\u019d\3\2"+
					"\2\2\u019f\u01a0\3\2\2\2\u01a0%\3\2\2\2\u01a1\u019f\3\2\2\2\u01a2\u01a3"+
					"\7\'\2\2\u01a3\u01a4\7A\2\2\u01a4\u01a7\7\17\2\2\u01a5\u01a7\5<\37\2\u01a6"+
					"\u01a2\3\2\2\2\u01a6\u01a5\3\2\2\2\u01a7\'\3\2\2\2\u01a8\u01a9\5\"\22"+
					"\2\u01a9)\3\2\2\2\u01aa\u01ab\7\16\2\2\u01ab\u01ac\5.\30\2\u01ac\u01ad"+
					"\7\17\2\2\u01ad\u01b5\3\2\2\2\u01ae\u01af\7\16\2\2\u01af\u01b0\5.\30\2"+
					"\u01b0\u01b1\7(\2\2\u01b1\u01b2\5,\27\2\u01b2\u01b3\7\17\2\2\u01b3\u01b5"+
					"\3\2\2\2\u01b4\u01aa\3\2\2\2\u01b4\u01ae\3\2\2\2\u01b5+\3\2\2\2\u01b6"+
					"\u01b7\7A\2\2\u01b7-\3\2\2\2\u01b8\u01b9\7A\2\2\u01b9/\3\2\2\2\u01ba\u01bc"+
					"\5\62\32\2\u01bb\u01ba\3\2\2\2\u01bb\u01bc\3\2\2\2\u01bc\u01bf\3\2\2\2"+
					"\u01bd\u01be\7(\2\2\u01be\u01c0\5,\27\2\u01bf\u01bd\3\2\2\2\u01bf\u01c0"+
					"\3\2\2\2\u01c0\u01c5\3\2\2\2\u01c1\u01c2\7\20\2\2\u01c2\u01c4\5,\27\2"+
					"\u01c3\u01c1\3\2\2\2\u01c4\u01c7\3\2\2\2\u01c5\u01c3\3\2\2\2\u01c5\u01c6"+
					"\3\2\2\2\u01c6\u01c9\3\2\2\2\u01c7\u01c5\3\2\2\2\u01c8\u01ca\5\64\33\2"+
					"\u01c9\u01c8\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca\61\3\2\2\2\u01cb\u01cc"+
					"\7A\2\2\u01cc\63\3\2\2\2\u01cd\u01da\7)\2\2\u01ce\u01cf\7)\2\2\u01cf\u01da"+
					"\7@\2\2\u01d0\u01d1\7)\2\2\u01d1\u01d2\7@\2\2\u01d2\u01d3\7*\2\2\u01d3"+
					"\u01da\7@\2\2\u01d4\u01d5\7+\2\2\u01d5\u01da\7@\2\2\u01d6\u01d7\7)\2\2"+
					"\u01d7\u01d8\7@\2\2\u01d8\u01da\7*\2\2\u01d9\u01cd\3\2\2\2\u01d9\u01ce"+
					"\3\2\2\2\u01d9\u01d0\3\2\2\2\u01d9\u01d4\3\2\2\2\u01d9\u01d6\3\2\2\2\u01da"+
					"\65\3\2\2\2\u01db\u01dc\7A\2\2\u01dc\u01dd\7\34\2\2\u01dd\u01de\5(\25"+
					"\2\u01de\67\3\2\2\2\u01df\u01e0\7\33\2\2\u01e0\u01e3\5:\36\2\u01e1\u01e3"+
					"\3\2\2\2\u01e2\u01df\3\2\2\2\u01e2\u01e1\3\2\2\2\u01e39\3\2\2\2\u01e4"+
					"\u01e5\b\36\1\2\u01e5\u01e6\5<\37\2\u01e6\u01e7\7=\2\2\u01e7\u01e8\5<"+
					"\37\2\u01e8\u0209\3\2\2\2\u01e9\u01ea\5<\37\2\u01ea\u01eb\7=\2\2\u01eb"+
					"\u01ec\5> \2\u01ec\u0209\3\2\2\2\u01ed\u01ee\5<\37\2\u01ee\u01ef\7\34"+
					"\2\2\u01ef\u01f0\5<\37\2\u01f0\u0209\3\2\2\2\u01f1\u01f2\5<\37\2\u01f2"+
					"\u01f3\7\34\2\2\u01f3\u01f4\5> \2\u01f4\u0209\3\2\2\2\u01f5\u01f6\7@\2"+
					"\2\u01f6\u01f7\7>\2\2\u01f7\u0209\5<\37\2\u01f8\u01f9\7A\2\2\u01f9\u01fa"+
					"\7\r\2\2\u01fa\u0209\7A\2\2\u01fb\u01fc\7\16\2\2\u01fc\u01fd\5:\36\2\u01fd"+
					"\u01fe\7\17\2\2\u01fe\u0209\3\2\2\2\u01ff\u0200\7.\2\2\u0200\u0209\5:"+
					"\36\5\u0201\u0202\7/\2\2\u0202\u0209\5:\36\4\u0203\u0204\5P)\2\u0204\u0205"+
					"\7\16\2\2\u0205\u0206\5<\37\2\u0206\u0207\7\17\2\2\u0207\u0209\3\2\2\2"+
					"\u0208\u01e4\3\2\2\2\u0208\u01e9\3\2\2\2\u0208\u01ed\3\2\2\2\u0208\u01f1"+
					"\3\2\2\2\u0208\u01f5\3\2\2\2\u0208\u01f8\3\2\2\2\u0208\u01fb\3\2\2\2\u0208"+
					"\u01ff\3\2\2\2\u0208\u0201\3\2\2\2\u0208\u0203\3\2\2\2\u0209\u0212\3\2"+
					"\2\2\u020a\u020b\f\n\2\2\u020b\u020c\7,\2\2\u020c\u0211\5:\36\13\u020d"+
					"\u020e\f\t\2\2\u020e\u020f\7-\2\2\u020f\u0211\5:\36\n\u0210\u020a\3\2"+
					"\2\2\u0210\u020d\3\2\2\2\u0211\u0214\3\2\2\2\u0212\u0210\3\2\2\2\u0212"+
					"\u0213\3\2\2\2\u0213;\3\2\2\2\u0214\u0212\3\2\2\2\u0215\u0216\b\37\1\2"+
					"\u0216\u0219\7A\2\2\u0217\u0218\7\60\2\2\u0218\u021a\7A\2\2\u0219\u0217"+
					"\3\2\2\2\u0219\u021a\3\2\2\2\u021a\u0221\3\2\2\2\u021b\u021c\5> \2\u021c"+
					"\u021d\5D#\2\u021d\u021e\5<\37\5\u021e\u0221\3\2\2\2\u021f\u0221\5@!\2"+
					"\u0220\u0215\3\2\2\2\u0220\u021b\3\2\2\2\u0220\u021f\3\2\2\2\u0221\u0228"+
					"\3\2\2\2\u0222\u0223\f\4\2\2\u0223\u0224\5D#\2\u0224\u0225\5> \2\u0225"+
					"\u0227\3\2\2\2\u0226\u0222\3\2\2\2\u0227\u022a\3\2\2\2\u0228\u0226\3\2"+
					"\2\2\u0228\u0229\3\2\2\2\u0229=\3\2\2\2\u022a\u0228\3\2\2\2\u022b\u022c"+
					"\t\4\2\2\u022c?\3\2\2\2\u022d\u022e\7A\2\2\u022e\u022f\7\f\2\2\u022f\u0230"+
					"\7@\2\2\u0230\u0236\7\21\2\2\u0231\u0232\7A\2\2\u0232\u0233\7\f\2\2\u0233"+
					"\u0234\7A\2\2\u0234\u0236\7\21\2\2\u0235\u022d\3\2\2\2\u0235\u0231\3\2"+
					"\2\2\u0236A\3\2\2\2\u0237\u0238\5<\37\2\u0238\u0239\7\34\2\2\u0239\u023a"+
					"\5<\37\2\u023a\u0240\3\2\2\2\u023b\u023c\5<\37\2\u023c\u023d\7\34\2\2"+
					"\u023d\u023e\5> \2\u023e\u0240\3\2\2\2\u023f\u0237\3\2\2\2\u023f\u023b"+
					"\3\2\2\2\u0240C\3\2\2\2\u0241\u0242\t\5\2\2\u0242E\3\2\2\2\u0243\u0244"+
					"\b$\1\2\u0244\u0245\5J&\2\u0245\u024e\3\2\2\2\u0246\u0247\f\3\2\2\u0247"+
					"\u0248\7#\2\2\u0248\u0249\5H%\2\u0249\u024a\7$\2\2\u024a\u024b\5F$\4\u024b"+
					"\u024d\3\2\2\2\u024c\u0246\3\2\2\2\u024d\u0250\3\2\2\2\u024e\u024c\3\2"+
					"\2\2\u024e\u024f\3\2\2\2\u024fG\3\2\2\2\u0250\u024e\3\2\2\2\u0251\u0253"+
					"\5\62\32\2\u0252\u0251\3\2\2\2\u0252\u0253\3\2\2\2\u0253\u0256\3\2\2\2"+
					"\u0254\u0255\7(\2\2\u0255\u0257\5,\27\2\u0256\u0254\3\2\2\2\u0256\u0257"+
					"\3\2\2\2\u0257I\3\2\2\2\u0258\u0259\7\16\2\2\u0259\u025a\5.\30\2\u025a"+
					"\u025b\7\17\2\2\u025b\u026f\3\2\2\2\u025c\u025d\7\16\2\2\u025d\u025e\5"+
					".\30\2\u025e\u025f\7(\2\2\u025f\u0260\5,\27\2\u0260\u0261\7\17\2\2\u0261"+
					"\u026f\3\2\2\2\u0262\u0263\7\16\2\2\u0263\u0264\5.\30\2\u0264\u0265\7"+
					"(\2\2\u0265\u026a\5,\27\2\u0266\u0267\7\64\2\2\u0267\u0268\5L\'\2\u0268"+
					"\u0269\7\65\2\2\u0269\u026b\3\2\2\2\u026a\u0266\3\2\2\2\u026a\u026b\3"+
					"\2\2\2\u026b\u026c\3\2\2\2\u026c\u026d\7\17\2\2\u026d\u026f\3\2\2\2\u026e"+
					"\u0258\3\2\2\2\u026e\u025c\3\2\2\2\u026e\u0262\3\2\2\2\u026fK\3\2\2\2"+
					"\u0270\u0271\7A\2\2\u0271\u0272\7(\2\2\u0272\u0277\5> \2\u0273\u0274\7"+
					"\13\2\2\u0274\u0276\5L\'\2\u0275\u0273\3\2\2\2\u0276\u0279\3\2\2\2\u0277"+
					"\u0275\3\2\2\2\u0277\u0278\3\2\2\2\u0278M\3\2\2\2\u0279\u0277\3\2\2\2"+
					"\u027a\u027b\t\6\2\2\u027bO\3\2\2\2\u027c\u027d\t\7\2\2\u027dQ\3\2\2\2"+
					"+_djrv\u0092\u009d\u00c1\u00d9\u00e1\u00f3\u0115\u0123\u0128\u012d\u0162"+
					"\u0194\u0196\u019f\u01a6\u01b4\u01bb\u01bf\u01c5\u01c9\u01d9\u01e2\u0208"+
					"\u0210\u0212\u0219\u0220\u0228\u0235\u023f\u024e\u0252\u0256\u026a\u026e"+
					"\u0277";
	public static final ATN _ATN =
			new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}