grammar View;

root : COMMAND NAME viewuse scope query |
        'CG' query |
        changegraph |
        viewuse scope query
        ;
scope: 'LOCAL' | 'GLOBAL' |;

query :  KEYWORD expr conditions pipeline? returnstmt |
         KEYWORD path conditions pipeline? returnstmt
        ;

changegraph : KEYWORD expr conditions 'SET' setattr
            | KEYWORD expr conditions 'DELETE' NAME
            | KEYWORD expr conditions 'REMOVE' attribute
            | KEYWORD expr conditions 'CREATE' insertion
            | 'CREATE' insertion
;

pipeline    : 'WITH' replacements pipeconditions | 'WITH' replacements pipeconditions pipeline
;

replacements : NAME as NAME (',' replacements) |
               function as NAME (',' replacements) |
               NAME (',' replacements) |NAME as NAME | function as NAME | NAME |
               iteration as NAME (',' replacements) | iteration as NAME
;

iteration : '[' NAME 'IN' range '(' validVal ',' validVal ')' '|'  iterationCase ']'
;

validVal : size '(' NAME ')' | validVal arithmetic validVal | VALUE | 'NULL' | indexing;

iterationCase : 'CASE WHEN' pipeexpr 'THEN' validVal 'ELSE' validVal 'END';

size: 'SIZE' | 'size';

range: 'RANGE' | 'range';
pipeconditions  : 'WHERE' pipeexpr |
;

pipeexpr    : attribute COMPARISON attribute |
              attribute COMPARISON val |
              attribute '=' attribute |
              attribute '=' val |
              function COMPARISON attribute |
              function COMPARISON val |
              function '=' attribute |
              function '=' val
;

function : 'COLLECT(' NAME ')' | 'UNWIND' NAME | 'COUNT(' NAME ')' | 'COUNT(*)' | 'MAX('NAME')';

viewuse  : 'WITH VIEWS' usedviews | ;
usedviews : NAME*;

// viewatom supports directed relationships
viewatom : variable | // for early version, this line was '(' NAME ':' MNAME ')'
             viewatom '-[' relation ']-' viewatom |
             viewatom '-[' relation ']->' viewatom |
             viewatom '<-[' relation ']-' viewatom |
             viewatom '<-[' relation ']->' viewatom |
             viewatom '-[' relation ']-' variable |
             viewatom '-[' relation ']->' variable |
             viewatom '<-[' relation ']-' variable |
             viewatom '<-[' relation ']->' variable |
             variable '-[' relation ']-' viewatom |
             variable '-[' relation ']->' viewatom |
             variable '<-[' relation ']-' viewatom |
             variable '<-[' relation ']->' viewatom |
             variable '-[' relation ']-' variable |
             variable '-[' relation ']->' variable |
             variable '<-[' relation ']-' variable |
             variable '<-[' relation ']->' variable
             ;
returnstmt : RETURN retval (',' retval)*;
retval : 'NODES(' NAME ')' |
         attribute
         ;

expr : viewatom ;
variable :  '('nodeName')' | '('nodeName':'type')'; // nodeName
type    : NAME ;
nodeName : NAME ;
// Changed by Mohanna to include
    // Queries with more than one type of relationships [:POSTED|PARENT_OF]
    // Specifying the length of the relationship in pattern matching
relation : relationValue?(':'type)?('|'type)*(relationLength)?;
relationValue : NAME ;
relationLength: '*' | '*'VALUE | '*'VALUE'..'VALUE | '*..'VALUE | '*'VALUE'..' ;
path    : NAME '=' expr ;
conditions  : 'WHERE' boolexpr | ;
boolexpr    :
              attribute COMPARISON attribute |
              attribute COMPARISON val |
              attribute '=' attribute |
              attribute '=' val |
              boolexpr 'AND' boolexpr |
              boolexpr 'OR' boolexpr |
              VALUE OPERATOR attribute | //not supported
              NAME 'IN' NAME | //viewUse
              '(' boolexpr ')' |
              'not' boolexpr |
              'NOT' boolexpr |
              exists '(' attribute ')';
attribute   : NAME('.'NAME)?  | val arithmetic attribute | attribute arithmetic val | indexing;
val         : VALUE | NAME | CONSTANTS;

indexing : NAME'['VALUE']' | NAME '[' NAME ']'
;

setattr:  attribute '=' attribute |
          attribute '=' val;

arithmetic: '*' | '+' | '-' | '/'
;

// TODO: Should insertion also support directed relationships?
insertion: insertionVar |
           insertion '-[' insertrelation ']-' insertion
           ;

insertrelation: relationValue?(':'type)? ;
insertionVar: '('nodeName')' | '('nodeName':'type')'|
                '('nodeName':'type ('{'insertAttributes'}')? ')'
            ;
insertAttributes:  NAME':'val (',' insertAttributes)*;

as : 'AS' | 'as';
exists : 'EXISTS' | 'exists';


/*
Lexer rules
*/

KEYWORD   : 'MATCH' | 'MERGE' ;
RETURN  : 'RETURN' ;
COMMAND : 'CREATE VIEW AS';
COMPARISON : '>' | '<' | '>=' |'<=' ;
OPERATOR : 'CASE'|'CONTAINS'|'ELSE'|'END'|
           'ENDS WITH'|'IN'|'IS NOT NULL'|'IS NULL'|'NOT'|'STARTS WITH'|'THEN'|'WHEN'|'XOR';
CONSTANTS : 'true' | 'false' | 'null';

VALUE   : [0]|[1-9][0-9]* | [0]'.'[0-9]*[1-9]
            | '"'[a-zA-Z_# 0-9]+'"'| '\''[a-zA-Z_# 0-9]+'\'';
// Mohanna's Change: Underscore is added to the number part to support better namings for
// views that are each other's variants
NAME    : [a-zA-Z_]+[0-9_]* ;
WHITESPACE : ' '-> skip ;
ANY     : . ;