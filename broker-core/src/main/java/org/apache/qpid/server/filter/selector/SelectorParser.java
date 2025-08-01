/* SelectorParser.java */
/* Generated By:JavaCC: Do not edit this line. SelectorParser.java */
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.qpid.server.filter.selector;

import java.io.StringReader;
import java.util.ArrayList;

import org.apache.qpid.server.filter.ArithmeticExpression;
import org.apache.qpid.server.filter.BooleanExpression;
import org.apache.qpid.server.filter.ComparisonExpression;
import org.apache.qpid.server.filter.ConstantExpression;
import org.apache.qpid.server.filter.Expression;
import org.apache.qpid.server.filter.LogicExpression;
import org.apache.qpid.server.filter.PropertyExpression;
import org.apache.qpid.server.filter.PropertyExpressionFactory;
import org.apache.qpid.server.filter.UnaryExpression;

/**
 * JMS Selector Parser generated by JavaCC
 *
 * Do not edit this .java file directly - it is autogenerated from SelectorParser.jj
 */
public class SelectorParser<E> implements SelectorParserConstants {
    private PropertyExpressionFactory<E> _factory;

    public SelectorParser()
    {
        this(new StringReader(""));
    }

    public void setPropertyExpressionFactory(PropertyExpressionFactory<E> factory)
    {
        _factory = factory;
    }

    public BooleanExpression<E> parse(String sql) throws ParseException
    {
        this.ReInit(new StringReader(sql));

        return this.JmsSelector();

    }

    private BooleanExpression<E> asBooleanExpression(Expression<E> value) throws ParseException
    {
        if (value instanceof BooleanExpression)
        {
            return (BooleanExpression<E>) value;
        }
        if (value instanceof PropertyExpression)
        {
            return UnaryExpression.createBooleanCast( (Expression<E>) value );
        }
        throw new ParseException("Expression will not result in a boolean value: " + value);
    }

// ----------------------------------------------------------------------------
// Grammer
// ----------------------------------------------------------------------------
  final public BooleanExpression<E> JmsSelector() throws ParseException {Expression<E> left=null;
    left = orExpression();
    jj_consume_token(0);
{if ("" != null) return asBooleanExpression(left);}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> orExpression() throws ParseException {Expression<E> left;
    Expression<E> right;
    left = andExpression();
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case OR:{
        ;
        break;
        }
      default:
        break label_1;
      }
      jj_consume_token(OR);
      right = andExpression();
left = LogicExpression.createOR(asBooleanExpression(left), asBooleanExpression(right));
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> andExpression() throws ParseException {Expression<E> left;
    Expression<E> right;
    left = equalityExpression();
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case AND:{
        ;
        break;
        }
      default:
        break label_2;
      }
      jj_consume_token(AND);
      right = equalityExpression();
left = LogicExpression.createAND(asBooleanExpression(left), asBooleanExpression(right));
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> equalityExpression() throws ParseException {Expression<E> left;
    Expression<E> right;
    left = comparisonExpression();
    label_3:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case IS:
      case 27:
      case 28:{
        ;
        break;
        }
      default:
        break label_3;
      }
      if (jj_2_1(2)) {
        jj_consume_token(27);
        right = comparisonExpression();
left = ComparisonExpression.createEqual(left, right);
      } else {
        switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
        case 28:{
          jj_consume_token(28);
          right = comparisonExpression();
left = ComparisonExpression.createNotEqual(left, right);
          break;
          }
        default:
          if (jj_2_2(2)) {
            jj_consume_token(IS);
            jj_consume_token(NULL);
left = ComparisonExpression.createIsNull(left);
          } else {
            switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
            case IS:{
              jj_consume_token(IS);
              jj_consume_token(NOT);
              jj_consume_token(NULL);
left = ComparisonExpression.createIsNotNull(left);
              break;
              }
            default:
              jj_consume_token(-1);
              throw new ParseException();
            }
          }
        }
      }
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> comparisonExpression() throws ParseException {Expression<E> left;
    Expression<E> right;
    Expression<E> low;
    Expression<E> high;
    String t, u;
        boolean not;
        ArrayList<String> list;
    left = addExpression();
    label_4:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case NOT:
      case BETWEEN:
      case LIKE:
      case IN:
      case 29:
      case 30:
      case 31:
      case 32:{
        ;
        break;
        }
      default:
        break label_4;
      }
      if (jj_2_3(2)) {
        jj_consume_token(29);
        right = addExpression();
left = ComparisonExpression.createGreaterThan(left, right);
      } else {
        switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
        case 30:{
          jj_consume_token(30);
          right = addExpression();
left = ComparisonExpression.createGreaterThanEqual(left, right);
          break;
          }
        case 31:{
          jj_consume_token(31);
          right = addExpression();
left = ComparisonExpression.createLessThan(left, right);
          break;
          }
        case 32:{
          jj_consume_token(32);
          right = addExpression();
left = ComparisonExpression.createLessThanEqual(left, right);
          break;
          }
        case LIKE:{
u=null;
          jj_consume_token(LIKE);
          t = stringLiteral();
          switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
          case ESCAPE:{
            jj_consume_token(ESCAPE);
            u = stringLiteral();
            break;
            }
          default:
            ;
          }
left = ComparisonExpression.createLike(left, t, u);
          break;
          }
        default:
          if (jj_2_4(2)) {
u=null;
            jj_consume_token(NOT);
            jj_consume_token(LIKE);
            t = stringLiteral();
            switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
            case ESCAPE:{
              jj_consume_token(ESCAPE);
              u = stringLiteral();
              break;
              }
            default:
              ;
            }
left = ComparisonExpression.createNotLike(left, t, u);
          } else {
            switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
            case BETWEEN:{
              jj_consume_token(BETWEEN);
              low = addExpression();
              jj_consume_token(AND);
              high = addExpression();
left = ComparisonExpression.createBetween(left, low, high);
              break;
              }
            default:
              if (jj_2_5(2)) {
                jj_consume_token(NOT);
                jj_consume_token(BETWEEN);
                low = addExpression();
                jj_consume_token(AND);
                high = addExpression();
left = ComparisonExpression.createNotBetween(left, low, high);
              } else {
                switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
                case IN:{
                  jj_consume_token(IN);
                  jj_consume_token(33);
                  t = stringLiteral();
list = new ArrayList<>();
            list.add( t );
                  label_5:
                  while (true) {
                    switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
                    case 34:{
                      ;
                      break;
                      }
                    default:
                      break label_5;
                    }
                    jj_consume_token(34);
                    t = stringLiteral();
list.add( t );
                  }
                  jj_consume_token(35);
left = ComparisonExpression.createInFilter(left, list, false );
                  break;
                  }
                default:
                  if (jj_2_6(2)) {
                    jj_consume_token(NOT);
                    jj_consume_token(IN);
                    jj_consume_token(33);
                    t = stringLiteral();
list = new ArrayList<>();
            list.add( t );
                    label_6:
                    while (true) {
                      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
                      case 34:{
                        ;
                        break;
                        }
                      default:
                        break label_6;
                      }
                      jj_consume_token(34);
                      t = stringLiteral();
list.add( t );
                    }
                    jj_consume_token(35);
left = ComparisonExpression.createNotInFilter(left, list, false);
                  } else {
                    jj_consume_token(-1);
                    throw new ParseException();
                  }
                }
              }
            }
          }
        }
      }
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> addExpression() throws ParseException {Expression<E> left;
    Expression<E> right;
    left = multExpr();
    label_7:
    while (true) {
      if (jj_2_7(2147483647)) {
        ;
      } else {
        break label_7;
      }
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case 36:{
        jj_consume_token(36);
        right = multExpr();
left = ArithmeticExpression.createPlus(left, right);
        break;
        }
      case 37:{
        jj_consume_token(37);
        right = multExpr();
left = ArithmeticExpression.createMinus(left, right);
        break;
        }
      default:
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> multExpr() throws ParseException {Expression<E> left;
    Expression<E> right;
    left = unaryExpr();
    label_8:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case 38:
      case 39:
      case 40:{
        ;
        break;
        }
      default:
        break label_8;
      }
      if (jj_2_8(2)) {
        jj_consume_token(38);
        right = unaryExpr();
left = ArithmeticExpression.createMultiply(left, right);
      } else {
        switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
        case 39:{
          jj_consume_token(39);
          right = unaryExpr();
left = ArithmeticExpression.createDivide(left, right);
          break;
          }
        case 40:{
          jj_consume_token(40);
          right = unaryExpr();
left = ArithmeticExpression.createMod(left, right);
          break;
          }
        default:
          jj_consume_token(-1);
          throw new ParseException();
        }
      }
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> unaryExpr() throws ParseException {String s=null;
    Expression<E> left=null;
    if (jj_2_9(2147483647)) {
      jj_consume_token(36);
      left = unaryExpr();
    } else {
      switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
      case 37:{
        jj_consume_token(37);
        left = unaryExpr();
left = UnaryExpression.createNegate(left);
        break;
        }
      default:
        if (jj_2_10(2147483647)) {
          jj_consume_token(NOT);
          left = equalityExpression();
left = UnaryExpression.createNOT( asBooleanExpression(left) );
        } else if (jj_2_11(2147483647)) {
          jj_consume_token(NOT);
          left = unaryExpr();
left = UnaryExpression.createNOT( asBooleanExpression(left) );
        } else {
          switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
          case TRUE:
          case FALSE:
          case NULL:
          case DECIMAL_LITERAL:
          case HEX_LITERAL:
          case OCTAL_LITERAL:
          case FLOATING_POINT_LITERAL:
          case STRING_LITERAL:
          case ID:
          case QUOTED_ID:
          case 33:{
            left = primaryExpr();
            break;
            }
          default:
            jj_consume_token(-1);
            throw new ParseException();
          }
        }
      }
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public Expression<E> primaryExpr() throws ParseException {Expression<E> left=null;
    switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
    case TRUE:
    case FALSE:
    case NULL:
    case DECIMAL_LITERAL:
    case HEX_LITERAL:
    case OCTAL_LITERAL:
    case FLOATING_POINT_LITERAL:
    case STRING_LITERAL:{
      left = literal();
      break;
      }
    case ID:
    case QUOTED_ID:{
      left = variable();
      break;
      }
    default:
      if (jj_2_12(2)) {
        jj_consume_token(33);
        left = orExpression();
        jj_consume_token(35);
      } else {
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public ConstantExpression<E> literal() throws ParseException {Token t;
    String s;
    ConstantExpression<E> left=null;
    switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
    case STRING_LITERAL:{
      s = stringLiteral();
left = new ConstantExpression<>(s);
      break;
      }
    case DECIMAL_LITERAL:{
      t = jj_consume_token(DECIMAL_LITERAL);
left = ConstantExpression.createFromDecimal(t.image);
      break;
      }
    case HEX_LITERAL:{
      t = jj_consume_token(HEX_LITERAL);
left = ConstantExpression.createFromHex(t.image);
      break;
      }
    case OCTAL_LITERAL:{
      t = jj_consume_token(OCTAL_LITERAL);
left = ConstantExpression.createFromOctal(t.image);
      break;
      }
    case FLOATING_POINT_LITERAL:{
      t = jj_consume_token(FLOATING_POINT_LITERAL);
left = ConstantExpression.createFloat(t.image);
      break;
      }
    case TRUE:{
      jj_consume_token(TRUE);
left = ConstantExpression.TRUE;
      break;
      }
    case FALSE:{
      jj_consume_token(FALSE);
left = ConstantExpression.FALSE;
      break;
      }
    case NULL:{
      jj_consume_token(NULL);
left = ConstantExpression.NULL;
      break;
      }
    default:
      jj_consume_token(-1);
      throw new ParseException();
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  final public String stringLiteral() throws ParseException {Token t;
    StringBuffer rc = new StringBuffer();
    boolean first=true;
    t = jj_consume_token(STRING_LITERAL);
// Decode the sting value.
        String image = t.image;
        for( int i=1; i < image.length()-1; i++ ) {
            char c = image.charAt(i);
            if( c == (char) 0x27 )//single quote
            {
                i++;
            }
            rc.append(c);
        }
            {if ("" != null) return rc.toString();}
    throw new Error("Missing return statement in function");
}

  final public PropertyExpression<E> variable() throws ParseException {Token t;
    StringBuffer rc = new StringBuffer();
    PropertyExpression<E> left=null;
    switch ((jj_ntk==-1)?jj_ntk_f():jj_ntk) {
    case ID:{
      t = jj_consume_token(ID);
left = _factory.createPropertyExpression(t.image);
      break;
      }
    case QUOTED_ID:{
      t = jj_consume_token(QUOTED_ID);
// Decode the sting value.
            String image = t.image;
            for( int i=1; i < image.length()-1; i++ ) {
                char c = image.charAt(i);
                if( c == '"' )
                {
                    i++;
                }
                rc.append(c);
            }
            {if ("" != null) return _factory.createPropertyExpression(rc.toString());}
      break;
      }
    default:
      jj_consume_token(-1);
      throw new ParseException();
    }
{if ("" != null) return left;}
    throw new Error("Missing return statement in function");
}

  private boolean jj_2_1(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_1()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_2(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_2()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_3(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_3()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_4(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_4()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_5(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_5()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_6(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_6()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_7(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_7()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_8(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_8()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_9(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_9()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_10(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_10()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_11(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_11()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_2_12(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return (!jj_3_12()); }
    catch(LookaheadSuccess ls) { return true; }
  }

  private boolean jj_3R_literal_468_5_38()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_literal_469_9_47()) {
    jj_scanpos = xsp;
    if (jj_3R_literal_476_9_48()) {
    jj_scanpos = xsp;
    if (jj_3R_literal_483_9_49()) {
    jj_scanpos = xsp;
    if (jj_3R_literal_490_9_50()) {
    jj_scanpos = xsp;
    if (jj_3R_literal_497_9_51()) {
    jj_scanpos = xsp;
    if (jj_3R_literal_504_9_52()) {
    jj_scanpos = xsp;
    if (jj_3R_literal_511_9_53()) {
    jj_scanpos = xsp;
    if (jj_3R_literal_518_9_54()) return true;
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

  private boolean jj_3R_comparisonExpression_277_5_9()
 {
    if (jj_3R_addExpression_365_5_10()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_comparisonExpression_279_9_26()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3R_variable_560_5_39()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_variable_561_9_55()) {
    jj_scanpos = xsp;
    if (jj_3R_variable_566_9_56()) return true;
    }
    return false;
  }

  private boolean jj_3R_addExpression_365_5_10()
 {
    if (jj_3R_multExpr_391_5_11()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_addExpression_367_13_31()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3_12()
 {
    if (jj_scan_token(33)) return true;
    if (jj_3R_orExpression_203_5_14()) return true;
    if (jj_scan_token(35)) return true;
    return false;
  }

  private boolean jj_3R_primaryExpr_452_9_30()
 {
    if (jj_3R_variable_560_5_39()) return true;
    return false;
  }

  private boolean jj_3R_primaryExpr_450_9_29()
 {
    if (jj_3R_literal_468_5_38()) return true;
    return false;
  }

  private boolean jj_3R_equalityExpression_256_9_28()
 {
    if (jj_scan_token(IS)) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(NULL)) return true;
    return false;
  }

  private boolean jj_3_11()
 {
    if (jj_scan_token(NOT)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3R_primaryExpr_449_5_25()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_primaryExpr_450_9_29()) {
    jj_scanpos = xsp;
    if (jj_3R_primaryExpr_452_9_30()) {
    jj_scanpos = xsp;
    if (jj_3_12()) return true;
    }
    }
    return false;
  }

  private boolean jj_3R_comparisonExpression_345_11_46()
 {
    if (jj_scan_token(34)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    return false;
  }

  private boolean jj_3R_comparisonExpression_311_44_44()
 {
    if (jj_scan_token(ESCAPE)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    return false;
  }

  private boolean jj_3_2()
 {
    if (jj_scan_token(IS)) return true;
    if (jj_scan_token(NULL)) return true;
    return false;
  }

  private boolean jj_3R_unaryExpr_437_13_20()
 {
    if (jj_3R_primaryExpr_449_5_25()) return true;
    return false;
  }

  private boolean jj_3R_stringLiteral_537_5_42()
 {
    if (jj_scan_token(STRING_LITERAL)) return true;
    return false;
  }

  private boolean jj_3_10()
 {
    if (jj_scan_token(NOT)) return true;
    if (jj_3R_equalityExpression_239_5_13()) return true;
    return false;
  }

  private boolean jj_3R_equalityExpression_246_9_27()
 {
    if (jj_scan_token(28)) return true;
    if (jj_3R_comparisonExpression_277_5_9()) return true;
    return false;
  }

  private boolean jj_3R_unaryExpr_432_13_19()
 {
    if (jj_scan_token(NOT)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3_6()
 {
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(IN)) return true;
    if (jj_scan_token(33)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_comparisonExpression_345_11_46()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(35)) return true;
    return false;
  }

  private boolean jj_3_9()
 {
    if (jj_scan_token(36)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3_1()
 {
    if (jj_scan_token(27)) return true;
    if (jj_3R_comparisonExpression_277_5_9()) return true;
    return false;
  }

  private boolean jj_3R_equalityExpression_241_9_21()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3_1()) {
    jj_scanpos = xsp;
    if (jj_3R_equalityExpression_246_9_27()) {
    jj_scanpos = xsp;
    if (jj_3_2()) {
    jj_scanpos = xsp;
    if (jj_3R_equalityExpression_256_9_28()) return true;
    }
    }
    }
    return false;
  }

  private boolean jj_3R_comparisonExpression_331_11_45()
 {
    if (jj_scan_token(34)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    return false;
  }

  private boolean jj_3R_unaryExpr_427_9_18()
 {
    if (jj_scan_token(NOT)) return true;
    if (jj_3R_equalityExpression_239_5_13()) return true;
    return false;
  }

  private boolean jj_3R_comparisonExpression_302_38_43()
 {
    if (jj_scan_token(ESCAPE)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    return false;
  }

  private boolean jj_3R_unaryExpr_422_13_17()
 {
    if (jj_scan_token(37)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3R_equalityExpression_239_5_13()
 {
    if (jj_3R_comparisonExpression_277_5_9()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_equalityExpression_241_9_21()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3R_unaryExpr_420_13_16()
 {
    if (jj_scan_token(36)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3R_literal_518_9_54()
 {
    if (jj_scan_token(NULL)) return true;
    return false;
  }

  private boolean jj_3R_comparisonExpression_326_9_37()
 {
    if (jj_scan_token(IN)) return true;
    if (jj_scan_token(33)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_comparisonExpression_331_11_45()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(35)) return true;
    return false;
  }

  private boolean jj_3R_unaryExpr_419_9_12()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_unaryExpr_420_13_16()) {
    jj_scanpos = xsp;
    if (jj_3R_unaryExpr_422_13_17()) {
    jj_scanpos = xsp;
    if (jj_3R_unaryExpr_427_9_18()) {
    jj_scanpos = xsp;
    if (jj_3R_unaryExpr_432_13_19()) {
    jj_scanpos = xsp;
    if (jj_3R_unaryExpr_437_13_20()) return true;
    }
    }
    }
    }
    return false;
  }

  private boolean jj_3R_literal_511_9_53()
 {
    if (jj_scan_token(FALSE)) return true;
    return false;
  }

  private boolean jj_3_5()
 {
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(BETWEEN)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    return false;
  }

  private boolean jj_3R_andExpression_223_9_58()
 {
    if (jj_scan_token(AND)) return true;
    if (jj_3R_equalityExpression_239_5_13()) return true;
    return false;
  }

  private boolean jj_3R_comparisonExpression_316_9_36()
 {
    if (jj_scan_token(BETWEEN)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    return false;
  }

  private boolean jj_3R_literal_504_9_52()
 {
    if (jj_scan_token(TRUE)) return true;
    return false;
  }

  private boolean jj_3R_andExpression_221_5_22()
 {
    if (jj_3R_equalityExpression_239_5_13()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_andExpression_223_9_58()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3R_multExpr_403_9_24()
 {
    if (jj_scan_token(40)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3R_literal_497_9_51()
 {
    if (jj_scan_token(FLOATING_POINT_LITERAL)) return true;
    return false;
  }

  private boolean jj_3_4()
 {
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(LIKE)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_comparisonExpression_311_44_44()) jj_scanpos = xsp;
    return false;
  }

  private boolean jj_3R_multExpr_398_9_23()
 {
    if (jj_scan_token(39)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3R_orExpression_205_9_57()
 {
    if (jj_scan_token(OR)) return true;
    if (jj_3R_andExpression_221_5_22()) return true;
    return false;
  }

  private boolean jj_3R_literal_490_9_50()
 {
    if (jj_scan_token(OCTAL_LITERAL)) return true;
    return false;
  }

  private boolean jj_3R_comparisonExpression_299_9_35()
 {
    if (jj_scan_token(LIKE)) return true;
    if (jj_3R_stringLiteral_537_5_42()) return true;
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_comparisonExpression_302_38_43()) jj_scanpos = xsp;
    return false;
  }

  private boolean jj_3_8()
 {
    if (jj_scan_token(38)) return true;
    if (jj_3R_unaryExpr_419_9_12()) return true;
    return false;
  }

  private boolean jj_3R_multExpr_393_9_15()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3_8()) {
    jj_scanpos = xsp;
    if (jj_3R_multExpr_398_9_23()) {
    jj_scanpos = xsp;
    if (jj_3R_multExpr_403_9_24()) return true;
    }
    }
    return false;
  }

  private boolean jj_3R_comparisonExpression_294_9_34()
 {
    if (jj_scan_token(32)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    return false;
  }

  private boolean jj_3R_orExpression_203_5_14()
 {
    if (jj_3R_andExpression_221_5_22()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_orExpression_205_9_57()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3R_literal_483_9_49()
 {
    if (jj_scan_token(HEX_LITERAL)) return true;
    return false;
  }

  private boolean jj_3R_multExpr_391_5_11()
 {
    if (jj_3R_unaryExpr_419_9_12()) return true;
    Token xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3R_multExpr_393_9_15()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

  private boolean jj_3R_comparisonExpression_289_9_33()
 {
    if (jj_scan_token(31)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    return false;
  }

  private boolean jj_3R_addExpression_374_17_41()
 {
    if (jj_scan_token(37)) return true;
    if (jj_3R_multExpr_391_5_11()) return true;
    return false;
  }

  private boolean jj_3_7()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_scan_token(36)) {
    jj_scanpos = xsp;
    if (jj_scan_token(37)) return true;
    }
    if (jj_3R_multExpr_391_5_11()) return true;
    return false;
  }

  private boolean jj_3R_literal_476_9_48()
 {
    if (jj_scan_token(DECIMAL_LITERAL)) return true;
    return false;
  }

  private boolean jj_3R_comparisonExpression_284_9_32()
 {
    if (jj_scan_token(30)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    return false;
  }

  private boolean jj_3R_addExpression_369_17_40()
 {
    if (jj_scan_token(36)) return true;
    if (jj_3R_multExpr_391_5_11()) return true;
    return false;
  }

  private boolean jj_3R_variable_566_9_56()
 {
    if (jj_scan_token(QUOTED_ID)) return true;
    return false;
  }

  private boolean jj_3R_literal_469_9_47()
 {
    if (jj_3R_stringLiteral_537_5_42()) return true;
    return false;
  }

  private boolean jj_3_3()
 {
    if (jj_scan_token(29)) return true;
    if (jj_3R_addExpression_365_5_10()) return true;
    return false;
  }

  private boolean jj_3R_comparisonExpression_279_9_26()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3_3()) {
    jj_scanpos = xsp;
    if (jj_3R_comparisonExpression_284_9_32()) {
    jj_scanpos = xsp;
    if (jj_3R_comparisonExpression_289_9_33()) {
    jj_scanpos = xsp;
    if (jj_3R_comparisonExpression_294_9_34()) {
    jj_scanpos = xsp;
    if (jj_3R_comparisonExpression_299_9_35()) {
    jj_scanpos = xsp;
    if (jj_3_4()) {
    jj_scanpos = xsp;
    if (jj_3R_comparisonExpression_316_9_36()) {
    jj_scanpos = xsp;
    if (jj_3_5()) {
    jj_scanpos = xsp;
    if (jj_3R_comparisonExpression_326_9_37()) {
    jj_scanpos = xsp;
    if (jj_3_6()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

  private boolean jj_3R_variable_561_9_55()
 {
    if (jj_scan_token(ID)) return true;
    return false;
  }

  private boolean jj_3R_addExpression_367_13_31()
 {
    Token xsp;
    xsp = jj_scanpos;
    if (jj_3R_addExpression_369_17_40()) {
    jj_scanpos = xsp;
    if (jj_3R_addExpression_374_17_41()) return true;
    }
    return false;
  }

  /** Generated Token Manager. */
  public SelectorParserTokenManager token_source;
  SimpleCharStream jj_input_stream;
  /** Current token. */
  public Token token;
  /** Next token. */
  public Token jj_nt;
  private int jj_ntk;
  private Token jj_scanpos, jj_lastpos;
  private int jj_la;

  /** Constructor with InputStream. */
  public SelectorParser(java.io.InputStream stream) {
	  this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public SelectorParser(java.io.InputStream stream, String encoding) {
	 try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
	 token_source = new SelectorParserTokenManager(jj_input_stream);
	 token = new Token();
	 jj_ntk = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream) {
	  ReInit(stream, null);
  }
  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream, String encoding) {
	 try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
	 token_source.ReInit(jj_input_stream);
	 token = new Token();
	 jj_ntk = -1;
  }

  /** Constructor. */
  public SelectorParser(java.io.Reader stream) {
	 jj_input_stream = new SimpleCharStream(stream, 1, 1);
	 token_source = new SelectorParserTokenManager(jj_input_stream);
	 token = new Token();
	 jj_ntk = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.Reader stream) {
	if (jj_input_stream == null) {
	   jj_input_stream = new SimpleCharStream(stream, 1, 1);
	} else {
	   jj_input_stream.ReInit(stream, 1, 1);
	}
	if (token_source == null) {
 token_source = new SelectorParserTokenManager(jj_input_stream);
	}

	 token_source.ReInit(jj_input_stream);
	 token = new Token();
	 jj_ntk = -1;
  }

  /** Constructor with generated Token Manager. */
  public SelectorParser(SelectorParserTokenManager tm) {
	 token_source = tm;
	 token = new Token();
	 jj_ntk = -1;
  }

  /** Reinitialise. */
  public void ReInit(SelectorParserTokenManager tm) {
	 token_source = tm;
	 token = new Token();
	 jj_ntk = -1;
  }

  private Token jj_consume_token(int kind) throws ParseException {
	 Token oldToken;
	 if ((oldToken = token).next != null) token = token.next;
	 else token = token.next = token_source.getNextToken();
	 jj_ntk = -1;
	 if (token.kind == kind) {
	   return token;
	 }
	 token = oldToken;
	 throw generateParseException();
  }

  @SuppressWarnings("serial")
  static private final class LookaheadSuccess extends Error {
    @Override
    public Throwable fillInStackTrace() {
      return this;
    }
  }
  static private final LookaheadSuccess jj_ls = new LookaheadSuccess();
  private boolean jj_scan_token(int kind) {
	 if (jj_scanpos == jj_lastpos) {
	   jj_la--;
	   if (jj_scanpos.next == null) {
		 jj_lastpos = jj_scanpos = jj_scanpos.next = token_source.getNextToken();
	   } else {
		 jj_lastpos = jj_scanpos = jj_scanpos.next;
	   }
	 } else {
	   jj_scanpos = jj_scanpos.next;
	 }
	 if (jj_scanpos.kind != kind) return true;
	 if (jj_la == 0 && jj_scanpos == jj_lastpos) throw jj_ls;
	 return false;
  }


/** Get the next Token. */
  final public Token getNextToken() {
	 if (token.next != null) token = token.next;
	 else token = token.next = token_source.getNextToken();
	 jj_ntk = -1;
	 return token;
  }

/** Get the specific Token. */
  final public Token getToken(int index) {
	 Token t = token;
	 for (int i = 0; i < index; i++) {
	   if (t.next != null) t = t.next;
	   else t = t.next = token_source.getNextToken();
	 }
	 return t;
  }

  private int jj_ntk_f() {
	 if ((jj_nt=token.next) == null)
	   return (jj_ntk = (token.next=token_source.getNextToken()).kind);
	 else
	   return (jj_ntk = jj_nt.kind);
  }

  /** Generate ParseException. */
  public ParseException generateParseException() {
	 Token errortok = token.next;
	 int line = errortok.beginLine, column = errortok.beginColumn;
	 String mess = (errortok.kind == 0) ? tokenImage[0] : errortok.image;
	 return new ParseException("Parse error at line " + line + ", column " + column + ".  Encountered: " + mess);
  }

  private boolean trace_enabled;

/** Trace enabled. */
  final public boolean trace_enabled() {
	 return trace_enabled;
  }

  /** Enable tracing. */
  final public void enable_tracing() {
  }

  /** Disable tracing. */
  final public void disable_tracing() {
  }

}
