// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Attribute expression compatibility with the Python client's Lark grammar.
use crate::{ErrorKind, MscError, Result};
use serde_json::Value;
use std::{cmp::Ordering, collections::BTreeMap};

pub(crate) struct AttributeFilter(Vec<Expression>);

impl AttributeFilter {
    pub(crate) fn compile(source: Option<&str>) -> Result<Option<Self>> {
        let Some(source) = source.filter(|source| !source.is_empty()) else {
            return Ok(None);
        };
        if source.len() > 65_536 {
            return Err(invalid());
        }
        let mut parser = Parser {
            source,
            position: 0,
            nodes: Vec::new(),
        };
        parser.expression(0)?;
        parser.whitespace();
        if parser.position != source.len() {
            return Err(invalid());
        }
        Ok(Some(Self(parser.nodes)))
    }
    pub(crate) fn matches(&self, attributes: &BTreeMap<String, Value>) -> bool {
        let mut results = Vec::with_capacity(self.0.len());
        for node in &self.0 {
            let value = match node {
                Expression::Compare(key, operator, expected) => attributes
                    .get(key)
                    .is_some_and(|actual| operator.compare(&python_string(actual), expected)),
                Expression::And(left, right) => results[*left] && results[*right],
                Expression::Or(left, right) => results[*left] || results[*right],
            };
            results.push(value);
        }
        results.last().copied().unwrap_or(false)
    }
}

enum Expression {
    Compare(String, Operator, String),
    And(usize, usize),
    Or(usize, usize),
}

#[derive(Clone, Copy)]
enum Operator {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}
impl Operator {
    fn compare(self, actual: &str, expected: &str) -> bool {
        match self {
            Self::Equal => actual == expected,
            Self::NotEqual => actual != expected,
            _ => {
                let order = match (python_float(actual), python_float(expected)) {
                    (Some(actual), Some(expected)) => actual.partial_cmp(&expected),
                    _ => Some(actual.cmp(expected)),
                };
                match self {
                    Self::Greater => order == Some(Ordering::Greater),
                    Self::GreaterEqual => {
                        matches!(order, Some(Ordering::Greater | Ordering::Equal))
                    }
                    Self::Less => order == Some(Ordering::Less),
                    Self::LessEqual => matches!(order, Some(Ordering::Less | Ordering::Equal)),
                    _ => unreachable!("equality handled above"),
                }
            }
        }
    }
}

fn invalid() -> MscError {
    MscError::new(
        ErrorKind::InvalidConfig,
        "invalid attribute filter expression",
    )
}

struct Parser<'a> {
    source: &'a str,
    position: usize,
    nodes: Vec<Expression>,
}
impl Parser<'_> {
    // Python's grammar leaves AND and OR at the same precedence; LALR shift
    // resolution makes both right associative. Parentheses override grouping.
    fn expression(&mut self, depth: usize) -> Result<usize> {
        if depth >= 256 {
            return Err(invalid());
        }
        let mut terms = Vec::new();
        let mut operators = Vec::new();
        loop {
            self.whitespace();
            let term = if self.take("(") {
                let term = self.expression(depth + 1)?;
                self.whitespace();
                if !self.take(")") {
                    return Err(invalid());
                }
                term
            } else {
                let node = self.comparison()?;
                self.nodes.push(node);
                self.nodes.len() - 1
            };
            terms.push(term);
            self.whitespace();
            if self.take("AND") {
                operators.push(true);
            } else if self.take("OR") {
                operators.push(false);
            } else {
                break;
            }
        }
        let mut right = terms.pop().ok_or_else(invalid)?;
        for (left, and) in terms.into_iter().zip(operators).rev() {
            self.nodes.push(if and {
                Expression::And(left, right)
            } else {
                Expression::Or(left, right)
            });
            right = self.nodes.len() - 1;
        }
        Ok(right)
    }
    fn comparison(&mut self) -> Result<Expression> {
        let start = self.position;
        if !self
            .peek()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            return Err(invalid());
        }
        self.position += 1;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            self.position += 1;
        }
        let key = self.source[start..self.position].to_owned();
        self.whitespace();
        let operator = [
            ("!=", Operator::NotEqual),
            (">=", Operator::GreaterEqual),
            ("<=", Operator::LessEqual),
            ("=", Operator::Equal),
            (">", Operator::Greater),
            ("<", Operator::Less),
        ]
        .into_iter()
        .find_map(|(token, operator)| self.take(token).then_some(operator))
        .ok_or_else(invalid)?;
        self.whitespace();
        let value = if self.take("\"") {
            self.string()?
        } else {
            self.number()?
        };
        Ok(Expression::Compare(key, operator, value))
    }
    fn string(&mut self) -> Result<String> {
        let start = self.position;
        let mut escaped = false;
        while let Some(character) = self.peek() {
            if character == '\n' {
                return Err(invalid());
            }
            self.position += character.len_utf8();
            if character == '"' && !escaped {
                return Ok(self.source[start..self.position - 1].to_owned());
            }
            escaped = character == '\\' && !escaped;
        }
        Err(invalid())
    }
    fn number(&mut self) -> Result<String> {
        let start = self.position;
        if !self.take("+") {
            self.take("-");
        }
        let mut digits = self.digits();
        if self.take(".") {
            digits += self.digits();
        }
        if digits == 0 {
            return Err(invalid());
        }
        if self.take("e") || self.take("E") {
            if !self.take("+") {
                self.take("-");
            }
            if self.digits() == 0 {
                return Err(invalid());
            }
        }
        Ok(self.source[start..self.position].to_owned())
    }
    fn digits(&mut self) -> usize {
        let start = self.position;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.position += 1;
        }
        self.position - start
    }
    fn take(&mut self, token: &str) -> bool {
        if self.source[self.position..].starts_with(token) {
            self.position += token.len();
            true
        } else {
            false
        }
    }
    fn whitespace(&mut self) {
        while self
            .peek()
            .is_some_and(|c| matches!(c, ' ' | '\t' | '\r' | '\n' | '\u{000c}'))
        {
            self.position += 1;
        }
    }
    fn peek(&self) -> Option<char> {
        self.source[self.position..].chars().next()
    }
}

fn python_float(value: &str) -> Option<f64> {
    let normalized;
    let value = if value.is_ascii() {
        value
    } else {
        normalized = value
            .chars()
            .map(|character| decimal_digit(character).unwrap_or(character))
            .collect::<String>();
        normalized.as_str()
    }
    .trim();
    if !value.contains('_') {
        return value.parse().ok();
    }
    let bytes = value.as_bytes();
    if bytes.iter().enumerate().any(|(index, byte)| {
        *byte == b'_'
            && (index == 0
                || index + 1 == bytes.len()
                || !bytes[index - 1].is_ascii_digit()
                || !bytes[index + 1].is_ascii_digit())
    }) {
        return None;
    }
    value.replace('_', "").parse().ok()
}

fn decimal_digit(character: char) -> Option<char> {
    // Unicode 15 Nd ranges used by the reference Python runtime. Each range
    // consists of ten consecutive decimal digits; other numeric characters,
    // such as superscripts and fractions, are not accepted by Python float().
    const ZEROES: &[u32] = &[
        0x30, 0x660, 0x6f0, 0x7c0, 0x966, 0x9e6, 0xa66, 0xae6, 0xb66, 0xbe6, 0xc66, 0xce6, 0xd66,
        0xde6, 0xe50, 0xed0, 0xf20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90,
        0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0,
        0xff10, 0x104a0, 0x10d30, 0x11066, 0x110f0, 0x11136, 0x111d0, 0x112f0, 0x11450, 0x114d0,
        0x11650, 0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50, 0x11d50, 0x11da0, 0x11f50, 0x16a60,
        0x16ac0, 0x16b50, 0x1d7ce, 0x1d7d8, 0x1d7e2, 0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e4f0,
        0x1e950, 0x1fbf0,
    ];
    let code = u32::from(character);
    let index = ZEROES
        .partition_point(|zero| *zero <= code)
        .checked_sub(1)?;
    let digit = code - ZEROES[index];
    (digit < 10)
        .then(|| char::from_u32(u32::from(b'0') + digit))
        .flatten()
}

fn python_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "None".into(),
        Value::Bool(value) => if *value { "True" } else { "False" }.into(),
        Value::Number(value) if value.is_i64() || value.is_u64() => value.to_string(),
        Value::Number(value) => {
            let text = format!(
                "{:?}",
                value.as_f64().expect("JSON number is representable")
            );
            text.split_once('e').map_or_else(
                || text.clone(),
                |(mantissa, exponent)| {
                    let exponent = exponent
                        .parse::<i32>()
                        .expect("formatted floating-point exponent");
                    format!(
                        "{mantissa}e{}{:02}",
                        if exponent < 0 { '-' } else { '+' },
                        exponent.abs()
                    )
                },
            )
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(python_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!("{}: {}", quoted(key), python_repr(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}
fn python_repr(value: &Value) -> String {
    if let Value::String(value) = value {
        quoted(value)
    } else {
        python_string(value)
    }
}
fn quoted(value: &str) -> String {
    use std::fmt::Write as _;
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut output = String::from(quote);
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character == quote => {
                output.push('\\');
                output.push(character);
            }
            character if character.is_control() => {
                write!(output, "\\x{:02x}", u32::from(character)).expect("writing a String");
            }
            character => output.push(character),
        }
    }
    output.push(quote);
    output
}

#[cfg(test)]
mod tests {
    use super::AttributeFilter;
    use crate::ErrorKind;
    use serde_json::json;

    fn matches(expression: &str, attributes: serde_json::Value) -> bool {
        AttributeFilter::compile(Some(expression))
            .unwrap()
            .unwrap()
            .matches(&serde_json::from_value(attributes).unwrap())
    }

    #[test]
    fn equality_preserves_literal_and_python_scalar_string_forms() {
        let attributes = json!({"integer":1,"decimal":1.0,"small":0.00001,"flag":true,"null":null,"escaped":r"a\nb", "array":[1,"x"], "object":{"a":1}});
        for expression in [
            "integer=1",
            "decimal=1.0",
            "flag=\"True\"",
            "null=\"None\"",
            "small=\"1e-05\"",
            r#"escaped="a\nb""#,
            "array=\"[1, 'x']\"",
            "object=\"{'a': 1}\"",
        ] {
            assert!(matches(expression, attributes.clone()), "{expression}");
        }
        for expression in [
            "integer=1.0",
            "integer=+1",
            "decimal=1",
            "small=1e-5",
            "flag=\"true\"",
            "missing!=1",
        ] {
            assert!(!matches(expression, attributes.clone()), "{expression}");
        }
    }

    #[test]
    fn ordering_coerces_numeric_values_and_falls_back_to_lexical_comparison() {
        let attributes = json!({"priority":"10","underscores":"1_0","word":"zeta","nan":"NaN","number":-1.5,"large":"Infinity","arabic":"١٢","styled":"𝟙_𝟚"});
        for expression in [
            "priority>2",
            "priority>=10",
            "priority<=10",
            "underscores<11",
            "word>\"alpha\"",
            "number<=-1.5",
            "large>1e100",
            "arabic<20",
            "styled<20",
        ] {
            assert!(matches(expression, attributes.clone()), "{expression}");
        }
        for expression in ["priority<2", "nan>0", "nan<=0", "number>-1.5", "missing>=0"] {
            assert!(!matches(expression, attributes.clone()), "{expression}");
        }
    }

    #[test]
    fn boolean_grouping_matches_reference_lalr_shift_resolution() {
        let attributes = json!({"a":0,"b":1,"c":1,"AND":1,"OR":2});
        assert!(!matches("a=1 AND b=1 OR c=1", attributes.clone()));
        assert!(matches("(a=1 AND b=1) OR c=1", attributes.clone()));
        assert!(matches("a=1 OR b=1 AND c=1", attributes.clone()));
        assert!(matches("AND=1 AND OR=2", attributes.clone()));
        assert!(matches("a=0ANDb=1", attributes));
    }

    #[test]
    fn long_flat_chains_preserve_right_associativity_without_recursive_evaluation() {
        let all = std::iter::repeat_n("a=1", 4000)
            .collect::<Vec<_>>()
            .join(" AND ");
        assert!(matches(&all, json!({"a": 1})));
        assert!(!matches(&format!("a=0 AND {all} OR a=1"), json!({"a": 1})));
        let mixed = std::iter::repeat_n("a=0 AND a=0 OR ", 1000).collect::<String>() + "a=1";
        assert!(!matches(&mixed, json!({"a": 1})));
    }

    #[test]
    fn malformed_expressions_and_unbounded_recursion_fail_before_evaluation() {
        for expression in [
            " ",
            "name",
            "name ~= \"value\"",
            "name==1",
            "name=bare",
            "name='value'",
            "name=1 and x=2",
            "NOT(name=1)",
            "(name=1",
            "name=1)",
            "name=1 AND",
            "name=1e+",
            "name=\"unterminated",
            "name=\"raw\nnewline\"",
            "name-with-dash=1",
            "α=1",
            "name=1\u{00a0}AND name=1",
        ] {
            assert_eq!(
                AttributeFilter::compile(Some(expression))
                    .err()
                    .unwrap()
                    .kind,
                ErrorKind::InvalidConfig,
                "{expression}"
            );
        }
        let deep = format!("{}name=1{}", "(".repeat(256), ")".repeat(256));
        assert!(AttributeFilter::compile(Some(&deep)).is_err());
        assert!(AttributeFilter::compile(None).unwrap().is_none());
        assert!(AttributeFilter::compile(Some("")).unwrap().is_none());
    }
}
