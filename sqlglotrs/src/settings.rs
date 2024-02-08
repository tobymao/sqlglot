use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};

pub type TokenType = u16;

#[derive(Clone, Debug)]
#[pyclass]
pub struct TokenTypeSettings {
    pub bit_string: TokenType,
    pub break_: TokenType,
    pub dcolon: TokenType,
    pub heredoc_string: TokenType,
    pub hex_string: TokenType,
    pub identifier: TokenType,
    pub number: TokenType,
    pub parameter: TokenType,
    pub semicolon: TokenType,
    pub string: TokenType,
    pub var: TokenType,
    pub heredoc_string_alternative: TokenType,
}

#[pymethods]
impl TokenTypeSettings {
    #[new]
    pub fn new(
        bit_string: TokenType,
        break_: TokenType,
        dcolon: TokenType,
        heredoc_string: TokenType,
        hex_string: TokenType,
        identifier: TokenType,
        number: TokenType,
        parameter: TokenType,
        semicolon: TokenType,
        string: TokenType,
        var: TokenType,
        heredoc_string_alternative: TokenType,
    ) -> Self {
        TokenTypeSettings {
            bit_string,
            break_,
            dcolon,
            heredoc_string,
            hex_string,
            identifier,
            number,
            parameter,
            semicolon,
            string,
            var,
            heredoc_string_alternative,
        }
    }
}

#[derive(Clone, Debug)]
#[pyclass]
pub struct TokenizerSettings {
    pub white_space: HashMap<char, TokenType>,
    pub single_tokens: HashMap<char, TokenType>,
    pub keywords: HashMap<String, TokenType>,
    pub numeric_literals: HashMap<String, String>,
    pub identifiers: HashMap<char, char>,
    pub identifier_escapes: HashSet<char>,
    pub string_escapes: HashSet<char>,
    pub quotes: HashMap<String, String>,
    pub format_strings: HashMap<String, (String, TokenType)>,
    pub has_bit_strings: bool,
    pub has_hex_strings: bool,
    pub comments: HashMap<String, Option<String>>,
    pub var_single_tokens: HashSet<char>,
    pub commands: HashSet<TokenType>,
    pub command_prefix_tokens: HashSet<TokenType>,
    pub heredoc_tag_is_identifier: bool,
}

#[pymethods]
impl TokenizerSettings {
    #[new]
    pub fn new(
        white_space: HashMap<String, TokenType>,
        single_tokens: HashMap<String, TokenType>,
        keywords: HashMap<String, TokenType>,
        numeric_literals: HashMap<String, String>,
        identifiers: HashMap<String, String>,
        identifier_escapes: HashSet<String>,
        string_escapes: HashSet<String>,
        quotes: HashMap<String, String>,
        format_strings: HashMap<String, (String, TokenType)>,
        has_bit_strings: bool,
        has_hex_strings: bool,
        comments: HashMap<String, Option<String>>,
        var_single_tokens: HashSet<String>,
        commands: HashSet<TokenType>,
        command_prefix_tokens: HashSet<TokenType>,
        heredoc_tag_is_identifier: bool,
    ) -> Self {
        let to_char = |v: &String| {
            if v.len() == 1 {
                v.chars().next().unwrap()
            } else {
                panic!("Invalid char: {}", v)
            }
        };

        let white_space_native: HashMap<char, TokenType> = white_space
            .into_iter()
            .map(|(k, v)| (to_char(&k), v))
            .collect();

        let single_tokens_native: HashMap<char, TokenType> = single_tokens
            .into_iter()
            .map(|(k, v)| (to_char(&k), v))
            .collect();

        let identifiers_native: HashMap<char, char> = identifiers
            .iter()
            .map(|(k, v)| (to_char(k), to_char(v)))
            .collect();

        let identifier_escapes_native: HashSet<char> =
            identifier_escapes.iter().map(&to_char).collect();

        let string_escapes_native: HashSet<char> = string_escapes.iter().map(&to_char).collect();

        let var_single_tokens_native: HashSet<char> =
            var_single_tokens.iter().map(&to_char).collect();

        TokenizerSettings {
            white_space: white_space_native,
            single_tokens: single_tokens_native,
            keywords,
            numeric_literals,
            identifiers: identifiers_native,
            identifier_escapes: identifier_escapes_native,
            string_escapes: string_escapes_native,
            quotes,
            format_strings,
            has_bit_strings,
            has_hex_strings,
            comments,
            var_single_tokens: var_single_tokens_native,
            commands,
            command_prefix_tokens,
            heredoc_tag_is_identifier,
        }
    }
}

#[derive(Clone, Debug)]
#[pyclass]
pub struct TokenizerDialectSettings {
    pub escape_sequences: HashMap<String, String>,
    pub identifiers_can_start_with_digit: bool,
}

#[pymethods]
impl TokenizerDialectSettings {
    #[new]
    pub fn new(
        escape_sequences: HashMap<String, String>,
        identifiers_can_start_with_digit: bool,
    ) -> Self {
        TokenizerDialectSettings {
            escape_sequences,
            identifiers_can_start_with_digit,
        }
    }
}
