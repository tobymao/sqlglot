use crate::TokenType;
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

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
}

#[pymethods]
impl TokenizerSettings {
    #[new]
    pub fn new(
        white_space: HashMap<String, String>,
        single_tokens: HashMap<String, String>,
        keywords: HashMap<String, String>,
        numeric_literals: HashMap<String, String>,
        identifiers: HashMap<String, String>,
        identifier_escapes: HashSet<String>,
        string_escapes: HashSet<String>,
        quotes: HashMap<String, String>,
        format_strings: HashMap<String, (String, String)>,
        has_bit_strings: bool,
        has_hex_strings: bool,
        comments: HashMap<String, Option<String>>,
        var_single_tokens: HashSet<String>,
        commands: HashSet<String>,
        command_prefix_tokens: HashSet<String>,
    ) -> Self {
        let to_token_type = |v: &String| match TokenType::from_str(v) {
            Ok(t) => t,
            Err(_) => panic!("Invalid token type: {}", v),
        };

        let to_char = |v: &String| {
            if v.len() == 1 {
                v.chars().next().unwrap()
            } else {
                panic!("Invalid char: {}", v)
            }
        };

        let white_space_native: HashMap<char, TokenType> = white_space
            .iter()
            .map(|(k, v)| (to_char(k), to_token_type(v)))
            .collect();

        let single_tokens_native: HashMap<char, TokenType> = single_tokens
            .iter()
            .map(|(k, v)| (to_char(k), to_token_type(v)))
            .collect();

        let keywords_native: HashMap<String, TokenType> = keywords
            .into_iter()
            .map(|(k, v)| (k, to_token_type(&v)))
            .collect();

        let identifiers_native: HashMap<char, char> = identifiers
            .iter()
            .map(|(k, v)| (to_char(k), to_char(v)))
            .collect();

        let identifier_escapes_native: HashSet<char> =
            identifier_escapes.iter().map(&to_char).collect();

        let string_escapes_native: HashSet<char> = string_escapes.iter().map(&to_char).collect();

        let format_strings_native: HashMap<String, (String, TokenType)> = format_strings
            .into_iter()
            .map(|(k, (v1, v2))| (k, (v1, to_token_type(&v2))))
            .collect();

        let var_single_tokens_native: HashSet<char> =
            var_single_tokens.iter().map(&to_char).collect();

        let commands_native: HashSet<TokenType> = commands.iter().map(&to_token_type).collect();

        let command_prefix_tokens_native: HashSet<TokenType> =
            command_prefix_tokens.iter().map(&to_token_type).collect();

        TokenizerSettings {
            white_space: white_space_native,
            single_tokens: single_tokens_native,
            keywords: keywords_native,
            numeric_literals,
            identifiers: identifiers_native,
            identifier_escapes: identifier_escapes_native,
            string_escapes: string_escapes_native,
            quotes,
            format_strings: format_strings_native,
            has_bit_strings,
            has_hex_strings,
            comments,
            var_single_tokens: var_single_tokens_native,
            commands: commands_native,
            command_prefix_tokens: command_prefix_tokens_native,
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
