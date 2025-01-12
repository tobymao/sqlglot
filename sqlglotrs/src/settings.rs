use pyo3::prelude::*;
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;

pub type TokenType = u16;

#[derive(Clone, Debug)]
#[pyclass]
#[cfg_attr(feature = "profiling", derive(serde::Serialize, serde::Deserialize))]
pub struct TokenTypeSettings {
    pub bit_string: TokenType,
    pub break_: TokenType,
    pub dcolon: TokenType,
    pub heredoc_string: TokenType,
    pub raw_string: TokenType,
    pub hex_string: TokenType,
    pub identifier: TokenType,
    pub number: TokenType,
    pub parameter: TokenType,
    pub semicolon: TokenType,
    pub string: TokenType,
    pub var: TokenType,
    pub heredoc_string_alternative: TokenType,
    pub hint: TokenType,
}

#[pymethods]
impl TokenTypeSettings {
    #[new]
    pub fn new(
        bit_string: TokenType,
        break_: TokenType,
        dcolon: TokenType,
        heredoc_string: TokenType,
        raw_string: TokenType,
        hex_string: TokenType,
        identifier: TokenType,
        number: TokenType,
        parameter: TokenType,
        semicolon: TokenType,
        string: TokenType,
        var: TokenType,
        heredoc_string_alternative: TokenType,
        hint: TokenType,
    ) -> Self {
        let token_type_settings = TokenTypeSettings {
            bit_string,
            break_,
            dcolon,
            heredoc_string,
            raw_string,
            hex_string,
            identifier,
            number,
            parameter,
            semicolon,
            string,
            var,
            heredoc_string_alternative,
            hint,
        };

        #[cfg(feature = "profiling")]
        {
            token_type_settings.write_json_to_string();
        }

        token_type_settings
    }
}

#[cfg(feature = "profiling")]
impl TokenTypeSettings {
    pub fn write_json_to_string(&self) {
        let json = serde_json::to_string(self).unwrap();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("benches/token_type_settings.json");
        // Write to file
        std::fs::write(path, &json).unwrap();
    }
}

#[derive(Clone, Debug)]
#[pyclass]
#[cfg_attr(feature = "profiling", derive(serde::Serialize, serde::Deserialize))]
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
    pub tokens_preceding_hint: HashSet<TokenType>,
    pub heredoc_tag_is_identifier: bool,
    pub string_escapes_allowed_in_raw_strings: bool,
    pub nested_comments: bool,
    pub hint_start: String,
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
        tokens_preceding_hint: HashSet<TokenType>,
        heredoc_tag_is_identifier: bool,
        string_escapes_allowed_in_raw_strings: bool,
        nested_comments: bool,
        hint_start: String,
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

        let tokenizer_settings = TokenizerSettings {
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
            tokens_preceding_hint,
            heredoc_tag_is_identifier,
            string_escapes_allowed_in_raw_strings,
            nested_comments,
            hint_start,
        };

        #[cfg(feature = "profiling")]
        {
            tokenizer_settings.write_json_to_string();
        }

        tokenizer_settings
    }
}

#[cfg(feature = "profiling")]
impl TokenizerSettings {
    pub fn write_json_to_string(&self) {
        let json = serde_json::to_string(self).unwrap();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("benches/tokenizer_settings.json");
        // Write to file
        std::fs::write(path, &json).unwrap();
    }
}

#[derive(Clone, Debug)]
#[pyclass]
#[cfg_attr(feature = "profiling", derive(serde::Serialize, serde::Deserialize))]
pub struct TokenizerDialectSettings {
    pub unescaped_sequences: HashMap<String, String>,
    pub identifiers_can_start_with_digit: bool,
    pub numbers_can_be_underscore_separated: bool,
}

#[pymethods]
impl TokenizerDialectSettings {
    #[new]
    pub fn new(
        unescaped_sequences: HashMap<String, String>,
        identifiers_can_start_with_digit: bool,
        numbers_can_be_underscore_separated: bool,
    ) -> Self {
        let settings = TokenizerDialectSettings {
            unescaped_sequences,
            identifiers_can_start_with_digit,
            numbers_can_be_underscore_separated,
        };

        #[cfg(feature = "profiling")]
        {
            settings.write_json_to_string();
        }

        settings
    }
}

#[cfg(feature = "profiling")]
impl TokenizerDialectSettings {
    pub fn write_json_to_string(&self) {
        let json = serde_json::to_string(self).unwrap();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("benches/tokenizer_dialect_settings.json");
        std::fs::write(path, &json).unwrap();
    }
}
