use crate::trie::{Trie, TrieResult};
use crate::{Token, TokenType, TokenTypeSettings, TokenizerDialectSettings, TokenizerSettings};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::cmp::{max, min};
use std::ops::Range;
use std::slice::Iter;
use std::usize;

#[derive(Debug)]
pub struct TokenizerError {
    message: String,
    context: String,
}

#[derive(Debug)]
#[pyclass]
pub struct Tokenizer {
    settings: TokenizerSettings,
    token_types: TokenTypeSettings,
    keyword_trie: Trie,
}

#[pymethods]
impl Tokenizer {
    #[new]
    pub fn new(settings: TokenizerSettings, token_types: TokenTypeSettings) -> Tokenizer {
        let mut keyword_trie = Trie::new();
        let single_token_strs: Vec<String> = settings
            .single_tokens
            .keys()
            .map(|s| s.to_string())
            .collect();
        let trie_filter =
            |key: &&String| key.contains(" ") || single_token_strs.iter().any(|t| key.contains(t));

        keyword_trie.add(settings.keywords.keys().filter(trie_filter));
        keyword_trie.add(settings.comments.keys().filter(trie_filter));
        keyword_trie.add(settings.quotes.keys().filter(trie_filter));
        keyword_trie.add(settings.format_strings.keys().filter(trie_filter));

        Tokenizer {
            settings,
            token_types,
            keyword_trie,
        }
    }

    pub fn tokenize(
        &self,
        sql: &str,
        dialect_settings: &TokenizerDialectSettings,
    ) -> Result<Vec<Token>, PyErr> {
        let vec = sql.chars().collect::<Vec<char>>();
        let sql = NonReAllocatedVec {
            vec: &vec,
            range: 0..vec.len(),
        };
        let mut state = TokenizerState::new(
            sql,
            &self.settings,
            &self.token_types,
            dialect_settings,
            &self.keyword_trie,
        );
        state.tokenize().map_err(|e| {
            PyException::new_err(format!("Error tokenizing '{}': {}", e.context, e.message))
        })
    }
}

#[derive(Debug)]
struct NonReAllocatedVec<'a, T> {
    vec: &'a [T],
    range: Range<usize>,
}

impl <'a, T> NonReAllocatedVec<'a, T> {
    

    fn iter(&self) -> Iter<'a, T> {
        self.vec[self.range.clone()].iter()
    }

    fn empty () -> NonReAllocatedVec<'a, T> {
        NonReAllocatedVec {
            vec: &[],
            range: 0..0,
        }
    }

    fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    fn index(&self, start: usize, end: usize) -> NonReAllocatedVec<'a, T> {
        NonReAllocatedVec {
            vec: self.vec,
            range: self.range.start+start..self.range.start+end,
        }
    }

    fn open_ended_index(&self, start: usize) -> NonReAllocatedVec<'a, T> {
        NonReAllocatedVec {
            vec: self.vec,
            range: self.range.start+start..self.range.end,
        }
    }

    fn get(&self, index: usize) -> Option<&T> {
        self.vec.get(index+self.range.start)
    }

    fn is_empty(&self) -> bool {
        self.range.start == self.range.end
    }
}

impl <'a> NonReAllocatedVec<'a, char> {
    fn to_string(&self) -> String {
        self.iter().collect()
    }

    fn trim(&self) -> NonReAllocatedVec<'a, char> {
        let mut start = self.range.start;
        let mut end = self.range.end;
        while start < end && self.vec[start].is_whitespace() {
            start += 1;
        }
        while end > start && self.vec[end-1].is_whitespace() {
            end -= 1;
        }
        NonReAllocatedVec {
            vec: self.vec,
            range: start..end,
        }
    }
}

impl PartialEq<str> for NonReAllocatedVec<'_, char> {
    fn eq(&self, other: &str) -> bool {
        let chars = other.chars();
        self.iter().copied().eq(chars)
    }
}

impl PartialEq<String> for NonReAllocatedVec<'_, char> {
    fn eq(&self, other: &String) -> bool {
        let chars = other.chars();
        self.iter().map(|c| *c).eq(chars)
    }
}

impl PartialEq<&String> for NonReAllocatedVec<'_, char> {
    fn eq(&self, other: &&String) -> bool {
        let chars = other.chars();
        self.iter().map(|c| *c).eq(chars)
    }
}

#[derive(Debug)]
struct TokenizerState<'a> {
    sql: NonReAllocatedVec<'a, char>,
    size: usize,
    tokens: Vec<Token>,
    start: usize,
    current: usize,
    line: usize,
    column: usize,
    comments: Vec<String>,
    is_end: bool,
    current_char: char,
    peek_char: char,
    previous_token_line: Option<usize>,
    keyword_trie: &'a Trie,
    settings: &'a TokenizerSettings,
    dialect_settings: &'a TokenizerDialectSettings,
    token_types: &'a TokenTypeSettings,
}

impl<'a> TokenizerState<'a> {
    fn new(
        sql: NonReAllocatedVec<'a, char>,
        settings: &'a TokenizerSettings,
        token_types: &'a TokenTypeSettings,
        dialect_settings: &'a TokenizerDialectSettings,
        keyword_trie: &'a Trie,
    ) -> TokenizerState<'a> {
        let sql_vec_len = sql.len();
        TokenizerState {
            sql: sql,
            size: sql_vec_len,
            tokens: Vec::new(),
            start: 0,
            current: 0,
            line: 1,
            column: 0,
            comments: Vec::new(),
            is_end: false,
            current_char: '\0',
            peek_char: '\0',
            previous_token_line: None,
            keyword_trie,
            settings,
            dialect_settings,
            token_types,
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
        self.scan(None)?;
        Ok(std::mem::take(&mut self.tokens))
    }

    fn scan(&mut self, until_peek_char: Option<char>) -> Result<(), TokenizerError> {
        while self.size > 0 && !self.is_end {
            let mut current = self.current;

            // Skip spaces here rather than iteratively calling advance() for performance reasons
            while current < self.size {
                let ch = self.char_at(current)?;

                if ch == ' ' || ch == '\t' {
                    current += 1;
                } else {
                    break;
                }
            }

            let offset = if current > self.current {
                current - self.current
            } else {
                1
            };

            self.start = current;
            self.advance(offset as isize)?;

            if self.current_char == '\0' {
                break;
            }

            if !self.settings.white_space.contains_key(&self.current_char) {
                if self.current_char.is_ascii_digit() {
                    self.scan_number()?;
                } else if let Some(identifier_end) =
                    self.settings.identifiers.get(&self.current_char)
                {
                    self.scan_identifier(&identifier_end.to_string())?;
                } else {
                    self.scan_keyword()?;
                }
            }

            if let Some(c) = until_peek_char {
                if self.peek_char == c {
                    break;
                }
            }
        }
        if !self.tokens.is_empty() && !self.comments.is_empty() {
            self.tokens
                .last_mut()
                .unwrap()
                .append_comments(&mut self.comments);
        }
        Ok(())
    }

    fn advance(&mut self, i: isize) -> Result<(), TokenizerError> {
        if Some(&self.token_types.break_) == self.settings.white_space.get(&self.current_char) {
            // Ensures we don't count an extra line if we get a \r\n line break sequence.
            if !(self.current_char == '\r' && self.peek_char == '\n') {
                self.column = 1;
                self.line += 1;
            }
        } else {
            self.column = self.column.wrapping_add_signed(i);
        }

        self.current = self.current.wrapping_add_signed(i);
        self.is_end = self.current >= self.size;
        self.current_char = self.char_at(self.current - 1)?;
        self.peek_char = if self.is_end {
            '\0'
        } else {
            self.char_at(self.current)?
        };
        Ok(())
    }

    fn chars(&'a self, size: usize) -> NonReAllocatedVec<'a, char> {
        let start = self.current - 1;
        let end = start + size;
        if end <= self.size {
            self.sql.index(start, end)
        } else {
            NonReAllocatedVec::empty()
        }
    }

    fn char_at(&self, index: usize) -> Result<char, TokenizerError> {
        self.sql.get(index).copied().ok_or_else(|| {
            self.error(format!(
                "Index {} is out of bound (size {})",
                index, self.size
            ))
        })
    }

    fn text(&self) -> NonReAllocatedVec<'a, char> {
        self.sql.index(self.start, self.current)
    }

    fn add(&mut self, token_type: TokenType, text: Option<String>) -> Result<(), TokenizerError> {
        self.previous_token_line = Some(self.line);

        if !self.comments.is_empty()
            && !self.tokens.is_empty()
            && token_type == self.token_types.semicolon
        {
            self.tokens
                .last_mut()
                .unwrap()
                .append_comments(&mut self.comments);
        }

        self.tokens.push(Token::new(
            token_type,
            text.unwrap_or(self.text().to_string()),
            self.line,
            self.column,
            self.start,
            self.current - 1,
            std::mem::take(&mut self.comments),
        ));

        // If we have either a semicolon or a begin token before the command's token, we'll parse
        // whatever follows the command's token as a string.
        if self.settings.commands.contains(&token_type)
            && self.peek_char != ';'
            && (self.tokens.len() == 1
                || self
                    .settings
                    .command_prefix_tokens
                    .contains(&self.tokens[self.tokens.len() - 2].token_type))
        {
            let start = self.current;
            let tokens_len = self.tokens.len();
            self.scan(Some(';'))?;
            self.tokens.truncate(tokens_len);
            let text = self.sql.index(start, self.current)
                .trim();
            if !text.is_empty() {
                let text = text.iter().collect();
                self.add(self.token_types.string, Some(text))?;
            }
        }
        Ok(())
    }

    fn scan_keyword(&mut self) -> Result<(), TokenizerError> {
        let mut size: usize = 0;
        let mut word: Option<String> = None;
        let mut chars = self.text().to_string();
        let mut current_char = '\0';
        let mut prev_space = false;
        let mut skip;
        let mut is_single_token = chars.len() == 1
            && self
                .settings
                .single_tokens
                .contains_key(&chars.to_string().chars().next().unwrap());

        let (mut trie_result, mut trie_node) =
            self.keyword_trie.root.contains(&&chars.to_uppercase());

        while !chars.is_empty() {
            if let TrieResult::Failed = trie_result {
                break;
            } else if let TrieResult::Exists = trie_result {
                word = Some(chars.to_string());
            }

            let end = self.current + size;
            size += 1;

            if end < self.size {
                current_char = self.char_at(end)?;
                is_single_token =
                    is_single_token || self.settings.single_tokens.contains_key(&current_char);
                let is_space = current_char.is_whitespace();

                if !is_space || !prev_space {
                    if is_space {
                        current_char = ' ';
                    }
                    chars.push(current_char);
                    prev_space = is_space;
                    skip = false;
                } else {
                    skip = true;
                }
            } else {
                current_char = '\0';
                break;
            }

            if skip {
                trie_result = TrieResult::Prefix;
            } else {
                (trie_result, trie_node) =
                    trie_node.contains(&current_char.to_uppercase().collect::<String>());
            }
        }

        if let Some(unwrapped_word) = word {
            if self.scan_string(&unwrapped_word)? {
                return Ok(());
            }
            if self.scan_comment(&unwrapped_word)? {
                return Ok(());
            }
            if prev_space || is_single_token || current_char == '\0' {
                self.advance((size - 1) as isize)?;
                let normalized_word = unwrapped_word.to_uppercase();
                let keyword_token =
                    *self
                        .settings
                        .keywords
                        .get(&normalized_word)
                        .ok_or_else(|| {
                            self.error(format!("Unexpected keyword '{}'", &normalized_word))
                        })?;
                self.add(keyword_token, Some(unwrapped_word))?;
                return Ok(());
            }
        }

        match self.settings.single_tokens.get(&self.current_char) {
            Some(token_type) => self.add(*token_type, Some(self.current_char.to_string())),
            None => self.scan_var(),
        }
    }

    fn scan_comment(&mut self, comment_start: &str) -> Result<bool, TokenizerError> {
        if !self.settings.comments.contains_key(comment_start) {
            return Ok(false);
        }

        let comment_start_line = self.line;
        let comment_start_size = comment_start.len();

        if let Some(comment_end) = self.settings.comments.get(comment_start).unwrap() {
            // Skip the comment's start delimiter.
            self.advance(comment_start_size as isize)?;

            let mut comment_count = 1;
            let comment_end_size = comment_end.len();

            while !self.is_end {
                if self.chars(comment_end_size) == comment_end {
                    comment_count -= 1;
                    if comment_count == 0 {
                        break;
                    }
                }

                self.advance(1)?;

                // Nested comments are allowed by some dialects, e.g. databricks, duckdb, postgres
                if self.settings.nested_comments && !self.is_end && self.chars(comment_start_size) == *comment_start {
                    self.advance(comment_start_size as isize)?;
                    comment_count += 1
                }
            }

            let text = self.text();
            self.comments
                .push(text.index(comment_start_size,text.len() - comment_end_size + 1).to_string());
            self.advance((comment_end_size - 1) as isize)?;
        } else {
            while !self.is_end
                && self.settings.white_space.get(&self.peek_char) != Some(&self.token_types.break_)
            {
                self.advance(1)?;
            }
            self.comments
                .push(self.text().open_ended_index(comment_start_size).to_string());
        }

        if comment_start == self.settings.hint_start
            && self.tokens.last().is_some()
            && self.settings.tokens_preceding_hint.contains(&self.tokens.last().unwrap().token_type) {
            self.add(self.token_types.hint, None)?;
        }

        // Leading comment is attached to the succeeding token, whilst trailing comment to the preceding.
        // Multiple consecutive comments are preserved by appending them to the current comments list.
        if Some(comment_start_line) == self.previous_token_line {
            self.tokens
                .last_mut()
                .unwrap()
                .append_comments(&mut self.comments);
            self.previous_token_line = Some(self.line);
        }

        Ok(true)
    }

    fn scan_string(&mut self, start: &String) -> Result<bool, TokenizerError> {
        let (base, token_type, end) = if let Some(end) = self.settings.quotes.get(start) {
            (None, self.token_types.string, end.clone())
        } else if self.settings.format_strings.contains_key(start) {
            let (ref end, token_type) = self.settings.format_strings.get(start).unwrap();

            if *token_type == self.token_types.hex_string {
                (Some(16), *token_type, end.clone())
            } else if *token_type == self.token_types.bit_string {
                (Some(2), *token_type, end.clone())
            } else if *token_type == self.token_types.heredoc_string {
                self.advance(1)?;

                let tag = if self.current_char.to_string() == *end {
                    String::from("")
                } else {
                    self.extract_string(end, false, true, !self.settings.heredoc_tag_is_identifier)?
                };

                if !tag.is_empty()
                    && self.settings.heredoc_tag_is_identifier
                    && (self.is_end || !self.is_identifier(&tag))
                {
                    if !self.is_end {
                        self.advance(-1)?;
                    }

                    self.advance(-(tag.len() as isize))?;
                    self.add(self.token_types.heredoc_string_alternative, None)?;
                    return Ok(true)
                }

                (None, *token_type, format!("{}{}{}", start, tag, end))
            } else {
                (None, *token_type, end.clone())
            }
        } else {
            return Ok(false);
        };

        self.advance(start.len() as isize)?;
        let text = self.extract_string(&end, false, token_type == self.token_types.raw_string, true)?;

        if let Some(b) = base {
            if u64::from_str_radix(&text, b).is_err() {
                return self.error_result(format!(
                    "Numeric string contains invalid characters from {}:{}",
                    self.line, self.start
                ));
            }
        }

        self.add(token_type, Some(text))?;
        Ok(true)
    }

    fn scan_number(&mut self) -> Result<(), TokenizerError> {
        if self.current_char == '0' {
            let peek_char = self.peek_char.to_ascii_uppercase();
            if peek_char == 'B' {
                if self.settings.has_bit_strings {
                    self.scan_bits()?;
                } else {
                    self.add(self.token_types.number, None)?;
                }
                return Ok(());
            } else if peek_char == 'X' {
                if self.settings.has_hex_strings {
                    self.scan_hex()?;
                } else {
                    self.add(self.token_types.number, None)?;
                }
                return Ok(());
            }
        }

        let mut decimal = false;
        let mut scientific = 0;

        loop {
            if self.peek_char.is_ascii_digit() {
                self.advance(1)?;
            } else if self.peek_char == '.' && !decimal {
                if self.tokens.last().map(|t| t.token_type) == Some(self.token_types.parameter) {
                    return self.add(self.token_types.number, None);
                }
                decimal = true;
                self.advance(1)?;
            } else if (self.peek_char == '-' || self.peek_char == '+') && scientific == 1 {
                scientific += 1;
                self.advance(1)?;
            } else if self.peek_char.to_ascii_uppercase() == 'E' && scientific == 0 {
                scientific += 1;
                self.advance(1)?;
            } else if self.is_alphabetic_or_underscore(self.peek_char) {
                let number_text = self.text();
                let mut literal = String::from("");

                while !self.peek_char.is_whitespace()
                    && !self.is_end
                    && !self.settings.single_tokens.contains_key(&self.peek_char)
                {
                    literal.push(self.peek_char);
                    self.advance(1)?;
                }

                let token_type = self
                    .settings
                    .keywords
                    .get(
                        self.settings
                            .numeric_literals
                            .get(&literal.to_uppercase())
                            .unwrap_or(&String::from("")),
                    )
                    .copied();

                if let Some(unwrapped_token_type) = token_type {
                    self.add(self.token_types.number, Some(number_text.to_string()))?;
                    self.add(self.token_types.dcolon, Some("::".to_string()))?;
                    self.add(unwrapped_token_type, Some(literal))?;
                } else if self.dialect_settings.identifiers_can_start_with_digit {
                    self.add(self.token_types.var, None)?;
                } else {
                    self.advance(-(literal.chars().count() as isize))?;
                    self.add(self.token_types.number, Some(number_text.to_string()))?;
                }
                return Ok(());
            } else {
                return self.add(self.token_types.number, None);
            }
        }
    }

    fn scan_bits(&mut self) -> Result<(), TokenizerError> {
        self.scan_radix_string(2, self.token_types.bit_string)
    }

    fn scan_hex(&mut self) -> Result<(), TokenizerError> {
        self.scan_radix_string(16, self.token_types.hex_string)
    }

    fn scan_radix_string(
        &mut self,
        radix: u32,
        radix_token_type: TokenType,
    ) -> Result<(), TokenizerError> {
        self.advance(1)?;
        let value = self.extract_value()?.open_ended_index(2).to_string();
        match u64::from_str_radix(&value, radix) {
            Ok(_) => self.add(radix_token_type, Some(value)),
            Err(_) => self.add(self.token_types.identifier, None),
        }
    }

    fn scan_var(&mut self) -> Result<(), TokenizerError> {
        loop {
            let peek_char = if !self.peek_char.is_whitespace() {
                self.peek_char
            } else {
                '\0'
            };
            if peek_char != '\0'
                && (self.settings.var_single_tokens.contains(&peek_char)
                    || !self.settings.single_tokens.contains_key(&peek_char))
            {
                self.advance(1)?;
            } else {
                break;
            }
        }

        let token_type =
            if self.tokens.last().map(|t| t.token_type) == Some(self.token_types.parameter) {
                self.token_types.var
            } else {
                self.settings
                    .keywords
                    .get(&self.text().to_string().to_uppercase())
                    .copied()
                    .unwrap_or(self.token_types.var)
            };
        self.add(token_type, None)
    }

    fn scan_identifier(&mut self, identifier_end: &str) -> Result<(), TokenizerError> {
        self.advance(1)?;
        let text = self.extract_string(identifier_end, true, false, true)?;
        self.add(self.token_types.identifier, Some(text))
    }

    fn extract_string(
        &mut self,
        delimiter: &str,
        use_identifier_escapes: bool,
        raw_string: bool,
        raise_unmatched: bool,
    ) -> Result<String, TokenizerError> {
        let mut text = String::from("");

        loop {
            let mut new_identifier_escapes;
            let escapes = if use_identifier_escapes {
                new_identifier_escapes = self.settings.identifier_escapes.clone();
                new_identifier_escapes.extend(delimiter.chars());
                &new_identifier_escapes
            } else {
                &self.settings.string_escapes
            };
            let peek_char_str = self.peek_char.to_string();

            if !raw_string
                && !self.dialect_settings.unescaped_sequences.is_empty()
                && !self.peek_char.is_whitespace()
                && self.settings.string_escapes.contains(&self.current_char)
            {
                let sequence_key = format!("{}{}", self.current_char, self.peek_char);
                if let Some(unescaped_sequence) =
                    self.dialect_settings.unescaped_sequences.get(&sequence_key)
                {
                    self.advance(2)?;
                    text.push_str(unescaped_sequence);
                    continue;
                }
            }

            if (self.settings.string_escapes_allowed_in_raw_strings || !raw_string)
                && escapes.contains(&self.current_char)
                && (peek_char_str == delimiter || escapes.contains(&self.peek_char))
                && (self.current_char == self.peek_char
                    || !self
                        .settings
                        .quotes
                        .contains_key(&self.current_char.to_string()))
            {
                if peek_char_str == delimiter {
                    text.push(self.peek_char);
                } else {
                    text.push(self.current_char);
                    text.push(self.peek_char);
                }
                if self.current + 1 < self.size {
                    self.advance(2)?;
                } else {
                    return self.error_result(format!(
                        "Missing {} from {}:{}",
                        delimiter, self.line, self.current
                    ));
                }
            } else {
                if self.chars(delimiter.len()) == delimiter.to_string() {
                    if delimiter.len() > 1 {
                        self.advance((delimiter.len() - 1) as isize)?;
                    }
                    break;
                }
                if self.is_end {
                    if !raise_unmatched {
                        text.push(self.current_char);
                        return Ok(text)
                    }

                    return self.error_result(format!(
                        "Missing {} from {}:{}",
                        delimiter, self.line, self.current
                    ));
                }

                let current = self.current - 1;
                self.advance(1)?;
                text.push_str(
                    &self.sql.index(current, self.current - 1)
                        .iter()
                        .collect::<String>(),
                );
            }
        }
        Ok(text)
    }

    fn is_alphabetic_or_underscore(&mut self, name: char) -> bool {
        name.is_alphabetic() || name == '_'
    }

    fn is_identifier(&mut self, s: &str) -> bool {
        s.chars().enumerate().all(
            |(i, c)|
            if i == 0 { self.is_alphabetic_or_underscore(c) }
            else { self.is_alphabetic_or_underscore(c) || c.is_ascii_digit() }
        )
    }

    fn extract_value(&mut self) -> Result<NonReAllocatedVec<'a, char>, TokenizerError> {
        loop {
            if !self.peek_char.is_whitespace()
                && !self.is_end
                && !self.settings.single_tokens.contains_key(&self.peek_char)
            {
                self.advance(1)?;
            } else {
                break;
            }
        }
        Ok(self.text())
    }

    fn error(&self, message: String) -> TokenizerError {
        let start = max((self.current as isize) - 50, 0);
        let end = min(self.current + 50, self.size - 1);
        let context = self.sql.index(start as usize, end).iter().collect::<String>();
        TokenizerError { message, context }
    }

    fn error_result<T>(&self, message: String) -> Result<T, TokenizerError> {
        Err(self.error(message))
    }
}
