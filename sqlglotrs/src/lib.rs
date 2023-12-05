use pyo3::prelude::*;

mod settings;
mod token;
mod tokenizer;
mod trie;

pub use self::settings::TokenizerSettings;
pub use self::token::{Token, TokenType};
pub use self::tokenizer::Tokenizer;

#[pymodule]
fn sqlglotrs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Token>()?;
    m.add_class::<TokenType>()?;
    m.add_class::<TokenizerSettings>()?;
    m.add_class::<Tokenizer>()?;
    Ok(())
}
