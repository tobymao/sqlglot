use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};

mod settings;
mod tokenizer;
mod trie;

pub use self::settings::{
    TokenType, TokenTypeSettings, TokenizerDialectSettings, TokenizerSettings,
};
pub use self::tokenizer::Tokenizer;

#[derive(Debug)]
#[pyclass]
pub struct Token {
    #[pyo3(get)]
    pub token_type: TokenType,
    #[pyo3(get)]
    pub text: Py<PyString>,
    #[pyo3(get)]
    pub line: usize,
    #[pyo3(get)]
    pub col: usize,
    #[pyo3(get)]
    pub start: usize,
    #[pyo3(get)]
    pub end: usize,
    #[pyo3(get)]
    pub comments: Py<PyList>,
}

impl Token {
    pub fn new(
        token_type: TokenType,
        text: String,
        line: usize,
        col: usize,
        start: usize,
        end: usize,
        comments: Vec<String>,
    ) -> Token {
        Python::with_gil(|py| Token {
            token_type,
            text: PyString::new(py, &text).into(),
            line,
            col,
            start,
            end,
            comments: PyList::new(py, &comments).into(),
        })
    }

    pub fn append_comments(&self, comments: &mut Vec<String>) {
        Python::with_gil(|py| {
            let pylist = self.comments.as_ref(py);
            for comment in comments.iter() {
                if let Err(_) = pylist.append(comment) {
                    panic!("Failed to append comments to the Python list");
                }
            }
        });
        // Simulate `Vec::append`.
        let _ = std::mem::replace(comments, Vec::new());
    }
}

#[pymethods]
impl Token {
    #[pyo3(name = "__repr__")]
    fn python_repr(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

#[pymodule]
fn sqlglotrs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Token>()?;
    m.add_class::<TokenTypeSettings>()?;
    m.add_class::<TokenizerSettings>()?;
    m.add_class::<TokenizerDialectSettings>()?;
    m.add_class::<Tokenizer>()?;
    Ok(())
}
