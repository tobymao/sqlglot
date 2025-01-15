use crate::settings::TokenType;
use pyo3::types::{PyNone};
use pyo3::{pyclass, pymethods, IntoPy, PyObject, Python};

#[derive(Debug)]
#[pyclass]
pub struct Token {
    #[pyo3(get)]
    pub token_type_index: TokenType,
    pub token_type: Option<PyObject>,
    #[pyo3(get)]
    pub text: String,
    #[pyo3(get)]
    pub line: usize,
    #[pyo3(get)]
    pub col: usize,
    #[pyo3(get)]
    pub start: usize,
    #[pyo3(get)]
    pub end: usize,
    pub comments: Vec<String>,
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
        Token {
            token_type_index: token_type,
            token_type: None,
            text,
            line,
            col,
            start,
            end,
            comments,
        }
    }

    pub fn append_comments(&mut self, new_comments: &mut Vec<String>) {
        self.comments.append(new_comments);
    }
}

#[pymethods]
impl Token {
    #[getter(comments)]
    fn comments(&self) -> Vec<String> {
        self.comments.clone()
    }

    #[getter(text)]
    fn text(&self) -> &str {
        &self.text
    }

    #[getter(token_type)]
    fn token_type(&self, py: Python) -> PyObject {
        match &self.token_type {
            Some(token_type) => token_type.clone_ref(py),
            None => PyNone::get_bound(py).into_py(py),
        }
    }

    #[setter(token_type)]
    fn set_token_type(&mut self, token_type: PyObject) {
        self.token_type = Some(token_type);
    }
}
