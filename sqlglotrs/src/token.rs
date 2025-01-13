use crate::settings::TokenType;
use pyo3::prelude::PyListMethods;
use pyo3::types::{PyList, PyNone, PyString};
use pyo3::{pyclass, IntoPy, Py, PyObject, Python};

#[derive(Debug)]
#[pyclass]
pub struct Token {
    #[pyo3(get, name = "token_type_index")]
    pub token_type: TokenType,
    #[pyo3(get, set, name = "token_type")]
    pub token_type_py: PyObject,
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
            token_type_py: PyNone::get_bound(py).into_py(py),
            text: PyString::new_bound(py, &text).into_py(py),
            line,
            col,
            start,
            end,
            comments: PyList::new_bound(py, &comments).into(),
        })
    }

    pub fn append_comments(&self, comments: &mut Vec<String>) {
        Python::with_gil(|py| {
            let pylist = self.comments.bind(py);
            for comment in comments.drain(..) {
                if let Err(_) = pylist.append(comment) {
                    panic!("Failed to append comments to the Python list");
                }
            }
        });
    }
}
