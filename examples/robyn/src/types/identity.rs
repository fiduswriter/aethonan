// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).

use std::collections::HashMap;

use pyo3::{pyclass, pymethods, Borrowed, FromPyObject, PyAny, PyErr, PyResult};

#[pyclass]
#[derive(Debug, Clone)]
pub struct Identity {
    #[pyo3(get, set)]
    claims: HashMap<String, String>,
}

#[pymethods]
impl Identity {
    #[new]
    pub fn new(claims: HashMap<String, String>) -> Self {
        Self { claims }
    }
}

impl FromPyObject<'_, '_> for Identity {
    type Error = PyErr;

    #[inline]
    fn extract(obj: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        Ok(obj.cast::<Identity>()?.try_borrow()?.clone())
    }
}
