// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).

use log::debug;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyString},
};

pub mod cookie;
pub mod function_info;
pub mod headers;
pub mod identity;
pub mod multimap;
pub mod request;
pub mod response;

#[allow(clippy::large_enum_variant)]
pub enum MiddlewareReturn {
    Request(request::Request),
    Response(response::Response),
}

#[pyclass]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
    HEAD,
    OPTIONS,
    CONNECT,
    TRACE,
}

impl HttpMethod {
    pub fn is_supported(method: &hyper::Method) -> bool {
        matches!(
            *method,
            hyper::Method::GET
                | hyper::Method::POST
                | hyper::Method::PUT
                | hyper::Method::DELETE
                | hyper::Method::PATCH
                | hyper::Method::HEAD
                | hyper::Method::OPTIONS
                | hyper::Method::CONNECT
                | hyper::Method::TRACE
        )
    }

    /// Port of `from_actix_method`: map a hyper/http method onto the supported set.
    pub fn from_hyper_method(method: &hyper::Method) -> Result<Self, &'static str> {
        match *method {
            hyper::Method::GET => Ok(Self::GET),
            hyper::Method::POST => Ok(Self::POST),
            hyper::Method::PUT => Ok(Self::PUT),
            hyper::Method::DELETE => Ok(Self::DELETE),
            hyper::Method::PATCH => Ok(Self::PATCH),
            hyper::Method::HEAD => Ok(Self::HEAD),
            hyper::Method::OPTIONS => Ok(Self::OPTIONS),
            hyper::Method::CONNECT => Ok(Self::CONNECT),
            hyper::Method::TRACE => Ok(Self::TRACE),
            _ => Err("Method Not Allowed"),
        }
    }
}

// https://github.com/sparckles/Robyn/pull/987#discussion_r2191722539
impl std::fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HttpMethod::GET => write!(f, "GET"),
            HttpMethod::POST => write!(f, "POST"),
            HttpMethod::PUT => write!(f, "PUT"),
            HttpMethod::DELETE => write!(f, "DELETE"),
            HttpMethod::PATCH => write!(f, "PATCH"),
            HttpMethod::HEAD => write!(f, "HEAD"),
            HttpMethod::OPTIONS => write!(f, "OPTIONS"),
            HttpMethod::CONNECT => write!(f, "CONNECT"),
            HttpMethod::TRACE => write!(f, "TRACE"),
        }
    }
}

#[pyclass]
#[derive(Default, Debug, Clone)]
pub struct Url {
    #[pyo3(get)]
    pub scheme: String,
    #[pyo3(get)]
    pub host: String,
    #[pyo3(get)]
    pub path: String,
}

#[pymethods]
impl Url {
    #[new]
    pub fn new(scheme: &str, host: &str, path: &str) -> Self {
        Self {
            scheme: scheme.to_string(),
            host: host.to_string(),
            path: path.to_string(),
        }
    }
}

pub fn get_body_from_pyobject(body: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(s) = body.cast::<PyString>() {
        Ok(s.to_string().into_bytes())
    } else if let Ok(b) = body.cast::<PyBytes>() {
        Ok(b.as_bytes().to_vec())
    } else {
        debug!("Could not convert specified body to bytes");
        Ok(vec![])
    }
}

pub fn get_description_from_pyobject(description: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(s) = description.cast::<PyString>() {
        Ok(s.to_string().into_bytes())
    } else if let Ok(b) = description.cast::<PyBytes>() {
        Ok(b.as_bytes().to_vec())
    } else {
        debug!("Could not convert specified response description to bytes");
        Ok(vec![])
    }
}

pub fn check_body_type(py: Python, body: &Py<PyAny>) -> PyResult<()> {
    if body.bind(py).cast::<PyString>().is_err() && body.bind(py).cast::<PyBytes>().is_err() {
        return Err(PyValueError::new_err(
            "Could not convert specified body to bytes",
        ));
    };
    Ok(())
}

pub fn check_description_type(py: Python, body: &Py<PyAny>) -> PyResult<()> {
    if body.bind(py).cast::<PyString>().is_err() && body.bind(py).cast::<PyBytes>().is_err() {
        return Err(PyValueError::new_err(
            "Could not convert specified response description to bytes",
        ));
    };
    Ok(())
}

impl FromPyObject<'_, '_> for Url {
    type Error = PyErr;

    #[inline]
    fn extract(obj: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        Ok(obj.cast::<Url>()?.try_borrow()?.clone())
    }
}
