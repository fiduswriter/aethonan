// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).

use anyhow::{anyhow, Result};
use hyper::http::request::Parts;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use pyo3::{exceptions::PyValueError, prelude::*, IntoPyObject};
use serde_json::Value;
use std::collections::HashMap;

use crate::types::{check_body_type, get_body_from_pyobject, Url};

use super::{headers::Headers, identity::Identity, multimap::QueryParams};

#[derive(Default, Debug, FromPyObject)]
pub struct Request {
    pub query_params: QueryParams,
    pub headers: Headers,
    pub method: String,
    pub path_params: HashMap<String, String>,
    // https://pyo3.rs/v0.19.2/function.html?highlight=from_py_#per-argument-options
    #[pyo3(from_py_with = get_body_from_pyobject)]
    pub body: Vec<u8>,
    pub url: Url,
    pub ip_addr: Option<String>,
    pub identity: Option<Identity>,
    pub form_data: Option<HashMap<String, String>>,
    pub files: Option<HashMap<String, Vec<u8>>>,
    // A Python session object (robyn.session.Session) shared by reference across
    // the before_request / handler / after_request phases, so in-handler mutations
    // are visible when the cookie is written back. Set by configure_sessions().
    pub session: Option<Py<PyAny>>,
}

impl Clone for Request {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            query_params: self.query_params.clone(),
            headers: self.headers.clone(),
            method: self.method.clone(),
            path_params: self.path_params.clone(),
            body: self.body.clone(),
            url: self.url.clone(),
            ip_addr: self.ip_addr.clone(),
            identity: self.identity.clone(),
            form_data: self.form_data.clone(),
            files: self.files.clone(),
            session: self.session.as_ref().map(|session| session.clone_ref(py)),
        })
    }
}

impl<'py> IntoPyObject<'py> for Request {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let headers: Py<Headers> = self.headers.into_pyobject(py)?.extract()?;
        let path_params = self.path_params.into_pyobject(py)?.extract()?;

        let body = if self.body.is_empty() {
            PyString::new(py, "").into_any()
        } else {
            match std::str::from_utf8(&self.body) {
                Ok(s) => PyString::new(py, s).into_any(),
                Err(_) => PyBytes::new(py, &self.body).into_any(),
            }
        };

        let form_data: Py<PyDict> = match self.form_data {
            Some(data) if !data.is_empty() => {
                let dict = PyDict::new(py);
                // Use with_capacity equivalent by setting items directly
                for (key, value) in data {
                    dict.set_item(key, value)?;
                }
                dict.into()
            }
            _ => PyDict::new(py).into(),
        };

        let files: Py<PyDict> = match self.files {
            Some(data) if !data.is_empty() => {
                let dict = PyDict::new(py);
                for (key, value) in data {
                    let bytes = PyBytes::new(py, &value);
                    dict.set_item(key, bytes)?;
                }
                dict.into()
            }
            _ => PyDict::new(py).into(),
        };

        let request = PyRequest {
            query_params: self.query_params,
            path_params,
            headers,
            body: body.into(),
            method: self.method,
            url: self.url,
            ip_addr: self.ip_addr,
            identity: self.identity,
            form_data,
            files,
            session: self.session,
        };
        Ok(Py::new(py, request)?.into_bound(py).into_any())
    }
}

async fn handle_multipart(
    body: &[u8],
    boundary: &str,
    files: &mut HashMap<String, Vec<u8>>,
    form_data: &mut HashMap<String, String>,
    output_body: &mut Vec<u8>,
) -> Result<()> {
    let stream = futures::stream::iter(std::iter::once(Ok::<
        hyper::body::Bytes,
        multer::Error,
    >(hyper::body::Bytes::copy_from_slice(body))));
    let mut multipart = multer::Multipart::new(stream, boundary);

    // Iterate over multipart fields
    while let Some(field) = multipart.next_field().await? {
        let field_name = field.name().unwrap_or_default().to_string();
        let file_name = field.file_name().map(|s| s.to_string());
        let data = field.bytes().await?.to_vec();

        output_body.extend_from_slice(&data);

        if let Some(name) = file_name {
            files.insert(name, data);
        } else if let Ok(text) = String::from_utf8(data) {
            form_data.insert(field_name, text);
        }
    }

    Ok(())
}

/// Extract the multipart boundary from a `content-type` header value.
pub fn multipart_boundary(content_type: &str) -> Option<String> {
    content_type.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        if key.eq_ignore_ascii_case("boundary") {
            Some(value.trim_matches('"').to_string())
        } else {
            None
        }
    })
}

impl Request {
    /// Port of `from_actix_request`: build a Robyn `Request` from the parts and
    /// the (already collected) body of a hyper request, as handed over by
    /// Aethonan's native route handler.
    pub async fn from_hyper_request(
        parts: &Parts,
        body: Vec<u8>,
        global_headers: &Headers,
        scheme: &str,
        host: &str,
        ip_addr: Option<String>,
    ) -> Result<Self> {
        let mut query_params: QueryParams = QueryParams::new();
        let mut form_data: HashMap<String, String> = HashMap::new();
        let mut files = HashMap::new();

        let query_string = parts.uri.query().unwrap_or_default();
        if !query_string.is_empty() {
            let split = query_string.split('&');
            for s in split {
                let path_params = s.split_once('=').unwrap_or((s, ""));
                let key = path_params.0.to_string();
                let value = path_params.1.to_string();

                query_params.set(key, value);
            }
        }

        let mut headers = Headers::from_hyper_headers(&parts.headers);
        headers.extend(global_headers);

        let body: Vec<u8> = if headers.contains(String::from("content-type"))
            && headers
                .get(String::from("content-type"))
                .is_some_and(|val| val.contains("multipart/form-data"))
        {
            let content_type = headers.get(String::from("content-type")).unwrap();
            let boundary = multipart_boundary(&content_type)
                .ok_or_else(|| anyhow!("missing multipart boundary"))?;
            let mut body_local: Vec<u8> = Vec::new();

            handle_multipart(&body, &boundary, &mut files, &mut form_data, &mut body_local).await?;

            body_local
        } else {
            body
        };

        let route_path = {
            let mut path = parts.uri.path();
            if path.ends_with('/') && path.len() > 1 {
                path = &path[..path.len() - 1]
            }
            path
        };

        let url = Url::new(scheme, host, route_path);

        Ok(Self {
            query_params,
            headers,
            method: parts.method.as_str().to_owned(),
            path_params: HashMap::new(),
            body,
            url,
            ip_addr,
            identity: None,
            form_data: Some(form_data),
            files: Some(files),
            session: None,
        })
    }
}

#[pyclass(name = "Request")]
pub struct PyRequest {
    #[pyo3(get, set)]
    pub query_params: QueryParams,
    #[pyo3(get, set)]
    pub headers: Py<Headers>,
    #[pyo3(get, set)]
    pub path_params: Py<PyDict>,
    #[pyo3(get, set)]
    pub identity: Option<Identity>,
    #[pyo3(get)]
    pub body: Py<PyAny>,
    #[pyo3(get)]
    pub method: String,
    #[pyo3(get)]
    pub url: Url,
    #[pyo3(get)]
    pub ip_addr: Option<String>,
    #[pyo3(get, set)]
    pub form_data: Py<PyDict>,
    #[pyo3(get, set)]
    pub files: Py<PyDict>,
    #[pyo3(get, set)]
    pub session: Option<Py<PyAny>>,
}

impl Clone for PyRequest {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            query_params: self.query_params.clone(),
            headers: self.headers.clone_ref(py),
            path_params: self.path_params.clone_ref(py),
            identity: self.identity.clone(),
            body: self.body.clone_ref(py),
            method: self.method.clone(),
            url: self.url.clone(),
            ip_addr: self.ip_addr.clone(),
            form_data: self.form_data.clone_ref(py),
            files: self.files.clone_ref(py),
            session: self.session.as_ref().map(|session| session.clone_ref(py)),
        })
    }
}

#[pymethods]
impl PyRequest {
    #[new]
    #[pyo3(signature = (query_params, headers, path_params, body, method, url, form_data, files, identity, ip_addr, session=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_params: QueryParams,
        headers: Py<Headers>,
        path_params: Py<PyDict>,
        body: Py<PyAny>,
        method: String,
        url: Url,
        form_data: Py<PyDict>,
        files: Py<PyDict>,
        identity: Option<Identity>,
        ip_addr: Option<String>,
        session: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            query_params,
            headers,
            path_params,
            identity,
            body,
            method,
            url,
            form_data,
            files,
            ip_addr,
            session,
        }
    }

    #[setter]
    pub fn set_body(&mut self, py: Python, body: Py<PyAny>) -> PyResult<()> {
        check_body_type(py, &body)?;
        self.body = body;
        Ok(())
    }

    pub fn json(&self, py: Python) -> PyResult<Py<PyAny>> {
        let parsed: Value = if let Ok(python_string) = self.body.bind(py).cast::<PyString>() {
            serde_json::from_str(python_string.extract()?)
        } else if let Ok(python_bytes) = self.body.bind(py).cast::<PyBytes>() {
            serde_json::from_slice(python_bytes.as_bytes())
        } else {
            return Err(PyValueError::new_err("Invalid JSON body"));
        }
        .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {}", e)))?;

        json_value_to_py(py, &parsed)
    }
}

/// Maximum allowed recursion depth for JSON parsing to prevent stack overflow attacks.
const MAX_JSON_DEPTH: usize = 128;

/// Converts a serde_json::Value to a Python object with proper type preservation.
/// This is a convenience wrapper that starts recursion with MAX_JSON_DEPTH.
fn json_value_to_py(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    json_value_to_py_with_depth(py, value, MAX_JSON_DEPTH)
}

/// Converts a serde_json::Value to a Python object with recursion depth limiting.
fn json_value_to_py_with_depth(py: Python, value: &Value, depth: usize) -> PyResult<Py<PyAny>> {
    if depth == 0 {
        return Err(PyValueError::new_err(
            "JSON nesting depth exceeds maximum allowed limit",
        ));
    }

    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Err(PyValueError::new_err("Invalid number in JSON"))
            }
        }
        Value::String(s) => Ok(s.as_str().into_pyobject(py)?.into_any().unbind()),
        Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_value_to_py_with_depth(py, item, depth - 1)?)?;
            }
            Ok(list.into_pyobject(py)?.into_any().unbind())
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_value_to_py_with_depth(py, v, depth - 1)?)?;
            }
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
    }
}
