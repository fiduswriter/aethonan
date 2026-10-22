// Vendored from Robyn (https://github.com/sparckles/Robyn) commit 1170748e — BSD 2-Clause (c) 2021 Sanskar Jethi.
// Ported from actix-web to the Aethonan rlib (Aethonan: Fidus Writer fork of Granian).

use aethonan::HTTPResponse;
use futures::{Stream, StreamExt};
use http_body_util::BodyExt;
use hyper::body::Bytes;
use pyo3::{
    exceptions::{PyIOError, PyTypeError},
    prelude::*,
    types::{PyBytes, PyDict, PyEllipsis},
    Bound, IntoPyObject,
};
use std::pin::Pin;
use tokio;

use crate::io_helpers::read_file;
use crate::types::{check_body_type, check_description_type, get_description_from_pyobject};

use super::cookie::{Cookie, Cookies};
use super::headers::Headers;

#[derive(Debug, Clone)]
pub struct Response {
    pub status_code: u16,
    pub response_type: String,
    pub headers: Headers,
    pub description: Vec<u8>,
    pub file_path: Option<String>,
    pub cookies: Cookies,
}

#[derive(Debug)]
pub struct StreamingResponse {
    pub status_code: u16,
    pub headers: Headers,
    pub content_generator: Py<PyAny>,
}

#[derive(Debug)]
pub enum ResponseType {
    Standard(Response),
    Streaming(StreamingResponse),
}

impl ResponseType {
    pub fn headers_mut(&mut self) -> &mut Headers {
        match self {
            ResponseType::Standard(response) => &mut response.headers,
            ResponseType::Streaming(streaming_response) => &mut streaming_response.headers,
        }
    }

    /// Port of the actix `Responder` impls: render the Robyn response into an
    /// Aethonan/hyper response.
    pub fn to_hyper_response(self) -> HTTPResponse {
        match self {
            ResponseType::Standard(response) => response.to_hyper_response(),
            ResponseType::Streaming(streaming_response) => streaming_response.to_hyper_response(),
        }
    }
}

fn response_builder(status_code: u16) -> hyper::http::response::Builder {
    hyper::Response::builder().status(
        hyper::StatusCode::from_u16(status_code).unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR),
    )
}

impl Response {
    fn default_text_headers() -> Headers {
        let mut headers = Headers::new(None);
        headers.set("Content-Type".to_string(), "text/plain".to_string());
        headers
    }

    pub fn not_found(headers: Option<&Headers>) -> Self {
        const NOT_FOUND_BYTES: &[u8] = b"Not found";

        Self {
            status_code: 404,
            response_type: "text".to_string(),
            headers: headers.cloned().unwrap_or_else(Self::default_text_headers),
            description: NOT_FOUND_BYTES.to_vec(),
            file_path: None,
            cookies: Cookies::new(),
        }
    }

    pub fn internal_server_error(headers: Option<&Headers>) -> Self {
        const SERVER_ERROR_BYTES: &[u8] = b"Internal server error";

        Self {
            status_code: 500,
            response_type: "text".to_string(),
            headers: headers.cloned().unwrap_or_else(Self::default_text_headers),
            description: SERVER_ERROR_BYTES.to_vec(),
            file_path: None,
            cookies: Cookies::new(),
        }
    }

    pub fn method_not_allowed(headers: Option<&Headers>) -> Self {
        const METHOD_NOT_ALLOWED_BYTES: &[u8] = b"Method not allowed";

        Self {
            status_code: 405,
            response_type: "text".to_string(),
            headers: headers.cloned().unwrap_or_else(Self::default_text_headers),
            description: METHOD_NOT_ALLOWED_BYTES.to_vec(),
            file_path: None,
            cookies: Cookies::new(),
        }
    }
}

impl Response {
    /// Port of the actix `Responder for Response` impl.
    pub fn to_hyper_response(self) -> HTTPResponse {
        let mut builder = response_builder(self.status_code);
        {
            let hyper_headers = builder.headers_mut().unwrap();
            crate::io_helpers::apply_hashmap_headers(hyper_headers, &self.headers);
        }

        // Apply cookies as Set-Cookie headers
        for (name, cookie) in &self.cookies.cookies {
            match cookie.to_header_value(name) {
                Ok(header_value) => {
                    if let Ok(value) = hyper::http::HeaderValue::from_str(&header_value) {
                        builder
                            .headers_mut()
                            .unwrap()
                            .append(hyper::header::SET_COOKIE, value);
                    }
                }
                Err(e) => {
                    log::debug!("Skipping invalid cookie '{}': {}", name, e);
                }
            }
        }

        builder
            .body(
                http_body_util::Full::new(Bytes::from(self.description))
                    .map_err(|e| match e {})
                    .boxed(),
            )
            .unwrap()
    }
}

impl Clone for StreamingResponse {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            status_code: self.status_code,
            headers: self.headers.clone(),
            content_generator: self.content_generator.clone_ref(py),
        })
    }
}

impl StreamingResponse {
    pub fn new(status_code: u16, headers: Headers, content_generator: Py<PyAny>) -> Self {
        Self {
            status_code,
            headers,
            content_generator,
        }
    }

    /// Port of the actix `Responder for StreamingResponse` impl.
    pub fn to_hyper_response(self) -> HTTPResponse {
        let mut builder = response_builder(self.status_code);
        {
            let hyper_headers = builder.headers_mut().unwrap();
            crate::io_helpers::apply_hashmap_headers(hyper_headers, &self.headers);
        }

        // Optimized headers for SSE streaming (skip values the response
        // already carries, mirroring what the actix builder produced)
        let hyper_headers = builder.headers_mut().unwrap();
        if !hyper_headers.contains_key(hyper::header::CONNECTION) {
            hyper_headers.append("Connection", hyper::http::HeaderValue::from_static("keep-alive"));
        }
        hyper_headers.append("X-Accel-Buffering", hyper::http::HeaderValue::from_static("no")); // Disable nginx buffering
        hyper_headers.append(
            "Cache-Control",
            hyper::http::HeaderValue::from_static("no-cache, no-store, must-revalidate"),
        );
        hyper_headers.append("Pragma", hyper::http::HeaderValue::from_static("no-cache"));
        hyper_headers.append("Expires", hyper::http::HeaderValue::from_static("0"));

        // Create the optimized stream from the Python generator
        let stream = create_python_stream(self.content_generator);

        builder
            .body(
                BodyExt::boxed(http_body_util::StreamBody::new(
                    stream.map(|chunk| chunk.map(hyper::body::Frame::data)),
                )),
            )
            .unwrap()
    }
}

/// Wrapper disposing of the Python generator on an attached thread: the
/// stream's state is dropped on a runtime worker thread (or cancelled mid-
/// flight), where pyo3 0.30's strict drop checker (reference pool disabled)
/// would otherwise abort the process.
struct GeneratorGuard(Option<Py<PyAny>>);

impl GeneratorGuard {
    fn clone_ref(&self, py: Python) -> Py<PyAny> {
        self.0.as_ref().expect("generator is set").clone_ref(py)
    }
}

impl Drop for GeneratorGuard {
    fn drop(&mut self) {
        Python::attach(|_| drop(self.0.take()));
    }
}

fn create_python_stream(
    generator: Py<PyAny>,
) -> Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + Sync>> {
    Box::pin(futures::stream::unfold(
        GeneratorGuard(Some(generator)),
        |state| async move {
            let generator_for_task = Python::attach(|py| state.clone_ref(py));
            let next = tokio::task::spawn_blocking(move || {
                let next = Python::attach(|py| {
                    let gen_obj = generator_for_task.bind(py);

                    match gen_obj.call_method0("__next__") {
                        Ok(value) => {
                            // Accept both `bytes` (used as-is) and `str` (UTF-8 encoded)
                            // chunks, so binary streaming works too (#1236).
                            if let Ok(py_bytes) = value.cast::<PyBytes>() {
                                Some(py_bytes.as_bytes().to_vec())
                            } else if let Ok(s) = value.extract::<String>() {
                                Some(s.into_bytes())
                            } else {
                                log::error!(
                                    "StreamingResponse generator yielded a value that is neither str nor bytes; ending stream"
                                );
                                None
                            }
                        }
                        Err(e) => {
                            if !e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                                log::error!("Generator error: {}", e);
                            }
                            None
                        }
                    }
                });
                // Dispose of the per-task clone on this (soon to be
                // blocking-pool) thread, attached.
                Python::attach(|_| drop(generator_for_task));
                next
            })
            .await;
            match next {
                Ok(Some(bytes_value)) => Some((Ok(Bytes::from(bytes_value)), state)),
                Ok(None) => None,
                Err(e) => Some((Err(anyhow::anyhow!(e)), state)),
            }
        },
    ))
}

/// Default sentinel for the `description`/`body` constructor args.
///
/// Python has no way to represent "argument omitted" as a value, so to support
/// the `body=`/`description=` aliasing we must tell "not passed" apart from an
/// explicitly-passed value. `Option<..> = None` can't do this: pyo3 extracts a
/// Python `None` into Rust `None`, which is indistinguishable from an omitted
/// argument. Using `...` (Ellipsis) as the sentinel keeps `None` a distinct,
/// observable value, so `Response(body=None)` is forwarded to `check_body_type`
/// and rejected with a `ValueError` ("Could not convert specified body to
/// bytes") -- the existing behavior -- rather than being mistaken for a missing
/// argument and raising a `TypeError` instead.
fn missing_response_body() -> Py<PyAny> {
    Python::attach(|py| py.Ellipsis())
}

impl<'py> IntoPyObject<'py> for Response {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let headers = self.headers.into_pyobject(py)?.extract()?;

        let description = if self.description.is_empty() {
            "".into_pyobject(py)?.into_any()
        } else {
            match std::str::from_utf8(&self.description) {
                Ok(s) => s.into_pyobject(py)?.into_any(),
                Err(_) => PyBytes::new(py, &self.description).into_any(),
            }
        };

        let cookies: Py<Cookies> = Py::new(py, self.cookies)?;

        let response = PyResponse {
            status_code: self.status_code,
            response_type: self.response_type,
            headers,
            description: description.into(),
            file_path: self.file_path,
            cookies,
        };
        Ok(Py::new(py, response)?.into_bound(py).into_any())
    }
}

#[pyclass(name = "Response")]
#[derive(Debug)]
pub struct PyResponse {
    #[pyo3(get)]
    pub status_code: u16,
    #[pyo3(get)]
    pub response_type: String,
    #[pyo3(get, set)]
    pub headers: Py<Headers>,
    #[pyo3(get)]
    pub description: Py<PyAny>,
    #[pyo3(get)]
    pub file_path: Option<String>,
    #[pyo3(get)]
    pub cookies: Py<Cookies>,
}

#[pyclass(name = "StreamingResponse")]
#[derive(Debug)]
pub struct PyStreamingResponse {
    #[pyo3(get)]
    pub status_code: u16,
    #[pyo3(get, set)]
    pub headers: Py<Headers>,
    #[pyo3(get)]
    pub content: Py<PyAny>,
    #[pyo3(get)]
    pub media_type: String,
}

impl Clone for PyResponse {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            status_code: self.status_code,
            response_type: self.response_type.clone(),
            headers: self.headers.clone_ref(py),
            description: self.description.clone_ref(py),
            file_path: self.file_path.clone(),
            cookies: self.cookies.clone_ref(py),
        })
    }
}

impl Clone for PyStreamingResponse {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            status_code: self.status_code,
            headers: self.headers.clone_ref(py),
            content: self.content.clone_ref(py),
            media_type: self.media_type.clone(),
        })
    }
}

#[pymethods]
impl PyStreamingResponse {
    #[new]
    pub fn new(
        py: Python,
        content: Py<PyAny>,
        status_code: Option<u16>,
        headers: Option<Bound<'_, PyAny>>,
        media_type: Option<String>,
    ) -> PyResult<Self> {
        let status_code = status_code.unwrap_or(200);
        let media_type = media_type.unwrap_or_else(|| "text/event-stream".to_string());

        let headers_output: Py<Headers> = if let Some(headers) = headers {
            if let Ok(headers_dict) = headers.cast::<PyDict>() {
                let headers = Headers::new(Some(headers_dict));
                Py::new(py, headers)?
            } else if let Ok(headers) = headers.extract::<Py<Headers>>() {
                headers
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "headers must be a Headers instance or a dict",
                ));
            }
        } else {
            let mut headers = Headers::new(None);
            if media_type == "text/event-stream" {
                headers.set("Content-Type".to_string(), "text/event-stream".to_string());
                headers.set("Cache-Control".to_string(), "no-cache".to_string());
                headers.set("Connection".to_string(), "keep-alive".to_string());
            } else {
                // For non-SSE streaming responses, still set appropriate headers
                headers.set("Content-Type".to_string(), media_type.clone());
            }
            Py::new(py, headers)?
        };

        Ok(Self {
            status_code,
            headers: headers_output,
            content,
            media_type,
        })
    }
}

#[pymethods]
impl PyResponse {
    // To do: Add check for content-type in header and change response_type accordingly
    #[new]
    #[pyo3(signature = (status_code, headers=None, description=missing_response_body(), *, body=missing_response_body()))]
    pub fn new(
        py: Python,
        status_code: u16,
        headers: Option<Bound<'_, PyAny>>,
        description: Py<PyAny>,
        body: Py<PyAny>,
    ) -> PyResult<Self> {
        // `body` is the preferred, intuitively-named alias for the response body;
        // `description` is the legacy name. Exactly one must be supplied (see
        // `missing_response_body` for why we use an Ellipsis sentinel here).
        let description_is_missing = description.bind(py).is(&**PyEllipsis::get(py));
        let body_is_missing = body.bind(py).is(&**PyEllipsis::get(py));

        let description = match (description_is_missing, body_is_missing) {
            (false, true) => description,
            (true, false) => body,
            (false, false) => {
                return Err(PyTypeError::new_err(
                    "Response() got both 'description' and 'body'; pass only one",
                ));
            }
            (true, true) => {
                return Err(PyTypeError::new_err(
                    "Response() missing required argument: 'description' (or its alias 'body')",
                ));
            }
        };

        check_body_type(py, &description)?;

        let headers_output: Py<Headers> = if let Some(headers) = headers {
            if let Ok(headers_dict) = headers.cast::<PyDict>() {
                // Here you'd have logic to create a Headers instance from a PyDict
                // For simplicity, let's assume you have a method `from_dict` on Headers for this
                let headers = Headers::new(Some(headers_dict)); // Hypothetical method
                Py::new(py, headers)?
            } else if let Ok(headers) = headers.extract::<Py<Headers>>() {
                // If it's already a Py<Headers>, use it directly
                headers
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "headers must be a Headers instance or a dict",
                ));
            }
        } else {
            Py::new(py, Headers::new(None))?
        };

        let cookies: Py<Cookies> = Py::new(py, Cookies::new())?;

        Ok(Self {
            status_code,
            // we should be handling based on headers but works for now
            response_type: "text".to_string(),
            headers: headers_output,
            description,
            file_path: None,
            cookies,
        })
    }

    #[setter]
    pub fn set_description(&mut self, py: Python, description: Py<PyAny>) -> PyResult<()> {
        check_description_type(py, &description)?;
        self.description = description;
        Ok(())
    }

    #[setter]
    pub fn set_file_path(&mut self, py: Python, file_path: &str) -> PyResult<()> {
        self.response_type = "static_file".to_string();
        self.file_path = Some(file_path.to_string());

        match read_file(file_path) {
            Ok(content) => {
                self.description = PyBytes::new(py, &content).into();
                Ok(())
            }
            Err(e) => Err(PyIOError::new_err(format!("Failed to read file: {}", e))),
        }
    }

    #[pyo3(signature = (key, value, path=None, domain=None, max_age=None, expires=None, secure=false, http_only=false, same_site=None))]
    pub fn set_cookie(
        &mut self,
        py: Python,
        key: &str,
        value: &str,
        path: Option<String>,
        domain: Option<String>,
        max_age: Option<i64>,
        expires: Option<String>,
        secure: bool,
        http_only: bool,
        same_site: Option<String>,
    ) -> PyResult<()> {
        let cookie = Cookie::new(
            value.to_string(),
            path,
            domain,
            max_age,
            expires,
            secure,
            http_only,
            same_site,
        );

        self.cookies
            .try_borrow_mut(py)
            .expect("value already borrowed")
            .cookies
            .insert(key.to_string(), cookie);
        Ok(())
    }
}

impl FromPyObject<'_, '_> for Response {
    type Error = PyErr;

    #[inline]
    fn extract(obj: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        let type_name = obj.get_type().name()?;
        if type_name != "Response" {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Expected Response, got {}",
                type_name
            )));
        }

        let status_code: u16 = obj.getattr("status_code")?.extract()?;
        let response_type: String = obj.getattr("response_type")?.extract()?;
        let headers: Headers = obj.getattr("headers")?.extract()?;
        let description: Vec<u8> = get_description_from_pyobject(&obj.getattr("description")?)?;
        let file_path: Option<String> = obj.getattr("file_path")?.extract()?;
        let cookies: Cookies = obj.getattr("cookies")?.extract()?;

        Ok(Response {
            status_code,
            response_type,
            headers,
            description,
            file_path,
            cookies,
        })
    }
}

impl FromPyObject<'_, '_> for StreamingResponse {
    type Error = PyErr;

    #[inline]
    fn extract(obj: pyo3::Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        if !obj.hasattr("content").unwrap_or(false) || !obj.hasattr("media_type").unwrap_or(false) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Object is missing required StreamingResponse attributes",
            ));
        }

        let status_code: u16 = obj.getattr("status_code")?.extract()?;
        let mut headers: Headers = obj.getattr("headers")?.extract()?;

        let media_type: String = obj
            .getattr("media_type")
            .and_then(|a| a.extract())
            .unwrap_or_else(|_| "text/event-stream".to_string());

        if media_type == "text/event-stream" {
            headers.set("Content-Type".to_string(), "text/event-stream".to_string());
            if headers.get("Cache-Control".to_string()).is_none() {
                headers.set("Cache-Control".to_string(), "no-cache".to_string());
            }
            if headers.get("Connection".to_string()).is_none() {
                headers.set("Connection".to_string(), "keep-alive".to_string());
            }
        }

        let content: pyo3::Py<PyAny> = obj.getattr("content")?.unbind();

        Ok(StreamingResponse::new(status_code, headers, content))
    }
}
