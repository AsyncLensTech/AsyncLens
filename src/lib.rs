use once_cell::sync::Lazy;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

static IS_CAPTURING: AtomicBool = AtomicBool::new(false);

static THREAD_NAMES: Lazy<RwLock<HashMap<u64, (String, u32)>>> = Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Serialize, Clone, Debug)]
struct StackFrame {
    filename: String,
    line_number: i32,
    function_name: String,
    code_line: String,
    pid: u32,
    tid: u64,
    thread_name: String
}

impl std::fmt::Display for StackFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{} ({}) 📁 {} l. {} in `{}` 👉 {}",
            self.pid,
            self.tid,
            self.thread_name,
            self.filename,
            self.line_number,
            self.function_name,
            self.code_line
        )
    }
}

#[pyclass]
struct AsyncWatchdog {
    is_alive: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    threshold_ms: u64,
    logger: Py<PyAny>,
    target_tid: Arc<Mutex<Option<u64>>>,
    ignore_patterns: Vec<String>, // <-- Stockage des motifs dynamiques transmis par Python
}

#[pymethods]
impl AsyncWatchdog {
    #[new]
    fn new(threshold_ms: u64, logger: Py<PyAny>, ignore_patterns: Vec<String>) -> Self {
        AsyncWatchdog {
            is_alive: Arc::new(AtomicBool::new(true)),
            running: Arc::new(AtomicBool::new(true)),
            threshold_ms,
            logger,
            target_tid: Arc::new(Mutex::new(None)),
            ignore_patterns,
        }
    }

    fn heartbeat(&self) {
        self.is_alive.store(true, Ordering::Relaxed);
    }

    fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    fn register_thread(&self, py: Python<'_>, name: String) -> PyResult<()> {
        let threading = py.import("threading")?;
        let current_thread = threading.call_method0("current_thread")?;
        let ident: u64 = current_thread.getattr("ident")?.extract()?;
        let current_pid = std::process::id(); 

        if let Ok(mut map) = THREAD_NAMES.write() {
            map.insert(ident, (name, current_pid));
        }

        if let Ok(mut tid_guard) = self.target_tid.lock() {
            *tid_guard = Some(ident);
        }

        Ok(())
    }

    fn start(&self) {
        let is_alive = Arc::clone(&self.is_alive);
        let running = Arc::clone(&self.running);
        let threshold = self.threshold_ms;
        let logger_clone = Python::attach(|py| self.logger.clone_ref(py));

        let ignore_patterns_clone = self.ignore_patterns.clone(); // Duplication pour le thread de garde

        thread::spawn(move || {
            let mut curr_lines: Vec<StackFrame> = vec![];
            let mut was_alerted = false;
            let mut blocked_since: Option<Instant> = None;

            while running.load(Ordering::Relaxed) || was_alerted {
                is_alive.store(false, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(threshold));

                let python_responded = is_alive.load(Ordering::Relaxed);

                if !python_responded {
                    if !running.load(Ordering::Relaxed) && !was_alerted {
                        break;
                    }

                    if !was_alerted {
                        blocked_since = Some(Instant::now());

                        let ignore_patterns_ref = &ignore_patterns_clone;

                        Python::attach(|py| {
                            if !running.load(Ordering::Relaxed) {
                                return;
                            }
                            if let Ok(lines) = trigger_python_stack_capture(py, ignore_patterns_ref) {
                                if !lines.is_empty() {
                                    curr_lines = lines;
                                    let logger_bound = logger_clone.bind(py);
                                    let mut msg = String::from("🚨 [AsyncLens] Loop blocked!");
                                    
                                    for line in &curr_lines {
                                        msg.push_str(&format!("\n  {}", line));
                                    }
                                    let _ = logger_bound.call_method1("error", (msg,));
                                    was_alerted = true;
                                    IS_CAPTURING.store(true, Ordering::Relaxed);
                                }
                            }
                        });
                    }
                } else {
                    if was_alerted {
                        let duration_ms = blocked_since
                            .map(|inst| inst.elapsed().as_millis())
                            .unwrap_or(0);

                        Python::attach(|py| {
                            let logger_bound = logger_clone.bind(py);
                            let mut msg = format!(
                                "✅ [AsyncLens] Loop released! (Blocked for {}ms)", 
                                duration_ms
                            );
                            for line in &curr_lines {
                                msg.push_str(&format!("\n  {}", line));
                            }
                            let _ = logger_bound.call_method1("warning", (msg,));
                        });

                        curr_lines.clear();
                        was_alerted = false;
                        blocked_since = None;
                        IS_CAPTURING.store(false, Ordering::Relaxed);
                    }
                }

                if !running.load(Ordering::Relaxed) && !was_alerted {
                    break;
                }
            }
        });
    }
}

fn trigger_python_stack_capture(
    py: Python<'_>, 
    ignore_patterns: &[String]
) -> PyResult<Vec<StackFrame>> {
    let sys = py.import("sys")?;
    let traceback = py.import("traceback")?;
    let current_frames = sys.call_method0("_current_frames")?;
    let dict = current_frames.cast::<PyDict>()?;
    let current_pid = std::process::id();

    let mut user_frames = Vec::new();
    let names_map = THREAD_NAMES.read().unwrap_or_else(|e| e.into_inner());

    for (_thread_id, frame) in dict.iter() {
        let tid: u64 = _thread_id.extract()?;

        let thread_name = names_map
            .get(&tid)
            .filter(|(_, pid)| *pid == current_pid)
            .map(|(name, _)| name.clone())
            .unwrap_or_else(|| format!("UnknownLoop-{}", tid));

        let stack = traceback.call_method1("extract_stack", (&frame,))?;
        let stack_list = stack.cast::<PyList>()?;

        for item in stack_list.iter() {
            let filename: String = item.getattr("filename")?.extract()?;
            let function_name: String = item.getattr("name")?.extract()?;

            // 🔥 CORRECTION DYNAMIQUE : On vérifie si le nom du fichier contient l'un des motifs à ignorer
            let is_system = function_name == "<module>" || ignore_patterns.iter().any(|p| filename.contains(p));

            if !is_system {
                let line_number: i32 = item.getattr("lineno")?.extract()?;
                let code_line: String = item.getattr("line")?.extract()?;

                user_frames.push(StackFrame {
                    filename,
                    line_number,
                    function_name,
                    code_line,
                    pid: current_pid,
                    tid,
                    thread_name: thread_name.clone()
                });
            }
        }
    }

    Ok(user_frames)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<AsyncWatchdog>()?;
    Ok(())
}
