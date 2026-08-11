#![cfg(target_os = "macos")]

use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use nix::{errno::Errno, sys::signal::kill, unistd::Pid};
use serde_json::json;

#[test]
fn reparented_session_escape_is_failed_and_reaped() {
    let temporary = tempfile::tempdir().unwrap();
    let pid_path = temporary.path().join("escaped.pid");
    let script = format!(
        "import os,time\np=os.fork()\nif p: os._exit(0)\nos.setsid()\nopen({:?},'w').write(str(os.getpid()))\nprint(os.getpid(),flush=True)\ntime.sleep(30)",
        pid_path.to_string_lossy()
    );
    let nonce = "containment-integration";
    let mut worker = Command::new(env!("CARGO_BIN_EXE_tollgate-worker"))
        .args(["--nonce", nonce])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = worker.stdin.take().unwrap();
    writeln!(
        input,
        "{}",
        json!({
            "type": "spec",
            "nonce": nonce,
            "cwd": temporary.path(),
            "argv": ["/usr/bin/python3", "-c", script],
            "environment": {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
            "timeout_ms": 5_000,
            "termination_grace_ms": 200,
            "rss_limit_bytes": null,
        })
    )
    .unwrap();
    writeln!(input, "{}", json!({"type": "start", "nonce": nonce})).unwrap();
    input.flush().unwrap();

    let mut output = BufReader::new(worker.stdout.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(8);
    let terminal = loop {
        assert!(
            Instant::now() < deadline,
            "worker did not reach terminal state"
        );
        let mut line = String::new();
        assert_ne!(output.read_line(&mut line).unwrap(), 0);
        let value: serde_json::Value = serde_json::from_str(&line).unwrap();
        if value["type"] == "terminal" {
            break value;
        }
    };
    drop(input);
    assert!(worker.wait().unwrap().success());

    let escaped_pid = std::fs::read_to_string(pid_path)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    assert_eq!(terminal["containment_escaped"], true);
    assert_eq!(terminal["process_group_reaped"], true);
    assert_eq!(kill(Pid::from_raw(escaped_pid), None), Err(Errno::ESRCH));
}
