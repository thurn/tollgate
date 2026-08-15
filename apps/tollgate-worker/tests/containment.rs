#![cfg(target_os = "macos")]

use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use nix::{errno::Errno, sys::signal::kill, unistd::Pid};
use serde_json::json;

fn run_worker(
    cwd: &std::path::Path,
    nonce: &str,
    script: &str,
    timeout_ms: u64,
) -> serde_json::Value {
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
            "cwd": cwd,
            "argv": ["/usr/bin/python3", "-c", script],
            "environment": {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
            "timeout_ms": timeout_ms,
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
    terminal
}

#[test]
fn reparented_session_escape_is_failed_and_reaped() {
    let temporary = tempfile::tempdir().unwrap();
    let pid_path = temporary.path().join("escaped.pid");
    let script = format!(
        "import os,time\np=os.fork()\nif p: os._exit(0)\nos.setsid()\nopen({:?},'w').write(str(os.getpid()))\nprint(os.getpid(),flush=True)\ntime.sleep(30)",
        pid_path.to_string_lossy()
    );
    let terminal = run_worker(
        temporary.path(),
        "containment-session-escape",
        &script,
        5_000,
    );

    let escaped_pid = std::fs::read_to_string(pid_path)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    assert_eq!(terminal["containment_escaped"], true);
    assert!(
        terminal["containment_diagnostic"]
            .as_str()
            .unwrap()
            .contains("Tollgate rejected")
    );
    assert_eq!(terminal["process_group_reaped"], true);
    assert_eq!(kill(Pid::from_raw(escaped_pid), None), Err(Errno::ESRCH));
}

#[test]
fn connected_session_escape_is_failed_and_reaped() {
    let temporary = tempfile::tempdir().unwrap();
    let pid_path = temporary.path().join("escaped.pid");
    let script = format!(
        "import os,time\np=os.fork()\nif p == 0:\n os.setsid()\n open({:?},'w').write(str(os.getpid()))\n time.sleep(30)\n os._exit(0)\nos.waitpid(p,0)",
        pid_path.to_string_lossy()
    );

    let terminal = run_worker(
        temporary.path(),
        "containment-connected-session-escape",
        &script,
        5_000,
    );

    let escaped_pid = std::fs::read_to_string(pid_path)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    assert_eq!(terminal["containment_escaped"], true);
    assert!(
        terminal["containment_diagnostic"]
            .as_str()
            .unwrap()
            .contains("session escape")
    );
    assert_eq!(terminal["process_group_reaped"], true);
    assert_eq!(kill(Pid::from_raw(escaped_pid), None), Err(Errno::ESRCH));
}

#[test]
fn same_session_subgroup_is_allowed_when_the_root_waits_for_it() {
    let temporary = tempfile::tempdir().unwrap();
    let script = "import os,time\np=os.fork()\nif p == 0:\n os.setpgid(0,0)\n time.sleep(0.3)\n os._exit(0)\nos.waitpid(p,0)\nprint('subgroup complete',flush=True)";

    let terminal = run_worker(
        temporary.path(),
        "containment-same-session-subgroup",
        script,
        5_000,
    );

    assert_eq!(terminal["exit_code"], 0);
    assert_eq!(terminal["signal"], serde_json::Value::Null);
    assert_eq!(terminal["containment_escaped"], false);
    assert_eq!(terminal["containment_diagnostic"], serde_json::Value::Null);
    assert_eq!(terminal["process_group_reaped"], true);
}

#[test]
fn timed_out_same_session_subgroup_is_reaped_without_a_containment_failure() {
    let temporary = tempfile::tempdir().unwrap();
    let pid_path = temporary.path().join("subgroup.pid");
    let script = format!(
        "import os,time\np=os.fork()\nif p == 0:\n os.setpgid(0,0)\n open({:?},'w').write(str(os.getpid()))\n time.sleep(30)\n os._exit(0)\nos.waitpid(p,0)",
        pid_path.to_string_lossy()
    );

    let terminal = run_worker(
        temporary.path(),
        "containment-timed-out-subgroup",
        &script,
        500,
    );
    let subgroup_pid = std::fs::read_to_string(pid_path)
        .unwrap()
        .parse::<i32>()
        .unwrap();

    assert_eq!(terminal["timed_out"], true);
    assert_eq!(terminal["containment_escaped"], false);
    assert_eq!(terminal["containment_diagnostic"], serde_json::Value::Null);
    assert_eq!(terminal["process_group_reaped"], true);
    assert_eq!(kill(Pid::from_raw(subgroup_pid), None), Err(Errno::ESRCH));
}
