#!/usr/bin/env python3
"""LaminaDB backup-selection experiment driven through the real HTTP service.

Layout convention (untrusted generated data lives under eval/runs only):
  eval/scripts/  - this orchestrator (project-authored code)
  eval/runs/<id>/source|restored-* - data dirs created by laminad itself
  eval/runs/<id>/images/*.lmi      - frozen backups in the on-disk format,
                                     bytes untouched after GET /backup

The script only uses the Python standard library and is intended to run with
`python3 -I`. It talks to the built bin/laminad over loopback HTTP.
"""
import argparse
import binascii
import hashlib
import json
import pathlib
import socket
import struct
import subprocess
import time
import urllib.error
import urllib.request

SCHEMA = {
    "name": "spectra",
    "primary": "sample",
    "columns": [
        {"name": "sample", "type": "int"},
        {"name": "band", "type": "text"},
        {"name": "amplitude", "type": "int", "nullable": True},
        {"name": "valid", "type": "bool"},
    ],
}


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class HTTPError_(Exception):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


class Server:
    def __init__(self, binary, datadir, log_handle, restore=None):
        self.binary = str(binary)
        self.datadir = pathlib.Path(datadir)
        self.log = log_handle
        self.restore = restore
        self.process = None
        self.url = None
        self.ready_at = None

    def start(self):
        port = free_port()
        args = [self.binary, "-data", str(self.datadir),
                "-listen", f"127.0.0.1:{port}"]
        if self.restore:
            args += ["-restore", str(self.restore)]
            self.restore = None
        started = time.monotonic()
        self.process = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        self.url = f"http://127.0.0.1:{port}/v1/lamina"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0)
                raise RuntimeError("laminad exited: " + self.log.read().decode())
            try:
                self.call("GET", "/health")
                self.ready_at = time.monotonic() - started
                return
            except (OSError, urllib.error.URLError, HTTPError_):
                time.sleep(0.03)
        raise RuntimeError("service did not become ready")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()

    def call(self, method, path, data=None, expect=None):
        encoded = None if data is None else json.dumps(data, ensure_ascii=False).encode()
        req = urllib.request.Request(
            self.url + path, data=encoded, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                status, body = response.status, response.read()
        except urllib.error.HTTPError as exc:
            status, body = exc.code, exc.read()
        if expect is not None and status != expect:
            raise HTTPError_(status, body.decode())
        return status, body

    def json(self, method, path, data=None, expect=200):
        status, body = self.call(method, path, data, expect)
        return json.loads(body)

    def begin(self, readonly=False):
        return self.json("POST", "/transactions", {"read_only": readonly}, 201)["id"]

    def put(self, tx, row_):
        return self.json("POST", f"/transactions/{tx}/put",
                         {"relation": "spectra", "row": row_}, 200)

    def commit(self, tx, expect=200):
        return self.json("POST", f"/transactions/{tx}/commit", expect=expect)

    def query_at(self, at):
        return self.call("POST", "/query", {"relation": "spectra", "at": at})

    def changes(self, after, limit=100):
        return self.call("GET", f"/changes?after={after}&limit={limit}")

    def backup(self, target: pathlib.Path):
        status, body = self.call("GET", "/backup", expect=200)
        target.write_bytes(body)
        return body

    def stats(self):
        return self.json("GET", "/stats")

    def vacuum(self, horizon):
        return self.json("POST", "/vacuum", {"horizon": horizon}, 200)

    def rollback(self, tx):
        return self.json("POST", f"/transactions/{tx}/rollback", {}, 200)

    def delete(self, tx, key):
        return self.json("POST", f"/transactions/{tx}/delete",
                         {"relation": "spectra", "key": key}, 200)


def row(sample, amplitude, band=None, valid=True):
    return {"sample": sample, "band": band or f"b{sample}",
            "amplitude": amplitude, "valid": valid}


# ---------------------------------------------------------------------------
# Independent decoders (no project code imported; wire formats re-derived
# from README/spec and the observed file layouts).
# ---------------------------------------------------------------------------

def extract_payload(blob: bytes):
    """Return the exact raw payload bytes of the envelope (digest input)."""
    key = b'"payload"'
    idx = blob.find(key)
    assert idx >= 0, "payload key missing"
    colon = blob.find(b":", idx + len(key))
    start = colon + 1
    while blob[start:start + 1].isspace():
        start += 1
    assert blob[start:start + 1] == b"{", "payload must be a JSON object"
    depth, in_str, esc, i = 0, False, False, start
    while i < len(blob):
        ch = blob[i:i + 1]
        if in_str:
            if esc:
                esc = False
            elif ch == b"\\":
                esc = True
            elif ch == b'"':
                in_str = False
        elif ch == b'"':
            in_str = True
        elif ch == b"{":
            depth += 1
        elif ch == b"}":
            depth -= 1
            if depth == 0:
                return blob[start:i + 1]
        i += 1
    raise AssertionError("unterminated payload object")


def decode_image(blob: bytes):
    envelope = json.loads(blob)
    payload = extract_payload(blob)
    digest = hashlib.sha256(payload).hexdigest()
    image = json.loads(payload)
    return {
        "format": envelope["format"],
        "declared_digest": envelope["sha256"],
        "computed_digest": digest,
        "digest_ok": digest == envelope["sha256"],
        "sequence": image["sequence"],
        "horizon": image["horizon"],
        "catalog": image["catalog"],
        "store": image["store"],
    }


def chain_versions(store):
    out = []
    for table, rows in store["tables"].items():
        for key, chain in rows.items():
            for version in chain:
                out.append((table, key, version["sequence"],
                            "tombstone" if version.get("deleted") else "live"))
    return sorted(out, key=lambda item: (item[2], item[0], item[1]))


def read_wal(path: pathlib.Path):
    """Parse an LMW1 journal with CRC32 frames, mirroring journal/wal.go."""
    data = path.read_bytes()
    assert data[:4] == b"LMW1", "bad magic"
    base = struct.unpack(">Q", data[4:12])[0]
    header_crc = struct.unpack(">I", data[12:16])[0]
    assert binascii.crc32(data[:12]) & 0xFFFFFFFF == header_crc, "bad header crc"
    offset, records, truncated = 16, [], 0
    while offset < len(data):
        if len(data) - offset < 8:
            truncated = len(data) - offset
            break
        length, crc = struct.unpack(">II", data[offset:offset + 8])
        frame = data[offset + 8:offset + 8 + length]
        if len(frame) < length:
            truncated = 8 + len(frame)
            break
        assert binascii.crc32(frame) & 0xFFFFFFFF == crc, "frame crc mismatch"
        records.append(json.loads(frame))
        offset += 8 + length
    seq = [r["sequence"] for r in records]
    assert seq == list(range(base + 1, base + 1 + len(records))), "non-contiguous"
    kinds = {}
    for r in records:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    return {"base": base, "records": records, "kinds": kinds,
            "truncated_tail_bytes": truncated, "bytes": len(data)}


def replay_until(records, head):
    """Independent mini-replay of WAL records up to `head`, incl. vacuum."""
    view = {"tables": {}, "horizon": 0}
    for r in records:
        if r["sequence"] > head:
            break
        if r["kind"] == "commit":
            for m in r["writes"]:
                view["tables"].setdefault(m["relation"], {})
                view["tables"][m["relation"]].setdefault(m["key"], [])
                version = {"sequence": r["sequence"]}
                if m.get("row"):
                    version["row"] = m["row"]
                if m.get("deleted"):
                    version["deleted"] = True
                view["tables"][m["relation"]][m["key"]].append(version)
        elif r["kind"] == "vacuum":
            view["horizon"] = r["horizon"]
            for rows in view["tables"].values():
                for key, chain in list(rows.items()):
                    keep = 0
                    while keep + 1 < len(chain) and chain[keep + 1]["sequence"] <= r["horizon"]:
                        keep += 1
                    if chain and chain[-1]["sequence"] <= r["horizon"] and chain[-1].get("deleted"):
                        del rows[key]
                        continue
                    rows[key] = chain[keep:]
    return view


def store_canonical(store):
    return json.dumps(store["tables"], sort_keys=True, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    run = pathlib.Path(args.run_dir)
    images_dir = run / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    log = open(run / "orchestrator.log", "w")

    report = {"timeline": [], "images": {}, "cross_checks": [],
              "restore_probes": {}, "gaps": {}, "sizes": {}}
    check_results = []

    def note(step, **facts):
        entry = {"step": step}
        entry.update(facts)
        report["timeline"].append(entry)

    def check(name, ok, detail):
        check_results.append({"check": name, "ok": bool(ok), "detail": detail})
        report["cross_checks"].append(check_results[-1])
        if not ok:
            raise AssertionError(f"cross-check failed: {name}: {detail}")

    # --- phase 0: open the source instance ---------------------------------
    src = Server(args.binary, run / "source", log)
    src.start()
    note("source-open", rto_seconds=round(src.ready_at, 4))

    # seq 1: DDL
    src.json("POST", "/relations", SCHEMA, 201)
    note("relation-created", sequence=src.stats()["sequence"])

    def txn_commit(rows, deletes=None, label=""):
        tx = src.begin()
        for r in rows:
            src.put(tx, r)
        for key in deletes or []:
            src.delete(tx, key)
        result = src.commit(tx)
        note(label or "commit", sequence=result["sequence"], writes=result["writes"])
        return result["sequence"]

    # seq 2..5: seed plus three update rounds -> head 5, horizon 0
    txn_commit([row(1, 10), row(2, 20), row(3, 30)], label="seed-1-3")
    txn_commit([row(1, 11), row(2, 21)], label="update-round-1")
    txn_commit([row(1, 12), row(3, 31)], label="update-round-2")
    txn_commit([row(2, 22), row(3, 32)], label="update-round-3")
    stats5 = src.stats()
    note("pre-A-state", sequence=stats5["sequence"], horizon=stats5["horizon"],
         versions=stats5["versions"])

    # --- image A: oldest point, zero retention horizon, full short history --
    path_a = images_dir / "image-A.lmi"
    blob_a = src.backup(path_a)
    note("backup-A", bytes=len(blob_a), head=stats5["sequence"])

    # seq 6..8: commits that exist only in the source journal afterwards
    txn_commit([row(1, 13)], label="post-A-update-1")
    txn_commit([row(2, 23)], label="post-A-update-2")
    txn_commit([row(3, 33)], label="post-A-update-3")

    # long-lived reader at seq 8 pins the first vacuum
    pin = src.begin(readonly=True)
    pin_snapshot = src.stats()["sequence"]
    note("read-tx-open", snapshot=pin_snapshot)

    txn_commit([row(1, 14)], label="pre-vacuum-update")  # seq 9
    requested = src.stats()["sequence"]                  # 9
    pinned = src.vacuum(requested)
    note("vacuum-pinned-request", requested=requested, result=pinned)
    check("vacuum-pinned-by-active-reader",
          pinned["pinned"] is True and pinned["horizon"] == pin_snapshot,
          f"requested {requested}, got {pinned}")

    src.rollback(pin)
    vac_advanced = src.vacuum(requested)
    note("vacuum-advanced", result=vac_advanced,
         sequence=src.stats()["sequence"], horizon=src.stats()["horizon"])
    check("vacuum-advanced-to-9",
          vac_advanced["horizon"] == 9 and not vac_advanced["pinned"],
          f"got {vac_advanced}")

    # one more commit, then image B: narrow retained window, horizon 9
    txn_commit([row(2, 24)], label="between-vacuums")
    stats_b = src.stats()
    path_b = images_dir / "image-B.lmi"
    blob_b = src.backup(path_b)
    note("backup-B", bytes=len(blob_b), head=stats_b["sequence"],
         horizon=stats_b["horizon"])

    # extend head past B: update then re-assert the same horizon (no regression)
    txn_commit([row(3, 34)], label="post-B-update")
    head_before = src.stats()["sequence"]
    vac_reassert = src.vacuum(9)
    note("vacuum-reassert-same-horizon", result=vac_reassert,
         head_before=head_before, horizon_after=src.stats()["horizon"])
    check("vacuum-horizon-monotone-stays-9",
          vac_reassert["horizon"] == 9 and src.stats()["horizon"] == 9,
          f"got {vac_reassert}")

    # tombstone newer than the horizon must survive inside image C
    txn_commit([], deletes=[2], label="post-B-delete-2")
    stats_c = src.stats()

    # --- image C: newest point, same horizon 9, widest retained window -----
    path_c = images_dir / "image-C.lmi"
    blob_c = src.backup(path_c)
    note("backup-C", bytes=len(blob_c), head=stats_c["sequence"],
         horizon=stats_c["horizon"])

    # commit after C: frozen images must not track the source head
    final_seq = txn_commit([row(1, 15)], label="post-C-update")
    final_stats = src.stats()
    note("source-head-after-C", sequence=final_stats["sequence"])

    # ------------------------------------------------------------------ #
    # Independent decode of all three images straight from the .lmi bytes
    # ------------------------------------------------------------------ #
    decoded = {}
    filenames = {"A": "image-A.lmi", "B": "image-B.lmi", "C": "image-C.lmi"}
    for name, blob in (("A", blob_a), ("B", blob_b), ("C", blob_c)):
        info = decode_image(blob)
        decoded[name] = info
        report["images"][name] = {
            "file": filenames[name],
            "sha256": info["computed_digest"],
            "digest_self_verifies": info["digest_ok"],
            "format": info["format"],
            "head_sequence": info["sequence"],
            "horizon": info["horizon"],
            "relations": sorted(info["catalog"]["relations"].keys()),
            "relation_created": {k: v["created"] for k, v in
                                 info["catalog"]["relations"].items()},
            "version_count": len(chain_versions(info["store"])),
            "versions": [
                {"table": t, "key": k, "sequence": s, "state": st}
                for (t, k, s, st) in chain_versions(info["store"])],
            "bytes": len(blob),
        }
        check(f"image-{name}-digest", info["digest_ok"],
              f"declared {info['declared_digest'][:12]}... "
              f"computed {info['computed_digest'][:12]}...")
        check(f"image-{name}-header-order", info["horizon"] <= info["sequence"],
              f"h={info['horizon']} seq={info['sequence']}")

    # ------------------------------------------------------------------ #
    # Independent WAL parse of the SOURCE journal (full history on disk)
    # ------------------------------------------------------------------ #
    src.stop()
    wal = read_wal(run / "source" / "journal.lmw")
    vacuum_records = [(r["sequence"], r["horizon"]) for r in wal["records"]
                      if r["kind"] == "vacuum"]
    report["source_journal"] = {
        "base": wal["base"], "frame_count": len(wal["records"]),
        "kinds": wal["kinds"], "truncated_tail_bytes": wal["truncated_tail_bytes"],
        "bytes": wal["bytes"],
        "sequence_span": [wal["records"][0]["sequence"],
                          wal["records"][-1]["sequence"]],
        "vacuum_records": vacuum_records,
    }

    commit_seqs = {r["sequence"] for r in wal["records"] if r["kind"] == "commit"}
    for name in ("A", "B", "C"):
        info = decoded[name]
        for table, rows in info["store"]["tables"].items():
            for key, chain in rows.items():
                for version in chain:
                    check(f"image-{name}-v{version['sequence']}-exists-in-wal",
                          version["sequence"] in commit_seqs,
                          f"{table}/{key}@{version['sequence']}")
        check(f"image-{name}-head-is-wal-commit-or-vacuum",
              info["sequence"] <= wal["records"][-1]["sequence"],
              f"head {info['sequence']}")
    check("wal-vacuum-records",
          vacuum_records == [(10, 8), (11, 9), (14, 9)],
          f"got {vacuum_records}")
    check("wal-complete-history-retained",
          report["source_journal"]["sequence_span"] == [1, final_seq],
          f"span={report['source_journal']['sequence_span']}")

    # Independent mini-replay must reproduce each frozen image byte-for-byte
    # at the semantic level (catalog relations + version chains + horizon).
    for name in ("A", "B", "C"):
        target = decoded[name]["sequence"]
        view = replay_until(wal["records"], target)
        same_store = store_canonical(view) == store_canonical(decoded[name]["store"])
        same_horizon = view["horizon"] == decoded[name]["horizon"]
        check(f"mini-replay-reproduces-image-{name}",
              same_store and same_horizon,
              f"head={target} replay_h={view['horizon']} "
              f"image_h={decoded[name]['horizon']} store_match={same_store}")

    # ------------------------------------------------------------------ #
    # Real restores: fresh data directory per image; reachable history,
    # change-stream base, consumer realignment cost, restart durability.
    # ------------------------------------------------------------------ #
    reach_cases = list(range(0, final_seq + 1))
    for name in ("A", "B", "C"):
        image_path = images_dir / filenames[name]
        datadir = run / f"restored-{name}"
        server_log = open(run / f"restored-{name}.log", "w")
        restored = Server(args.binary, datadir, server_log, restore=image_path)
        restored.start()
        rto = restored.ready_at

        st = restored.stats()
        rwal = read_wal(datadir / "journal.lmw")
        reach = {}
        for at in reach_cases:
            status, body = restored.query_at(at)
            entry = {"http": status}
            if status == 200:
                payload = json.loads(body)
                entry["matched"] = payload["matched"]
                entry["rows"] = payload["rows"]
            else:
                entry["error"] = json.loads(body)["error"]["code"]
            reach[str(at)] = entry

        status_base, body_base = restored.changes(st["history_base"])
        status_old, body_old = restored.changes(st["history_base"] - 1)
        status_head, body_head = restored.changes(st["sequence"])

        new_tx = restored.begin()
        restored.put(new_tx, row(9, 90))
        commit_after = restored.commit(new_tx)
        stats_after = restored.stats()
        rwal_after = read_wal(datadir / "journal.lmw")
        status_new, body_new = restored.changes(st["sequence"])

        restored.stop()
        server_log2 = open(run / f"restored-{name}-restart.log", "w")
        restored2 = Server(args.binary, datadir, server_log2)
        restored2.start()
        post_restart_stats = restored2.stats()
        status_after_restart, _ = restored2.query_at(post_restart_stats["sequence"])
        restored2.stop()

        report["restore_probes"][name] = {
            "rto_seconds": round(rto, 4),
            "startup_stats": st,
            "installed_journal_header_base": rwal["base"],
            "installed_journal_frames": len(rwal["records"]),
            "reachable_history": reach,
            "changes_at_base": {"http": status_base, "body": json.loads(body_base)},
            "changes_before_base": {"http": status_old,
                                    "error": json.loads(body_old)["error"]["code"]},
            "changes_at_head": {"http": status_head, "body": json.loads(body_head)},
            "new_commit": commit_after,
            "stats_after_new_commit": stats_after,
            "journal_frames_after_commit": len(rwal_after["records"]),
            "changes_covering_new_commit": {"http": status_new,
                                            "body": json.loads(body_new)},
            "restart_stats": post_restart_stats,
            "restart_query_http": status_after_restart,
        }
        check(f"restore-{name}-stats-match-image",
              st["sequence"] == decoded[name]["sequence"]
              and st["horizon"] == decoded[name]["horizon"]
              and st["history_base"] == decoded[name]["sequence"]
              and st["journal_records"] == 0,
              f"stats={st}")
        check(f"restore-{name}-journal-base",
              rwal["base"] == decoded[name]["sequence"] and rwal["records"] == [],
              f"base={rwal['base']} frames={len(rwal['records'])}")
        check(f"restore-{name}-changes-before-base-410", status_old == 410,
              f"got {status_old}")
        check(f"restore-{name}-suffix-continues",
              commit_after["sequence"] == decoded[name]["sequence"] + 1
              and len(rwal_after["records"]) == 1
              and rwal_after["records"][0]["sequence"] == decoded[name]["sequence"] + 1,
              f"commit={commit_after} frames={len(rwal_after['records'])}")
        check(f"restore-{name}-restart-durable",
              post_restart_stats["sequence"] == decoded[name]["sequence"] + 1
              and post_restart_stats["horizon"] == decoded[name]["horizon"]
              and status_after_restart == 200,
              f"stats={post_restart_stats}")

    # ------------------------------------------------------------------ #
    # Negative controls
    # ------------------------------------------------------------------ #
    bad_dir = run / "bad-restore"
    tampered = json.loads(blob_a)
    tampered["sha256"] = "00" * 32
    bad_file = run / "tampered.lmi"
    bad_file.write_text(json.dumps(tampered))
    proc = subprocess.run([args.binary, "-restore", str(bad_file),
                           "-data", str(bad_dir), "-listen", "127.0.0.1:0"],
                          capture_output=True, text=True, timeout=15)
    report["tamper_probe"] = {"exit_code": proc.returncode,
                              "stderr_tail": proc.stderr.strip().splitlines()[-1:],
                              "dir_published": bad_dir.exists()}
    check("tampered-digest-rejected", proc.returncode != 0 and not bad_dir.exists(),
          f"rc={proc.returncode}")

    proc2 = subprocess.run([args.binary, "-restore",
                            str(images_dir / filenames["A"]),
                            "-data", str(run / "restored-A"),
                            "-listen", "127.0.0.1:0"],
                           capture_output=True, text=True, timeout=15)
    report["existing_dir_probe"] = {"exit_code": proc2.returncode,
                                    "stderr_tail": proc2.stderr.strip().splitlines()[-1:]}
    check("restore-requires-new-dir", proc2.returncode != 0, f"rc={proc2.returncode}")

    check("image-A-frozen", decoded["A"]["sequence"] == stats5["sequence"]
          and final_stats["sequence"] > decoded["C"]["sequence"],
          f"A={decoded['A']['sequence']} final={final_stats['sequence']}")

    # ------------------------------------------------------------------ #
    # Sizes / retention accounting
    # ------------------------------------------------------------------ #
    total = 0
    for name, filename in filenames.items():
        size = (images_dir / filename).stat().st_size
        total += size
        report["sizes"][filename] = size
    report["sizes"]["total_three_images"] = total
    report["sizes"]["source_journal_bytes"] = wal["bytes"]
    checkpoint = run / "source" / "checkpoint.lmi"
    report["sizes"]["source_checkpoint_bytes"] = (
        checkpoint.stat().st_size if checkpoint.exists() else None)

    # ------------------------------------------------------------------ #
    # Measured reachability and the unprovable gaps
    # ------------------------------------------------------------------ #
    retained_sets = {}
    for name in ("A", "B", "C"):
        reachable = set()
        for at_str, entry in report["restore_probes"][name]["reachable_history"].items():
            if entry["http"] == 200:
                reachable.add(int(at_str))
        retained_sets[name] = reachable
    report["measured_reachable_at_points"] = {
        name: sorted(seq) for name, seq in retained_sets.items()}
    union = set().union(*retained_sets.values())
    report["union_reachable_at_points"] = sorted(union)
    all_missing = [at for at in reach_cases if at not in union]
    historical_gap = [at for at in all_missing if 1 <= at <= decoded["C"]["sequence"]]
    report["gaps"] = {
        "all_non_serving_points": all_missing,
        "historical_gap_unprovable_by_images": historical_gap,
        "pre_relation_point": [at for at in all_missing if at == 0],
        "beyond_newest_image": [at for at in all_missing
                                if at > decoded["C"]["sequence"]],
        "note": "points beyond the newest image still exist as source WAL "
                "frames; they are not frozen into any retained backup.",
    }
    check("historical-gap-is-6-7-8", historical_gap == [6, 7, 8],
          f"measured {historical_gap}")

    report["all_cross_checks_passed"] = all(item["ok"] for item in check_results)
    report["cross_check_count"] = len(check_results)

    with open(args.report, "w") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    log.close()
    print(json.dumps({"report": args.report,
                      "checks": len(check_results),
                      "passed": report["all_cross_checks_passed"],
                      "historical_gap": historical_gap}, ensure_ascii=False))


if __name__ == "__main__":
    main()
