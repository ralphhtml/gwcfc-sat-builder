#!/usr/bin/env python3
"""Draws the satellite pictures for the Guta Forecasting Center radar site and uploads them, signed.

    python3 sat.py --group east-meso --loop 320      # keep going for 320 minutes
    python3 sat.py --group east-conus --once --dry   # one pass, upload nothing

One job a group of sectors. A job goes round its sectors for a few hours: each time, any scan that has come out is
drawn, the new pictures are uploaded, and the server is told (a check-in) that this job is alive. The server keeps
the lists of frames; only pictures are sent. While the check-ins keep coming the server's own satellite jobs draw
nothing, and they start again by themselves if these stop.

The drawing code is pipeline.bin, encrypted: it is not readable from this repository. The server's address and the
signing key come from the environment (repository secrets) and are never printed.
"""
import argparse
import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PART_BYTES = 30 * 1024 * 1024

# group: (seconds between passes, [what to draw])
GROUPS = {
    # Four jobs, not nine: GitHub runs twenty jobs at a time for the whole account, and the models, the small jobs
    # and the archive builder need room too. The small sectors (a few seconds each) share one job and still come
    # round every minute; the big ones share by how long a pass takes.
    "boxes":    (60,  [("east", "meso1"), ("east", "meso2"), ("west", "meso1"), ("west", "meso2"),
                       ("himawari", "japan"), ("himawari", "target")]),
    "conus":    (120, [("east", "conus"), ("west", "conus")]),
    "fulldisk": (120, [("east", "fulldisk"), ("west", "fulldisk")]),
    "world":    (120, [("himawari", "fulldisk"), ("global", None), ("eumetsat", None)]),
}

# The builder keeps only the last couple of hours on its own disk: the server holds the archive.
os.environ.setdefault("GWCFC_SAT_KEEP_HOURS", "2")
os.environ.setdefault("GWCFC_NO_UPLOADS", "1")


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()), msg, flush=True)


def unlock_pipeline():
    """The drawing code, unpacked to a temporary folder and put on the import path. The key is derived from the
    signing key, so only a job here (or the owner) can unlock it."""
    plain = os.path.join(HERE, "pipeline")
    if os.path.isdir(plain):
        sys.path.insert(0, plain)
        return True
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    body = "".join(ln.strip() for ln in key.splitlines() if ln.strip() and not ln.startswith("-----"))
    if not body:
        return False
    env = dict(os.environ, GWCFC_BUNDLE_PASS=hashlib.sha256(body.encode()).hexdigest())
    out = tempfile.mkdtemp(prefix="pl-")
    dec = subprocess.run(["openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-in",
                          os.path.join(HERE, "pipeline.bin"), "-pass", "env:GWCFC_BUNDLE_PASS"],
                         capture_output=True, env=env)
    if dec.returncode != 0:
        return False
    with tarfile.open(fileobj=io.BytesIO(dec.stdout), mode="r:gz") as tf:
        tf.extractall(out, filter="data")
    sys.path.insert(0, out)
    return True


def sign(msg, key_path):
    fd, mp = tempfile.mkstemp(prefix="m-")
    with os.fdopen(fd, "wb") as fh:
        fh.write(msg)
    try:
        return subprocess.run(["openssl", "pkeyutl", "-sign", "-inkey", key_path, "-rawin", "-in", mp],
                              capture_output=True, check=True).stdout
    finally:
        os.unlink(mp)


def post(base, key_path, run, kind, part, body, tries=5):
    import requests
    last = ""
    for n in range(tries):
        sha = hashlib.sha256(body).hexdigest()
        ts = str(int(time.time()))
        msg = "\n".join(["gwcfc-upload-v1", "tree", "satellite", run, kind, str(part), sha, ts]).encode()
        headers = {"X-GWCFC-Model": "tree", "X-GWCFC-Region": "satellite", "X-GWCFC-Run": run, "X-GWCFC-Kind": kind,
                   "X-GWCFC-Part": str(part), "X-GWCFC-Sha256": sha, "X-GWCFC-Ts": ts,
                   "X-GWCFC-Sig": base64.b64encode(sign(msg, key_path)).decode(),
                   "Content-Type": "application/octet-stream", "User-Agent": "gwcfc-sat-builder"}
        try:
            r = requests.post(base + "/models/upload", data=body, headers=headers, timeout=300)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code} {r.text[:200]}"
            if r.status_code in (400, 403, 409, 413):
                break
        except Exception as e:                          # noqa: BLE001
            last = e.__class__.__name__                 # the address is in the message, so only the kind is kept
        time.sleep(min(40, 4 * 2 ** n))
    raise RuntimeError(f"upload of {kind} {part} failed: {last}")


def new_files(root, rel, sent):
    """{frame: [file names]} under one sector folder that have not been sent yet (whole files only)."""
    out = {}
    d = os.path.join(root, *rel.split("/"))
    try:
        frames = sorted(x for x in os.listdir(d) if x[:1].isdigit() and os.path.isdir(os.path.join(d, x)))
    except OSError:
        return out
    for fr in frames:
        for n in sorted(os.listdir(os.path.join(d, fr))):
            p = os.path.join(d, fr, n)
            if n.endswith(".part") or n.startswith("."):
                continue
            try:
                size = os.path.getsize(p)
            except OSError:
                continue
            if size > 0 and sent.get(f"{rel}/{fr}/{n}") != size:
                out.setdefault(fr, []).append(n)
    return out


def frames_for(root, rel, stamps):
    """What the server is asked to list: the local manifest's entries for the frames just sent."""
    try:
        with open(os.path.join(root, *rel.split("/"), "manifest.json")) as fh:
            man = json.load(fh)
    except (OSError, ValueError):
        return None
    prods = {}
    for key, p in (man.get("products") or {}).items():
        rows = [f for f in (p.get("frames") or []) if f.get("t") in stamps]
        if rows:
            prods[key] = {"label": p.get("label"), "built": p.get("built"), "frames": rows}
    head = {k: v for k, v in man.items() if k not in ("products", "updated") and isinstance(v, (str, int, float))}
    return {"dir": rel, "head": head, "products": prods} if prods else None


def upload(base, key_path, root, job, rels, sent, state, dry):
    """Send what is new under these sector folders, then check in. Returns how many files went."""
    run = time.strftime("%Y%m%d_%H", time.gmtime())
    total = 0
    for rel in rels:
        fresh = new_files(root, rel, sent)
        if not fresh:
            continue
        names = [(fr, n) for fr in sorted(fresh) for n in fresh[fr]]
        if dry:
            log(f"  {rel}: {len(names)} new files in {len(fresh)} frames (not sent)")
            for fr, n in names:
                sent[f"{rel}/{fr}/{n}"] = os.path.getsize(os.path.join(root, *rel.split("/"), fr, n))
            continue
        batch, size, part = [], 0, 0

        def flush():
            nonlocal batch, size, part
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tf:
                for fr, n in batch:
                    tf.add(os.path.join(root, *rel.split("/"), fr, n), arcname=f"{rel}/{fr}/{n}", recursive=False)
            post(base, key_path, run, "part", part, buf.getvalue())
            part += 1
            batch, size = [], 0
        sizes = {}
        for fr, n in names:
            s = os.path.getsize(os.path.join(root, *rel.split("/"), fr, n))
            sizes[(fr, n)] = s
            if batch and size + s > PART_BYTES:
                flush()
            batch.append((fr, n))
            size += s
        if batch:
            flush()
        body = {"job": job}
        fr_spec = frames_for(root, rel, set(fresh))
        if fr_spec:
            body["frames"] = fr_spec
        got = post(base, key_path, run, "commit", part, json.dumps(body).encode())
        for (fr, n), s in sizes.items():
            sent[f"{rel}/{fr}/{n}"] = s
        total += len(names)
        state["checked_in"] = time.time()
        log(f"  {rel}: sent {len(names)} files, {sum(sizes.values()) / 1e6:.1f} MB; the server listed "
            f"{got.get('listed', 0)} new frames")
    # The check-in goes even with nothing new, every five minutes: it is what keeps the server's own job stood down
    # (the server waits half an hour before taking a sector back). Not more often: every request is work for the
    # server's web process, which answers the site too (2026-11-23: thirteen jobs checking in every minute beside
    # the model uploads, and the site's requests took 1 to 11 seconds).
    if not dry and time.time() - state.get("checked_in", 0) > 300:
        post(base, key_path, run, "commit", 0, json.dumps({"job": job}).encode())
        state["checked_in"] = time.time()
    # Forget files the local prune has let go of, so the note of what was sent does not grow for ever.
    if len(sent) > 20000:
        for k in [k for k in sent if not os.path.exists(os.path.join(root, *k.split("/")))]:
            sent.pop(k, None)
    return total


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True, choices=sorted(GROUPS))
    ap.add_argument("--loop", type=int, default=0, help="minutes to keep going")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry", action="store_true", help="draw, do not upload")
    a = ap.parse_args(argv)
    base = (os.environ.get("GWCFC_BASE") or "").strip().rstrip("/")
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    if not a.dry and (not base or "PRIVATE KEY" not in key):
        print("::warning::Nothing drawn: add the repository secrets GWCFC_BASE and GWCFC_UPLOAD_KEY "
              "(Settings > Secrets and variables > Actions).", flush=True)
        return 0          # not a failure: the run's page shows the note
    if not unlock_pipeline():
        print("::warning::Nothing drawn: the code could not be unlocked (is GWCFC_UPLOAD_KEY the right key?)", flush=True)
        return 0          # not a failure: the run's page shows the note
    key_path = None
    if not a.dry:
        fd, key_path = tempfile.mkstemp(prefix="k-")
        with os.fdopen(fd, "w") as fh:
            fh.write(key.strip() + "\n")
        os.chmod(key_path, 0o600)

    import satellite_pipeline as SP
    import sat_engine as SE
    period, items = GROUPS[a.group]
    root = SP.OUT_DIR
    os.makedirs(root, exist_ok=True)
    eng = SE.Engine()
    E = None
    sent = {x: {} for x in range(len(items))}
    state = {x: {} for x in range(len(items))}
    end = time.time() + a.loop * 60
    n_pass = 0
    while True:
        t0 = time.time()
        for i, (sat_key, sector) in enumerate(items):
            try:
                if sat_key == "global":
                    built = SP.build_global()
                    job, rels = "sat-global", [f"global/{sk}" for sk in SP.GLOBAL_SECTORS]
                elif sat_key == "eumetsat":
                    if E is None:
                        import eumetsat_pipeline as E_
                        E = E_
                    built = E.run_pass(list(E.SATS))
                    job, rels = "sat-eumetsat", [f"eumetsat/{sk}" for sk in E.SATS]
                else:
                    built = eng.build_sector(sat_key, sector)
                    job, rels = f"sat-{sat_key}-{sector}", [f"{sat_key}/{sector}"]
                n = upload(base, key_path, root, job, rels, sent[i], state[i], a.dry)
                if built or n:
                    log(f"{job}: drew {built}, sent {n} files")
            except Exception as e:                      # noqa: BLE001
                log(f"{sat_key}/{sector}: {e.__class__.__name__}: {str(e)[:160] if 'http' not in str(e).lower() else ''}")
        n_pass += 1
        if a.once or time.time() + period > end:
            break
        time.sleep(max(2, period - (time.time() - t0)))
    log(f"{a.group}: {n_pass} passes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
