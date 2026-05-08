# TriWave Session Storage

- GitHub stores source code only; it does **not** store live runtime recorded files.
- Railway container filesystem can be ephemeral and may reset after restart/redeploy.
- For permanent session files, attach a Railway Volume and set:
  - `TRIWAVE_SESSION_BASE_DIR=/data/triwave_sessions`
- Without a volume, files under local container paths may disappear.

## Analysis command

```bash
python scripts/analyze_triwave_session.py --date YYYY-MM-DD --expiry unknown
```
