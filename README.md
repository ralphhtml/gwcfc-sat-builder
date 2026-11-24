# gwcfc-sat-builder

Draws the satellite pictures for the Guta Forecasting Center radar site (https://radar.gwcfc.net/) on GitHub's
machines and uploads them to the site's parsing server: GOES-East and GOES-West (CONUS, the mesoscale boxes and the
full disks), Himawari (full disk, Japan and Target), the global mosaic and Meteosat.

## How it works

- `.github/workflows/sat.yml` runs one job a group of sectors. Each job goes round its sectors for a few hours, and
  the next run is started when they end.
- `sat.py` draws any scan that has come out, uploads the new pictures in signed parts, and checks in with the server.
  The server keeps the lists of frames and the archive; only pictures are sent.
- `pipeline.bin` is the parsing server's own drawing code, encrypted (AES-256). It is not readable from this
  repository: a job unlocks it with a key derived from a repository secret.

## Security

- Two repository secrets are needed: `GWCFC_BASE` (the server's address) and `GWCFC_UPLOAD_KEY` (a private signing
  key). Neither is ever printed or written to a log, and neither belongs in this repository.
- Every upload is signed. The server holds only the matching public key and refuses anything that is not signed
  and recent. Nobody without the private key can upload.
- The workflow never runs on pull requests, so code from a fork never sees the secrets.

## Start it by hand

Actions > Satellite pictures > Run workflow.
