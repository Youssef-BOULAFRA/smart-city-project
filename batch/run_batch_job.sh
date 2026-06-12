#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PYTHON="$PROJECT_ROOT/venv/Scripts/python.exe"

if [[ -x "$VENV_PYTHON" ]]; then
  PYTHON_BIN="$VENV_PYTHON"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

LOG_FILE="$SCRIPT_DIR/batch_scheduler.log"

echo "[$(date -Iseconds)] Démarrage du batch ETL" | tee -a "$LOG_FILE"
"$PYTHON_BIN" "$SCRIPT_DIR/batch_writer.py" >> "$LOG_FILE" 2>&1

echo "[$(date -Iseconds)] Démarrage de l'analyse historique" | tee -a "$LOG_FILE"
"$PYTHON_BIN" "$SCRIPT_DIR/batch_analysis.py" >> "$LOG_FILE" 2>&1

echo "[$(date -Iseconds)] Fin du batch" | tee -a "$LOG_FILE"

# Exemple d'entrée crontab :
# 0 * * * * /bin/bash /chemin/vers/batch/run_batch_job.sh >> /chemin/vers/batch/batch_scheduler.log 2>&1