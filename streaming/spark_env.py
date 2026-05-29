import sys
import os
import subprocess

# --- Java ---
os.environ['JAVA_HOME']            = r'C:\Program Files\Java\jdk-21'

# --- Python pour les workers Spark ---
def _windows_short_path(path):
    if os.name != 'nt' or ' ' not in path:
        return path
    try:
        result = subprocess.run(
            ['cmd', '/c', f'for %I in ("{path}") do @echo %~sI'],
            capture_output=True,
            text=True,
            check=True,
        )
        short_path = result.stdout.strip().splitlines()[-1].strip()
        return short_path or path
    except Exception:
        return path

def _map_venv_drive_letter():
    if os.name != 'nt':
        return None
    venv_root = os.path.dirname(os.path.dirname(sys.executable))
    if ' ' not in venv_root:
        return None
    drive_letter = 'X:'
    try:
        subprocess.run(
            ['subst', drive_letter, venv_root],
            capture_output=True,
            text=True,
            check=True,
        )
        return drive_letter
    except Exception:
        return None

_venv_drive = _map_venv_drive_letter()
if _venv_drive:
    _python_executable = rf'{_venv_drive}\Scripts\python.exe'
else:
    _python_executable = _windows_short_path(sys.executable)

os.environ['PYSPARK_PYTHON']        = _python_executable
os.environ['PYSPARK_DRIVER_PYTHON'] = _python_executable

# --- FIX CRITIQUE Windows: winutils.exe doit exister dans C:\hadoop\bin ---
# Télécharger depuis https://github.com/cdarlint/winutils (hadoop-3.3.5)
if os.path.exists(r'C:\hadoop\bin\winutils.exe'):
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    hadoop_bin = r'C:\hadoop\bin'
    os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
elif 'HADOOP_HOME' in os.environ:
    os.environ.pop('HADOOP_HOME', None)

# --- Évite les erreurs de binding réseau sur Windows ---
os.environ['SPARK_LOCAL_IP']        = '127.0.0.1'

# --- Dossier temporaire Spark (évite les conflits avec le dossier système) ---
os.environ['SPARK_LOCAL_DIRS']      = r'C:\spark-temp'
os.makedirs(r'C:\spark-temp', exist_ok=True)
os.makedirs(r'C:\tmp\spark-checkpoints', exist_ok=True)


def get_jars_string():
    """
    Construit la chaîne de JARs locaux à passer à spark.jars.
    Quitte le programme si des JARs sont manquants.
    """
    import sys as _sys

    current_file  = os.path.abspath(__file__)
    # APRÈS (correct)
    streaming_dir = os.path.dirname(current_file)  # streaming/
    project_root = os.path.dirname(streaming_dir)  # smart-city-project/
    jars_dir      = os.path.join(project_root, 'infra', 'jars')

    # Import ici pour éviter la dépendance circulaire
    _sys.path.insert(0, streaming_dir) if streaming_dir not in _sys.path else None
    from configuration.config import JARS_REQUIRED

    jars_list   = [os.path.join(jars_dir, j) for j in JARS_REQUIRED]
    missing     = [j for j in jars_list if not os.path.exists(j)]

    if missing:
        print("❌ JARs manquants dans infra/jars/ :")
        for j in missing:
            print(f"   {os.path.basename(j)}")
        print("\n💡 Télécharge-les depuis :")
        print("   https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10")
        print("   (version 2.12-3.5.1 pour Spark 3.5 + Scala 2.12)")
        _sys.exit(1)

    return ",".join(jars_list)


def add_streaming_to_path():
    """Ajoute streaming/ au sys.path pour les imports relatifs."""
    current_file  = os.path.abspath(__file__)
    # spark_env.py est dans streaming/
    streaming_dir = os.path.dirname(current_file)
    if streaming_dir not in sys.path:
        sys.path.insert(0, streaming_dir)