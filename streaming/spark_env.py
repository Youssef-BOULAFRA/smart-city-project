import sys
import os
import subprocess

# ── Java & chemins selon l'OS ─────────────────────────────────────────────────
# ── Java & chemins selon l'OS ─────────────────────────────────────────────────
if os.name == 'nt':
    # Windows
    os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'
    if os.path.exists(r'C:\hadoop\bin\winutils.exe'):
        os.environ['HADOOP_HOME'] = r'C:\hadoop'
        hadoop_bin = r'C:\hadoop\bin'
        os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
    os.environ['SPARK_LOCAL_DIRS'] = r'C:\spark-temp'
    os.makedirs(r'C:\spark-temp', exist_ok=True)
    os.makedirs(r'C:\tmp\spark-checkpoints', exist_ok=True)
else:
    # Linux / macOS
    java_candidates = [
        '/usr/lib/jvm/java-17-openjdk-amd64',
        '/usr/lib/jvm/java-17-openjdk',
        '/usr/lib/jvm/temurin-17',
    ]
    for candidate in java_candidates:
        if os.path.exists(candidate):
            os.environ['JAVA_HOME'] = candidate
            break
    # Utilisation de /tmp (Linux)
    os.environ['SPARK_LOCAL_DIRS'] = '/tmp/spark-temp'
    os.makedirs('/tmp/spark-temp', exist_ok=True)
    os.makedirs('/tmp/spark-checkpoints', exist_ok=True)

# ── Python pour les workers Spark ─────────────────────────────────────────────
os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ── Réseau local ──────────────────────────────────────────────────────────────
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'


# ── JARs ──────────────────────────────────────────────────────────────────────
def get_jars_string():
    """
    Construit la chaîne de JARs locaux à passer à spark.jars.
    Quitte le programme si des JARs sont manquants.
    """
    current_file  = os.path.abspath(__file__)
    streaming_dir = os.path.dirname(current_file)
    project_root  = os.path.dirname(streaming_dir)
    jars_dir      = os.path.join(project_root, 'infra', 'jars')

    if streaming_dir not in sys.path:
        sys.path.insert(0, streaming_dir)

    from configuration.config import JARS_REQUIRED

    jars_list = [os.path.join(jars_dir, j) for j in JARS_REQUIRED]
    missing   = [j for j in jars_list if not os.path.exists(j)]

    if missing:
        print("❌ JARs manquants dans infra/jars/ :")
        for j in missing:
            print(f"   {os.path.basename(j)}")
        print("\n💡 Télécharge-les avec :")
        print("   cd infra/jars && bash download_jars.sh")
        sys.exit(1)

    return ",".join(jars_list)


# ── sys.path ──────────────────────────────────────────────────────────────────
def add_streaming_to_path():
    """Ajoute streaming/ au sys.path pour les imports relatifs."""
    current_file  = os.path.abspath(__file__)
    streaming_dir = os.path.dirname(current_file)
    if streaming_dir not in sys.path:
        sys.path.insert(0, streaming_dir)
