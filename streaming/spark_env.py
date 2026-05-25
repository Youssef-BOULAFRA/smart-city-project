import sys
import os

# --- Java ---
os.environ['JAVA_HOME']            = r'C:\Program Files\Java\jdk-11.0.30'

# --- Python pour les workers Spark ---
os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- FIX CRITIQUE Windows: winutils.exe doit exister dans C:\hadoop\bin ---
# Télécharger depuis https://github.com/cdarlint/winutils (hadoop-3.3.5)
os.environ['HADOOP_HOME']           = r'C:\hadoop'

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