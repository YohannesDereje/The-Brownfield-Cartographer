import os
import shutil
from pathlib import Path
from src.agents.hydrologist import Hydrologist
from src.models.nodes import ModuleNode

def test_full_lineage_flow():
    # 1. Create a physical temp environment
    temp_root = Path("./test_sandbox")
    if temp_root.exists():
        shutil.rmtree(temp_root)
    
    (temp_root / "data").mkdir(parents=True)
    (temp_root / "scripts").mkdir(parents=True)
    (temp_root / "models").mkdir(parents=True)
    (temp_root / "dags").mkdir(parents=True)

    # 2. Define contents
    csv_content = "id,name,email"
    py_content = "import pandas as pd\ndf = pd.read_csv('data/raw_users.csv')\ndf.to_sql('stg_users', con='engine')"
    sql_content = "CREATE TABLE analytics.dim_users AS SELECT * FROM stg_users"
    dag_content = "from airflow import DAG\ningest >> transform"

    # 3. Write real files so Path.read_text() works
    (temp_root / "data/raw_users.csv").write_text(csv_content)
    (temp_root / "scripts/ingest.py").write_text(py_content)
    (temp_root / "models/transform_users.sql").write_text(sql_content)
    (temp_root / "dags/user_pipeline.py").write_text(dag_content)

    # 4. Create nodes using the real relative paths
    nodes = [
        ModuleNode(path=str(temp_root / "data/raw_users.csv"), language="csv"),
        ModuleNode(path=str(temp_root / "scripts/ingest.py"), language="python"),
        ModuleNode(path=str(temp_root / "models/transform_users.sql"), language="sql"),
        ModuleNode(path=str(temp_root / "dags/user_pipeline.py"), language="python"),
    ]

    h = Hydrologist()
    
    print("--- 1. Hydrating Lineage ---")
    # This should now find the files and not log "FileNotFoundError"
    h.hydrate_repository_lineage(nodes)
    
    print("\n--- 2. System Summary ---")
    summary = h.generate_lineage_summary(nodes)
    print(summary)

    print("\n--- 3. Blast Radius Test ---")
    target = str(temp_root / "data/raw_users.csv")
    impact = h.get_blast_radius(target, nodes)
    print(f"Impact of changing '{target}':")
    for item in impact:
        print(f" - {item}")

    # Cleanup
    # shutil.rmtree(temp_root)

if __name__ == "__main__":
    test_full_lineage_flow()