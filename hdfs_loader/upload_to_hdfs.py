import os
import subprocess

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(PROJECT_ROOT, "sink", "silver")

HDFS_PATH = "/user/root/silver"

DOCKER_CONTAINER = "master1"

def get_latest_parquet_file(folder_path):
    """Return path of the most recent .parquet file in a folder."""
    files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
    if not files:
        return None
    files = [os.path.join(folder_path, f) for f in files]
    return max(files, key=os.path.getmtime)

def upload_to_hdfs(local_path, hdfs_target_dir):
    """Upload a file to HDFS via docker exec into the Hadoop container."""
    try:
        subprocess.run(["docker", "cp", local_path, f"{DOCKER_CONTAINER}:/tmp/"], check=True)

        subprocess.run([
            "docker", "exec", DOCKER_CONTAINER,
            "hdfs", "dfs", "-mkdir", "-p", hdfs_target_dir
        ], check=True)

        filename = os.path.basename(local_path)
        subprocess.run([
            "docker", "exec", DOCKER_CONTAINER,
            "hdfs", "dfs", "-put", "-f", f"/tmp/{filename}", os.path.join(hdfs_target_dir, filename)
        ], check=True)

        print(f"Uploaded to HDFS: {local_path} → {hdfs_target_dir}/{filename}")

    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {local_path}: {e}")

def main():
    # Loop over all datasets inside sink/silver/
    for dataset_folder in os.listdir(SILVER_DIR):
        dataset_path = os.path.join(SILVER_DIR, dataset_folder)
        if not os.path.isdir(dataset_path):
            continue

        latest_file = get_latest_parquet_file(dataset_path)
        if latest_file:
            target_hdfs_path = os.path.join(HDFS_PATH, dataset_folder)
            upload_to_hdfs(latest_file, target_hdfs_path)

if __name__ == "__main__":
    main()
