import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pipeline import Pipeline
from concurrent.futures import ThreadPoolExecutor

def process_new_file(file_path):
    print(f"New file detected: {file_path}")
    with open(file_path, 'r') as f:
        content = f.read()
        print("File content:")
        print(content)

def run_pipeline(file_path):

    try:
        file_name = os.path.basename(file_path)
        pipeline = Pipeline(
            input_file_path=file_path,
            output_path=os.path.join("sink", "silver", file_name.split('.')[0]),
            output_file_format="parquet"
        )
        pipeline.run()
        print(f"Pipeline completed for: {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

class FileCreatedHandler(FileSystemEventHandler):
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=4)

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            print(f"File created: {file_path}")
            
            self.executor.submit(run_pipeline, file_path)

if __name__ == "__main__":
    path_to_watch = "incoming_data"
    os.makedirs(path_to_watch, exist_ok=True)

    event_handler = FileCreatedHandler()
    observer = Observer()
    observer.schedule(event_handler, path=path_to_watch, recursive=True)
    observer.start()

    print(f"Watching directory: {path_to_watch}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        event_handler.executor.shutdown(wait=True)
        print("Observer stopped and threads shut down.")
    
    observer.join()