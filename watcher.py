
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pipeline import Pipeline


def process_new_file(file_path):
    print(f"New file detected: {file_path}")
    with open(file_path, 'r') as f:
        content = f.read()
        print("File content:")
        print(content)

class FileCreatedHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            print(file_path)
            file_name = os.path.basename(file_path)
            pipeline = Pipeline(input_file_path=file_path, output_path=os.path.join("sink", "silver", file_name.split('.')[0]), output_file_format="parquet")
            pipeline.run()

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
    
    observer.join()
