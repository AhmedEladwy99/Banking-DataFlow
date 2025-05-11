import os
import shutil
from datetime import datetime


class Generator:

    @staticmethod
    def generate():
        LANDING = "incoming_data"
        current_day = datetime.now().date().strftime("%Y-%m-%d")
        current_hour = str(datetime.now().hour)

        os.makedirs(LANDING, exist_ok=True)

        SOURCE = "incoming_data/2025-04-18/14"
        DESTINATION = os.path.join(LANDING, current_day, current_hour)

        if os.path.exists(DESTINATION):
            print(f"Destination path {DESTINATION} does not exist. Creating it.")
            return

        os.makedirs(DESTINATION, exist_ok=True)

        for file in os.listdir(SOURCE):
            source_file = os.path.join(SOURCE, file)
            destination_file = os.path.join(DESTINATION, file)
            if os.path.isfile(source_file):
                shutil.copy(source_file, destination_file)
                print(f"Copied {file} to {DESTINATION}")
            else:
                print(f"{file} is not a file and was not copied.")

if __name__ == "__main__":
    Generator.generate()