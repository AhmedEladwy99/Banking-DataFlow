import os
from utils import PipelineLogger
from pipeline import Pipeline
from datetime import datetime

logger = PipelineLogger(__name__).get_logger()

def getDayAndHour():
    current_day = datetime.now().date().strftime("%Y-%m-%d")
    hour = datetime.now().hour
    return current_day, str(hour)

class Workflow:

    def __init__(self):
        self.pipelines_file_names = []
        self.pipelines = []
        self.source_path = 'incoming_data'
        self.sink_path = 'sink'

    def create_pipeline(self, file_name: str):

        new_pipeline = Pipeline(
            input_file_path=os.path.join(self.source_path, *getDayAndHour(), file_name),
            output_path=os.path.join(self.sink_path, *getDayAndHour(), file_name.split('.')[0])
            , output_file_format='parquet'
        )

        self.pipelines.append(new_pipeline)
        self.pipelines_file_names.append(file_name)

    def run(self):
        self._before_run()
        for _ in range(len(self.pipelines)):
            pipeline = self.pipelines[_]
            try:
                pipeline.run()
                logger.info(f"Pipeline {pipeline} completed successfully")
            except Exception as e:
                logger.error(f"Error running pipeline {pipeline}: {e}")

    def _before_run(self):
        self.refactor()

    def refactor(self):
        
        for _ in range(len(self.pipelines_file_names)):

            file_name = self.pipelines_file_names[_]
            updated_input_path = os.path.join(self.source_path, *getDayAndHour(), file_name)
            updated_output_path = os.path.join(self.sink_path, "silver", file_name.split('.')[0])
            self.pipelines[_].setInputPath(updated_input_path)
            self.pipelines[_].setOutputPath(updated_output_path)

def main():
    workflow = Workflow()
    workflow.create_pipeline('customer_profiles.csv')
    workflow.create_pipeline('loans.txt')

    workflow.create_pipeline('support_tickets.csv')
    workflow.create_pipeline('credit_cards_billing.csv')
    workflow.create_pipeline('transactions.json')

    workflow.run()

if __name__ == "__main__":
    main()

# crontab -e
# 20 * * * * /usr/bin/python3 ~/TeleConnect-DataFlow/workflow.py