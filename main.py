from .workflow import Workflow

def main():
    workflow = Workflow()
    workflow.create_pipeline('customer_profiles.csv')
    workflow.create_pipeline('loans.txt')
    workflow.run()