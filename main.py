from .workflow import Workflow

def main():
    workflow = Workflow()
    workflow.create_pipeline('customer_profiles.csv')
    workflow.create_pipeline('loans.txt')
    workflow.create_pipeline('transactions.json')
    workflow.create_pipeline('billing.csv')
    workflow.create_pipeline('support_tickets.csv')
    workflow.run()