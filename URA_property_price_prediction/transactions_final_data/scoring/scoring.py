import pandas as pd
from sklearn.metrics import mean_squared_error
import logging
import argparse

logging.basicConfig(format='\n%(asctime)s %(message)s')
session_log = logging.getLogger('Pipeline Log')

def hygiene_checker(pred, ground_truth):

    expected_columns = ['property_key', 'contractDate', 'prediction']
    error_count = 0
    try:
        assert pred.shape[0] == ground_truth.shape[0]
    except:
        session_log.warning(f"Number of records do not meet specifications\n"
                            f"Expected number of records: {ground_truth.shape[0]}\n"
                            f"Submitted number of records: {pred.shape[0]}")

    try:
        assert expected_columns == list(pred.columns)
    except:
        session_log.warning(f"Unexpected columns encountered\n"
                            f"Expected column headers: {expected_columns}\n"
                            f"Submitted number of records: {list(pred.columns)}")
        error_count+=1

    try:
        assert expected_columns == list(pred.columns)
    except:
        session_log.warning(f"Unexpected columns encountered\n"
                            f"Expected column headers: {expected_columns}\n"
                            f"Submitted number of records: {list(pred.columns)}")
        error_count += 1

    try:
        mdf = ground_truth[['property_key', 'contractDate']].merge(pred[['property_key', 'contractDate']],
                                                                on=['property_key', 'contractDate'], how='outer')

        assert mdf['property_key'].shape[0] == ground_truth['property_key'].shape[0]
    except:
        session_log.warning(f"Property keys and lease date mismatch")
        error_count += 1

    return error_count

def ingest_submitted_file(args):

    # read files
    pred = pd.read_csv(args.file_submitted)
    ground_truth = pd.read_csv('test_target.csv')

    # hygiene check
    checker = hygiene_checker(pred, ground_truth)

    # score
    if checker == 0 :
        mdf = ground_truth.merge(pred, on=['property_key', 'contractDate'], how='left')
        rmse = mean_squared_error(mdf['price'], mdf['prediction'])**(1/2)
        session_log.warning(f'Scoring Completed\n'
                            f'RMSE: {rmse}')
    else:
        session_log.warning(f'Scoring Failed\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_submitted', required=True)

    args = parser.parse_args()

    ingest_submitted_file(args)

if __name__ == "__main__":
    main()





