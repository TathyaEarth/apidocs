import argparse
import json
import logging

from usage import Product, get_token, get_indexes_data, get_subscribed_indexes, set_credentials

logging.basicConfig(encoding='utf-8', level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tathya API Usage.')
    parser.add_argument('--username', dest='username', required=True, help='Tathya Account username')
    parser.add_argument('--password', dest='password', required=True, help='Tathya Account password')
    args = parser.parse_args()
    product = Product.HOTMETAL
    # 1. Set your tathya credentials and get_token
    set_credentials(args.username, args.password)
    get_token()
    # 2. Get list of all the subscribed indexes type {"name": "Title of identifier", "identifier": "Index Identifier"} of HOTMETAL product
    indexes = get_subscribed_indexes(product=product)
    index_identifier_list = list(map(lambda x: x.get("identifier"), indexes))
    # 3. Get the indexes data for all subscribed indexes
    #    Lets take data for full series data
    #    passing end_date as None to get the latest data
    start_date = '2018-01-01'
    indexes_data = get_indexes_data(product=product, start_date=start_date, end_date=None, identifiers=index_identifier_list)
    out_file_name = "indexes_result.json"
    logging.info(f"Generating file for writing output {out_file_name}")
    with open(out_file_name, 'w') as f:
        f.write(json.dumps(indexes_data))
