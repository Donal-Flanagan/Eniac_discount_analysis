import pandas as pd


def count_decimal_points(df):
    prices = df[['orderline_id', 'total_paid', 'regular_price', 'sale_price']].copy()

    prices['regular_price_decimal_count'] = prices['regular_price'].str.count(r'\.')
    prices['sale_price_decimal_count'] = prices['sale_price'].str.count(r'\.')

    return prices

def generate_test_data(sales_df):
    decimal_points_per_price = count_decimal_points(sales_df)
    
    # Extract a subset of the data with only one decimal point so we can transform the str values to floats and check our test logic.
    test_data_ids = decimal_points_per_price[
                        (decimal_points_per_price.regular_price_decimal_count == 1) & 
                        (decimal_points_per_price.sale_price_decimal_count == 1)
                    ].orderline_id
    test_data = sales_df[sales_df.orderline_id.isin(test_data_ids)].copy()

    # Transform the price values to float.
    test_data.regular_price = test_data.regular_price.astype(float)
    test_data.sale_price = test_data.sale_price.astype(float)
    
    # This data is corrupted and should fail the tests
    failing_test_data = test_data
    # This subset of the data has regular_price >= promo_price and promo_price >= sale_price and should therefore (hopefully) be uncorrupted
    passing_test_data = test_data[(test_data.regular_price >= test_data.sale_price)]

    return failing_test_data, passing_test_data

def test_col_vals_are_greater_or_equal_to_other(df, greater_col, lesser_col):
    num_incorrect_vals = df[df[greater_col] < df[lesser_col]].shape[0]
    if num_incorrect_vals == 0:
        print(f"All of the {greater_col} values are greater or equal to the corresponding {lesser_col} values.\n")
        return pd.DataFrame()
    else:
        print(f"There are corrupted values in {greater_col} which are less than their corresponding {lesser_col} values.")
        print(f"This respresents {num_incorrect_vals/df.shape[0]*100:.2f}% of the data.\n")
        corrupted_price_orderline_ids = df[df[greater_col] < df[lesser_col]].orderline_id
        return corrupted_price_orderline_ids
    
def test_regular_greater_or_equal_to_sale(df):
    incorrect_val_ids = test_col_vals_are_greater_or_equal_to_other(df, 'regular_price', 'sale_price')
    return incorrect_val_ids

def test_order_total_paid_equal_sum_of_orderlines(df):
    # Group by order_id and calculate the sum of sale_price*product_quantity for all orderline_ids
    grouped_orderlines = df.groupby('order_id').apply(
        lambda x: pd.Series({
            'calculated_total': (x['product_quantity'] * x['sale_price']).sum(),
            'total_paid': x['total_paid'].iloc[0]  # Total paid is the same for all rows in the group
        }),
        include_groups=False
    )

    # Compare calculated_total with total_paid
    incorrect_orders = grouped_orderlines[grouped_orderlines['calculated_total'] != grouped_orderlines['total_paid']]
    
    if incorrect_orders.empty:
        print("All orders have total_paid values equal to the sum of product_quantity * sale_price.")
        return pd.DataFrame()
    else:
        print(f"There are {incorrect_orders.shape[0]} orders where total_paid does not match the sum of product_quantity * sale_price.")
        print(f"This respresents {incorrect_orders.shape[0]/df.shape[0]*100:.2f}% of the data.\n")
        return incorrect_orders