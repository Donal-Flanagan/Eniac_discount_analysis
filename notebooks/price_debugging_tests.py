import pandas as pd

def test_col_vals_are_greater_than_other(df, greater_col, lesser_col):
    num_incorrect_vals = df[df[greater_col] < df[lesser_col]].shape[0]
    if num_incorrect_vals == 0:
        print(f"All of the {greater_col} values are greater than the corresponding {lesser_col} values.\n")
        return pd.DataFrame()
    else:
        print(f"There are corrupted values in {greater_col} which are less than their corresponding {lesser_col} values.")
        print(f"This respresents {num_incorrect_vals/df.shape[0]*100:.2f}% of the data.\n")
        corrupted_price_orderline_ids = df[df[greater_col] < df[lesser_col]].orderline_id
        return corrupted_price_orderline_ids

# test_data[test_data.regular_price < test_data.promo_price].shape[0]

def test_regular_greater_than_promo(df):
    incorrect_val_ids = test_col_vals_are_greater_than_other(df, 'regular_price', 'promo_price')
    return incorrect_val_ids
    
def test_regular_greater_than_sale(df):
    incorrect_val_ids = test_col_vals_are_greater_than_other(df, 'regular_price', 'sale_price')
    return incorrect_val_ids

def test_promo_greater_than_sale(df):
    incorrect_val_ids = test_col_vals_are_greater_than_other(df, 'promo_price', 'sale_price')
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