


class data_clean():
    def clean_card_data(self,card_df):
        # to remove 5th and 6th row 
        card_df.drop(card_df.columns[len(card_df.columns)-1], axis=1, inplace=True)
        card_df.drop(card_df.columns[len(card_df.columns)-1], axis=1, inplace=True)

        #rename row 
        card_df.rename(columns = {'0':'index', '1':'card_number',
                              '2':'expiry_data','3':'card_provider','4':'date_payment_confirmed'}, inplace = True)
        
        return card_df
        
    def clean_orders_table(self,orders_df):
        clean_orders_df = orders_df.drop(['first_name', 'last_name', '1'], axis=1)
        return clean_orders_df

    def clean_store_details_table(self,stores_df):
         clean_stores_df = stores_df.drop(['index'], axis=1)
         clean_stores_df = stores_df.drop(stores_df.index[[0]])
         return clean_stores_df