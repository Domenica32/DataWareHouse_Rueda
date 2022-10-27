from datetime import datetime

#country
def country_subKey(id, idSor):
        
        country_Sub=dict()
        if not idSor.empty:
            for id_two,cou_id in zip(idSor['ID'],idSor['COUNTRY_ID']):
                country_Sub[cou_id] = id_two
        return country_Sub[id]


#customer
def customer_subKey(id, idSor):
        
        customer_Sub=dict()
        if not idSor.empty:
            for id_two,cust_id in zip(idSor['ID'],idSor['CUST_ID']):
                customer_Sub[cust_id] = id_two
        return customer_Sub[id]


#promotions
def     promo_subKey (id, idSor):
        prom_Sub =dict()
        if not idSor.empty:
            for id_two, promo_id in zip(idSor['ID'],idSor['PROMO_ID']):
                prom_Sub[promo_id] = id_two
        return prom_Sub[id]


#channels
def     channel_subKey (id, idSor):
        channels_Sub=dict()
        if not idSor.empty:
            for id_two, channel_id in zip(idSor['ID'],idSor['CHANNEL_ID']):
                channels_Sub[channel_id] = id_two
        return channels_Sub[id]



#product
def product_subKey(id, idSor): 
    product_Sub=dict()
    if not idSor.empty:
            for id_two, pro_id in zip(idSor['ID'],idSor['PROD_ID']):
                product_Sub[pro_id] = id_two
    return product_Sub[id]     






