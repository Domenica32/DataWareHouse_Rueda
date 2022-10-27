from datetime import datetime

def join_2_strings(string1, string2):
    return f"{string1} {string2}"
    
def obt_gender(gen):
    if gen == 'M':
        return 'MASCULINO'
    elif gen == 'F':
        return 'FEMENINO'
    else:
        return 'NO DEFINIDO'

def obt_date(date_string):
    return datetime.strptime(date_string, '%Y-%m-%d')
    


def month_date_numeric(date_string):

    fecha =  datetime.strptime(date_string,'%d-%b-%y')

    return (fecha)

def date_numeric(fecha_str):
    fecha_date = datetime.strptime(fecha_str, '%d-%b-%y')
    fecha_numerica= int(fecha_date.strftime("%d%m%y"))
    return (fecha_numerica)

def mapeoCountries(countrie_id, countriesor):
        
        arreglo=dict()
        if not countriesor.empty:
            for id,cou_id \
                in zip(countriesor['ID'],countriesor['COUNTRY_ID']):
                arreglo[cou_id] = id
        return arreglo[countrie_id]