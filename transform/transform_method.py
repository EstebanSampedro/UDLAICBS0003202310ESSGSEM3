from datetime import datetime

#variable types methods
def join_two_strings(str1, str2):
    return f"{str1} {str2}"

def str_int(str):
    try:
        x = int(str)
    except:
        x = None
        print("Only string allowed")
    return x


def str_char(str):
    return str[0]


def str_to_date(str):
    return datetime.strptime(str, "%d-%b-%y")


def str_float(str):
    try:
        x = float(str)
    except:
        x = None
        print("Only string allowed")
    return x


def month_n(str):
    x = datetime.strptime(str, "%m")
    return x.strftime("%b")

def obt_gender(gen):
    if gen == 'M':
        return 'MASCULINO'
    elif gen == 'F':
        return 'FEMENINO'
    else:
        return 'NO DEFINIDO'

def obt_date(date_string):
    return datetime.strptime(date_string, "%d-%b-%y")

def obt_month_name(string_month):
    month = datetime.strptime(string_month, "%m")
    return month.strftime("%B")


