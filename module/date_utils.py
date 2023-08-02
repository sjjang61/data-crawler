from datetime import datetime
from dateutil.relativedelta import relativedelta

def date_to_str( dt, fmt = '%Y-%m-%d' ):
    return dt.strftime( fmt )

def str_to_date( str, fmt = '%Y-%m-%d'):
    return datetime.strptime( str, fmt ).date()


def get_month_list( start_date, end_date, in_fmt = '%Y-%m-%d', out_fmt = '%Y-%m-%d' ):

  month_list = []
  base_date = str_to_date( start_date, in_fmt )

  for idx in range(10000):
    next_month = base_date + relativedelta(months=idx)
    next_month_str = date_to_str( next_month, out_fmt )
    # print( next_month_str )
    month_list.append( next_month_str )

    if next_month_str == end_date:
        break

  return month_list