# -*- coding: utf-8 -*-
import datetime as dt
from typing import Tuple
import pytz
from dateutil.relativedelta import relativedelta
import calendar

TZ_LIMA = pytz.timezone('America/Lima')

def get_current_lima_time() -> dt.datetime:
    """Get current time in Lima timezone"""
    return dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

def get_date_parts() -> Tuple[str, str, str]:
    """Get current year, month, day in Lima timezone"""
    now = get_current_lima_time()
    return now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')

def transform_to_datetime(date_str: str) -> dt.datetime:
    """Convert string date to datetime object"""
    return dt.datetime(
        year=int(date_str[:4]),
        month=int(date_str[5:7]),
        day=int(date_str[8:10]),
        hour=int(date_str[11:13]),
        minute=int(date_str[14:16]),
        second=int(date_str[17:19])
    )

def format_date_for_db(date_str: str, date_type: str) -> str:
    """Format date string for specific database types"""
    if date_type == 'smalldatetime':
        return f"CONVERT(smalldatetime, '{date_str}', 120)"
    elif date_type == 'DATE':
        return f"TO_DATE('{date_str[:19]}', 'YYYY-MM-DD HH24:MI:SS')"
    elif date_type == 'TIMESTAMP(6)':
        return f"TO_TIMESTAMP('{date_str}', 'YYYY-MM-DD HH24:MI:SS.FF')"
    elif date_type == 'SQL_DATETIME':
        return f"CONVERT(DATETIME, '{date_str}', 102)"
    elif date_type == 'BIGINT':
        end_dt = dt.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return str(int(end_dt.timestamp()))
    else:
        return f"'{date_str}'"
    
    # utils/date_utils.py - nueva función

def get_date_limits_with_range(
    delay_ini: str, 
    delay_end: str, 
    data_type: str
) -> Tuple[str, str]:
    """
    Get date limits using both start and end delays
    
    :param delay_ini: Months to subtract for start date (e.g., "-3")
    :param delay_end: Months to subtract for end date (e.g., "0" or "-1")
    :param data_type: Data type for formatting
    :return: Tuple of (lower_limit, upper_limit)
    """
    delay_ini = delay_ini.strip().replace("'", "")
    delay_end = delay_end.strip().replace("'", "") if delay_end else "0"
    
    current_time = get_current_lima_time()
    
    # Calcular fecha de inicio (delay_ini meses atrás)
    start_date = current_time - relativedelta(months=(-1 * int(delay_ini)))
    
    # Calcular fecha de fin (delay_end meses atrás)
    end_date = current_time - relativedelta(months=(-1 * int(delay_end)))
    
    if data_type == "aje_period":
        return start_date.strftime('%Y%m'), end_date.strftime('%Y%m')
    
    elif data_type in ["aje_date", "aje_processperiod"]:
        # Ajustar a primer día del mes para start y último día para end
        start_date = start_date.replace(day=1, tzinfo=TZ_LIMA)
        
        _, last_day = calendar.monthrange(end_date.year, end_date.month)
        end_date = end_date.replace(day=last_day, tzinfo=TZ_LIMA)
        
        # Convert to Excel date format
        start_days = (start_date - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        end_days = (end_date - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
        
        return str(start_days), str(end_days)
    
    # Default: formato estándar
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def get_date_limits(month_diff: str, data_type: str) -> Tuple[str, str]:
    """
    Get lower and upper limits for date filters (función original)
    Ahora usa la nueva función internamente
    """
    return get_date_limits_with_range(month_diff, "0", data_type)