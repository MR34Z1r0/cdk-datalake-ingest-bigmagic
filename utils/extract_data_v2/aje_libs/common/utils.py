import json
from decimal import Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Si el decimal es entero, devolver int
            if obj % 1 == 0:
                return int(obj)
            # Si no, devolver float
            return float(obj)
        return super(DecimalEncoder, self).default(obj)