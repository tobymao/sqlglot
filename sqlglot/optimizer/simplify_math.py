import sqlglot.expressions as exp
from datetime import datetime
from dateutil.relativedelta import relativedelta

def simplify_math(expr):
    def math(node):
        if isinstance(node, exp.Add):
            left, right = node.unnest_operands()
            if isinstance(left, exp.Literal) and isinstance(right, exp.Literal) and not left.is_string and not right.is_string:
                return exp.Literal.number(float(left.name) + float(right.name))
            if isinstance(left, exp.Cast) and isinstance(right, exp.Interval):
                unit = right.args['unit'].name
                if unit == "year":
                    interval = relativedelta(years = int(right.name))
                elif unit == "month":
                    interval = relativedelta(months = int(right.name))
                elif unit == "day":
                    interval = relativedelta(days = int(right.name))
                else:
                    print("warning: granularity finer than days not supported by optimizer right now")
                    return node.unnest()
                new_date = datetime.strptime(left.name,"%Y-%m-%d")  + interval
                node.left.set("this",new_date.__str__().split(" ")[0])
                return node.left #exp.Cast(this=exp.Literal.string(new_date.__str__()), to=exp.DataType)
        elif isinstance(node, exp.Sub):
            left, right = node.unnest_operands()
            if isinstance(left, exp.Literal) and isinstance(right, exp.Literal) and not left.is_string and not right.is_string:
                return exp.Literal.number(float(left.name) - float(right.name))
            if isinstance(left, exp.Cast) and isinstance(right, exp.Interval):
                unit = right.args['unit'].name
                if unit == "year":
                    interval = relativedelta(years = -int(right.name))
                elif unit == "month":
                    interval = relativedelta(months = -int(right.name))
                elif unit == "day":
                    interval = relativedelta(days = -int(right.name))
                else:
                    print("warning: granularity finer than days not supported by optimizer right now")
                    return node.unnest()
                new_date = datetime.strptime(left.name,"%Y-%m-%d")  + interval
                node.left.set("this",new_date.__str__().split(" ")[0])
                return node.left #exp.Cast(this=exp.Literal.string(new_date.__str__()), to=exp.DataType)
        elif isinstance(node, exp.Mul):
            left, right = node.unnest_operands()
            if isinstance(left, exp.Literal) and isinstance(right, exp.Literal) and not left.is_string and not right.is_string:
                return exp.Literal.number(float(left.name) * float(right.name))
        elif isinstance(node, exp.Div):
            left, right = node.unnest_operands()
            if isinstance(left, exp.Literal) and isinstance(right, exp.Literal) and not left.is_string and not right.is_string:
                return exp.Literal.number(float(left.name) / float(right.name))
            
        return node.unnest()
    
    while True:
        if expr.transform(math) != expr:
            expr = expr.transform(math)
        else:
            break

    return expr
